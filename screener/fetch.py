"""主入口:从 Tushare Pro 拉数据 → 应用 14 项条件 → 生成投资建议 → 写 stocks.json"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import tushare as ts
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "screener"))

from criteria import StockData, CRITERIA, CONCEPT_KEYWORDS, evaluate_all  # noqa
from advice import make_client, generate_advice  # noqa


load_dotenv(ROOT / ".env")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("screener")

CST = timezone(timedelta(hours=8))
MIN_PASS_TO_INCLUDE = 8
MAX_ADVICE_STOCKS = 30
WORKERS = 4

# Track endpoints unavailable on this token (insufficient points / permission)
UNAVAILABLE: set[str] = set()


def make_pro_api():
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        log.error("TUSHARE_TOKEN 未设置。请在 .env 写入 TUSHARE_TOKEN=...")
        sys.exit(2)
    return ts.pro_api(token)


def _safe_call(label: str, fn, *args, **kwargs):
    """统一处理 Tushare 调用:权限/积分不足时记录并返回 None。"""
    if label in UNAVAILABLE:
        return None
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        msg = str(e)
        if "权限" in msg or "积分" in msg or "permission" in msg.lower():
            log.warning(f"  {label} 权限不足(积分不够),后续将跳过此接口")
            UNAVAILABLE.add(label)
        else:
            log.debug(f"  {label} 调用失败: {msg[:120]}")
        return None


def latest_trade_date(pro) -> str:
    """获取最近一个**已收盘**的交易日。北京时间 16:00 前不取今天(数据未出)。"""
    now = datetime.now(CST)
    end_dt = now if now.hour >= 16 else (now - timedelta(days=1))
    end = end_dt.strftime("%Y%m%d")
    start = (end_dt - timedelta(days=15)).strftime("%Y%m%d")
    cal = _safe_call("trade_cal", pro.trade_cal,
                     exchange="SSE", start_date=start, end_date=end, is_open="1")
    if cal is not None and not cal.empty:
        return str(cal["cal_date"].max())
    d = end_dt
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def get_universe(pro) -> pd.DataFrame | None:
    log.info("拉取股票基础信息...")
    df = _safe_call("stock_basic", pro.stock_basic,
                    list_status="L",
                    fields="ts_code,symbol,name,industry,market,list_date")
    if df is None or df.empty:
        return None
    df = df[~df["name"].str.contains("ST", na=False)]
    df = df[df["market"].isin(["主板", "创业板", "科创板"])]
    log.info(f"  全 A 股 {len(df)} 只")
    return df


def get_spot(pro, trade_date: str) -> pd.DataFrame | None:
    log.info(f"拉取 {trade_date} 当日指标...")
    df = _safe_call("daily_basic", pro.daily_basic,
                    trade_date=trade_date,
                    fields="ts_code,close,turnover_rate,pe_ttm,pb,dv_ttm,total_mv,circ_mv")
    if df is None or df.empty:
        return None
    log.info(f"  当日 {len(df)} 只")
    return df


def get_concept_map(pro) -> dict[str, list[str]]:
    """返回 {ts_code: [concept_name, ...]},仅包含命中目标主题的概念。"""
    log.info("拉取概念板块...")
    all_keywords = {kw for kws in CONCEPT_KEYWORDS.values() for kw in kws}

    concepts = _safe_call("concept", pro.concept)
    if concepts is None or concepts.empty:
        log.warning("  概念列表拉取失败")
        return {}

    matched = concepts[concepts["name"].str.contains("|".join(all_keywords), na=False)]
    log.info(f"  匹配 {len(matched)} 个目标概念")

    result: dict[str, list[str]] = {}
    for _, row in matched.iterrows():
        detail = _safe_call("concept_detail",
                            pro.concept_detail, id=row["code"])
        if detail is None or detail.empty:
            continue
        for ts_code in detail["ts_code"]:
            result.setdefault(ts_code, []).append(row["name"])
        time.sleep(0.3)

    log.info(f"  概念映射覆盖 {len(result)} 只")
    return result


def get_kline(pro, ts_code: str) -> pd.DataFrame | None:
    end = datetime.now(CST).strftime("%Y%m%d")
    start = (datetime.now(CST) - timedelta(days=60)).strftime("%Y%m%d")
    df = _safe_call("daily", pro.daily, ts_code=ts_code,
                    start_date=start, end_date=end)
    if df is None or df.empty:
        return None
    df = df.sort_values("trade_date").reset_index(drop=True)

    db = _safe_call("daily_basic", pro.daily_basic, ts_code=ts_code,
                    start_date=start, end_date=end,
                    fields="trade_date,turnover_rate")
    if db is not None and not db.empty:
        db = db.sort_values("trade_date")
        df = df.merge(db, on="trade_date", how="left")

    df.rename(columns={
        "close": "收盘", "vol": "成交量", "pct_chg": "涨跌幅",
        "turnover_rate": "换手率",
    }, inplace=True)
    return df


def get_pe_history(pro, ts_code: str) -> pd.Series | None:
    end = datetime.now(CST).strftime("%Y%m%d")
    start = (datetime.now(CST) - timedelta(days=365 * 5 + 30)).strftime("%Y%m%d")
    df = _safe_call("daily_basic_history", pro.daily_basic,
                    ts_code=ts_code, start_date=start, end_date=end,
                    fields="trade_date,pe_ttm")
    if df is None or df.empty:
        return None
    return df["pe_ttm"]


def get_annual_reports(pro, ts_code: str) -> pd.DataFrame | None:
    fields_ind = "end_date,roe,debt_to_assets"
    fields_cf = "end_date,n_cashflow_act"
    fields_inc = "end_date,n_income"

    ind = _safe_call("fina_indicator", pro.fina_indicator,
                     ts_code=ts_code, fields=fields_ind)
    cf = _safe_call("cashflow", pro.cashflow,
                    ts_code=ts_code, fields=fields_cf)
    inc = _safe_call("income", pro.income,
                     ts_code=ts_code, fields=fields_inc)

    if ind is None and cf is None and inc is None:
        return None

    out = pd.DataFrame()
    if ind is not None and not ind.empty:
        ind = ind[ind["end_date"].str.endswith("1231")].drop_duplicates("end_date")
        out["end_date"] = ind["end_date"]
        out["roe"] = ind["roe"]
        out["debt_ratio"] = ind["debt_to_assets"]
        out = out.set_index("end_date")
    if cf is not None and not cf.empty:
        cf = cf[cf["end_date"].str.endswith("1231")].drop_duplicates("end_date").set_index("end_date")
        out = out.join(cf[["n_cashflow_act"]].rename(columns={"n_cashflow_act": "cfo"}),
                       how="outer")
    if inc is not None and not inc.empty:
        inc = inc[inc["end_date"].str.endswith("1231")].drop_duplicates("end_date").set_index("end_date")
        out = out.join(inc[["n_income"]].rename(columns={"n_income": "net_profit"}),
                       how="outer")

    out = out.sort_index(ascending=False).reset_index(drop=True)
    return out if not out.empty else None


def get_fund_flow(pro, ts_code: str) -> pd.DataFrame | None:
    end = datetime.now(CST).strftime("%Y%m%d")
    start = (datetime.now(CST) - timedelta(days=10)).strftime("%Y%m%d")
    df = _safe_call("moneyflow", pro.moneyflow,
                    ts_code=ts_code, start_date=start, end_date=end,
                    fields="trade_date,net_mf_amount")
    if df is None or df.empty:
        return None
    df = df.sort_values("trade_date").reset_index(drop=True)
    df = df.rename(columns={"net_mf_amount": "main_net"})
    return df


def industry_averages(spot: pd.DataFrame, universe: pd.DataFrame) -> dict[str, dict]:
    merged = spot.merge(universe[["ts_code", "industry"]], on="ts_code", how="left")
    out: dict[str, dict] = {}
    for industry, group in merged.groupby("industry"):
        pbs = group["pb"][(group["pb"] > 0) & (group["pb"] < 100)]
        out[industry] = {
            "pb_avg": float(pbs.mean()) if not pbs.empty else None,
            "debt_avg": None,
        }
    return out


def match_themes_by_industry(industry: str) -> list[str]:
    matched = []
    for theme, keywords in CONCEPT_KEYWORDS.items():
        if any(kw in industry for kw in keywords):
            matched.append(theme)
    return matched


def build_stock(pro, row, industry_avg, concept_map) -> StockData:
    ts_code = row["ts_code"]
    code = row["symbol"]
    name = row["name"]
    industry = row.get("industry") or ""

    concepts = concept_map.get(ts_code, [])
    if not concepts:
        themes = match_themes_by_industry(industry)
        concepts = themes

    s = StockData(
        code=code, name=name, industry=industry, concepts=concepts,
        market_cap=float(row.get("total_mv") or 0) * 1e4,
        pe_ttm=float(row["pe_ttm"]) if pd.notna(row.get("pe_ttm")) else None,
        pb=float(row["pb"]) if pd.notna(row.get("pb")) else None,
        dv_ttm=float(row["dv_ttm"]) if pd.notna(row.get("dv_ttm")) else None,
        industry_pb_avg=industry_avg.get(industry, {}).get("pb_avg"),
    )

    s.kline_30d = get_kline(pro, ts_code)
    s.pe_history_5y = get_pe_history(pro, ts_code)
    s.annual_reports = get_annual_reports(pro, ts_code)
    s.fund_flow_3d = get_fund_flow(pro, ts_code)

    if s.annual_reports is not None and not s.annual_reports.empty:
        if "debt_ratio" in s.annual_reports.columns:
            v = s.annual_reports["debt_ratio"].iloc[0]
            s.debt_ratio = float(v) if pd.notna(v) else None

    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-advice", action="store_true")
    ap.add_argument("--out", default=str(ROOT / "web" / "data" / "stocks.json"))
    args = ap.parse_args()

    started = time.time()
    pro = make_pro_api()

    trade_date = latest_trade_date(pro)
    log.info(f"最新交易日: {trade_date}")

    universe = get_universe(pro)
    spot = get_spot(pro, trade_date) if universe is not None else None
    if universe is None or spot is None:
        out_path = Path(args.out)
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8")) if out_path.exists() else None
        except Exception:
            existing = None
        if existing and existing.get("stocks"):
            log.warning("当日数据不可用(可能未收盘/无权限),保留昨日 stocks.json,不覆盖。")
            return
        log.error("daily_basic 接口无权限或返回空。请前往 tushare.pro 完善资料获取积分。")
        out = {
            "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M CST"),
            "trade_date": trade_date,
            "elapsed_sec": round(time.time() - started, 1),
            "unavailable_endpoints": sorted(UNAVAILABLE | {"daily_basic"}),
            "criteria_meta": [{"key": k, "label": label} for k, label, _ in CRITERIA],
            "stocks": [],
            "error": "Tushare 接口权限不足(daily_basic 需要 ≥2000 积分)。前往 tushare.pro 个人主页完善资料即可获得。",
        }
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    df = universe.merge(spot, on="ts_code", how="inner")
    df = df[df["total_mv"].notna() & (df["total_mv"] * 1e4 < 2e10)]
    df = df[df["pb"].notna() & (df["pb"] < 10)]
    log.info(f"市值<200亿 且 PB<10 后:{len(df)} 只")

    concept_map = get_concept_map(pro)
    industry_avg = industry_averages(spot, universe)

    def hits_theme(row):
        ts_code = row["ts_code"]
        if ts_code in concept_map:
            return True
        return bool(match_themes_by_industry(row.get("industry") or ""))

    df = df[df.apply(hits_theme, axis=1)]
    log.info(f"命中目标行业/概念后:{len(df)} 只")

    if args.limit:
        df = df.head(args.limit)
        log.info(f"开发模式限制为前 {len(df)} 只")

    rows = list(df.to_dict("records"))
    stocks: list[StockData] = []

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(build_stock, pro, r, industry_avg, concept_map): r for r in rows}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                s = fut.result()
                if s is not None:
                    stocks.append(s)
            except Exception as e:
                log.warning(f"  build_stock 异常: {e}")
            if i % 10 == 0:
                log.info(f"  已抓 {i}/{len(rows)}")

    debt_samples: dict[str, list[float]] = {}
    for s in stocks:
        if s.industry and s.debt_ratio is not None and 0 < s.debt_ratio < 100:
            debt_samples.setdefault(s.industry, []).append(s.debt_ratio)
    for ind, vs in debt_samples.items():
        if ind in industry_avg:
            industry_avg[ind]["debt_avg"] = sum(vs) / len(vs)
    for s in stocks:
        s.industry_debt_avg = industry_avg.get(s.industry, {}).get("debt_avg")

    output_stocks = []
    for s in stocks:
        result = evaluate_all(s)
        n_pass = sum(result["passed"].values())
        if n_pass < MIN_PASS_TO_INCLUDE:
            continue
        output_stocks.append({
            "code": s.code,
            "name": s.name,
            "industry": s.industry,
            "concepts": s.concepts,
            "metrics": {
                "market_cap": s.market_cap,
                "pe_ttm": s.pe_ttm,
                "pb": s.pb,
                "dv_ttm": s.dv_ttm,
                "debt_ratio": s.debt_ratio,
                "industry_pb_avg": s.industry_pb_avg,
                "industry_debt_avg": s.industry_debt_avg,
            },
            "criteria": result["passed"],
            "n_pass": n_pass,
            "advice": "",
        })

    output_stocks.sort(key=lambda x: -x["n_pass"])
    log.info(f"通过 ≥{MIN_PASS_TO_INCLUDE} 项的股票: {len(output_stocks)} 只")

    if not args.skip_advice and output_stocks:
        log.info(f"生成投资建议(最多 {MAX_ADVICE_STOCKS} 只)...")
        client = make_client()
        for stock in output_stocks[:MAX_ADVICE_STOCKS]:
            stock["advice"] = generate_advice(client, stock)

    out = {
        "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M CST"),
        "trade_date": trade_date,
        "elapsed_sec": round(time.time() - started, 1),
        "unavailable_endpoints": sorted(UNAVAILABLE),
        "criteria_meta": [{"key": k, "label": label} for k, label, _ in CRITERIA],
        "stocks": output_stocks,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"已写入 {out_path} (耗时 {out['elapsed_sec']}s)")
    if UNAVAILABLE:
        log.warning(f"权限不足的接口: {sorted(UNAVAILABLE)} -- 相关条件未被精确评估")


if __name__ == "__main__":
    main()
