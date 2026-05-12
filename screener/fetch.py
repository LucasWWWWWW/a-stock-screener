"""主入口:从 Tushare Pro 拉数据 → 应用 13 项条件 → 生成投资建议 → 写 stocks.json

设计:
- 后端唯一硬筛:命中目标行业/概念主题(能源/安全/高股息/硬科技/AI/存储/芯片/光通信/算力租赁)
- 不限市值、不限 PB、不限通过条数;所有命中主题的 A 股全部入选
- K 线、换手率、资金流用按日期 batch 接口拉(从每股 1 次降到每日 1 次)
- 财报和 PE 历史仍需 per-stock(无 vip 积分时)
"""

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

from criteria import StockData, CRITERIA, CRITERIA_META, CONCEPT_KEYWORDS, evaluate_all, extract_tunable_values  # noqa
from advice import make_client, generate_advice, generate_daily_script  # noqa


load_dotenv(ROOT / ".env")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("screener")

CST = timezone(timedelta(hours=8))
WORKERS = 4
ADVICE_WORKERS = 4

UNAVAILABLE: set[str] = set()


def make_pro_api():
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        log.error("TUSHARE_TOKEN 未设置")
        sys.exit(2)
    return ts.pro_api(token)


def _safe_call(label: str, fn, *args, **kwargs):
    if label in UNAVAILABLE:
        return None
    last_err = None
    for attempt in range(3):
        try:
            r = fn(*args, **kwargs)
            return r
        except Exception as e:
            last_err = e
            msg = str(e)
            if "权限" in msg or "积分" in msg or "permission" in msg.lower():
                log.warning(f"  {label} 权限不足,跳过")
                UNAVAILABLE.add(label)
                return None
            log.warning(f"  {label} 第{attempt+1}次失败: {msg[:100]} — 重试")
            time.sleep(2 ** attempt)
    log.error(f"  {label} 3 次重试均失败: {str(last_err)[:120]}")
    return None


def get_trade_calendar(pro, days_back: int) -> list[str]:
    """获取最近 N 天的交易日(已收盘),返回升序日期列表 [oldest..latest]。"""
    now = datetime.now(CST)
    end_dt = now if now.hour >= 16 else (now - timedelta(days=1))
    end = end_dt.strftime("%Y%m%d")
    start = (end_dt - timedelta(days=days_back * 2)).strftime("%Y%m%d")
    cal = _safe_call("trade_cal", pro.trade_cal,
                     exchange="SSE", start_date=start, end_date=end, is_open="1")
    if cal is not None and not cal.empty:
        return sorted(cal["cal_date"].astype(str).tolist())[-days_back:]
    dates = []
    d = end_dt
    while len(dates) < days_back:
        if d.weekday() < 5:
            dates.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return sorted(dates)


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
        detail = _safe_call("concept_detail", pro.concept_detail, id=row["code"])
        if detail is None or detail.empty:
            continue
        for ts_code in detail["ts_code"]:
            result.setdefault(ts_code, []).append(row["name"])
        time.sleep(0.25)
    log.info(f"  概念映射覆盖 {len(result)} 只")
    return result


def batch_kline(pro, trading_dates: list[str]) -> dict[str, pd.DataFrame]:
    """按日期 batch 拉取 K 线 + 换手率。返回 {ts_code: DataFrame[trade_date, 收盘, 成交量, 涨跌幅, 换手率]}"""
    log.info(f"按日期 batch 拉 K 线({len(trading_dates)} 天)...")
    kline_rows = []
    db_rows = []
    for i, d in enumerate(trading_dates, 1):
        k = _safe_call("daily_batch", pro.daily, trade_date=d,
                       fields="ts_code,trade_date,close,vol,pct_chg")
        if k is not None and not k.empty:
            kline_rows.append(k)
        db = _safe_call("daily_basic_batch", pro.daily_basic, trade_date=d,
                        fields="ts_code,trade_date,turnover_rate")
        if db is not None and not db.empty:
            db_rows.append(db)
        if i % 10 == 0:
            log.info(f"  K线 {i}/{len(trading_dates)}")
    if not kline_rows:
        return {}
    all_k = pd.concat(kline_rows, ignore_index=True)
    all_db = pd.concat(db_rows, ignore_index=True) if db_rows else None
    if all_db is not None:
        all_k = all_k.merge(all_db, on=["ts_code", "trade_date"], how="left")
    all_k = all_k.rename(columns={
        "close": "收盘", "vol": "成交量", "pct_chg": "涨跌幅", "turnover_rate": "换手率",
    })
    all_k["trade_date"] = all_k["trade_date"].astype(str)
    all_k = all_k.sort_values(["ts_code", "trade_date"])
    result = {code: g.reset_index(drop=True) for code, g in all_k.groupby("ts_code")}
    log.info(f"  K线汇总 {len(result)} 只股票")
    return result


def batch_moneyflow(pro, trading_dates: list[str]) -> dict[str, pd.DataFrame]:
    log.info(f"按日期 batch 拉资金流(最近 {min(6, len(trading_dates))} 天)...")
    rows = []
    for d in trading_dates[-6:]:
        mf = _safe_call("moneyflow_batch", pro.moneyflow, trade_date=d,
                        fields="ts_code,trade_date,net_mf_amount")
        if mf is not None and not mf.empty:
            rows.append(mf)
    if not rows:
        return {}
    all_mf = pd.concat(rows, ignore_index=True)
    all_mf = all_mf.rename(columns={"net_mf_amount": "main_net"})
    all_mf["trade_date"] = all_mf["trade_date"].astype(str)
    all_mf = all_mf.sort_values(["ts_code", "trade_date"])
    result = {code: g.reset_index(drop=True) for code, g in all_mf.groupby("ts_code")}
    log.info(f"  资金流汇总 {len(result)} 只股票")
    return result


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
    ind = _safe_call("fina_indicator", pro.fina_indicator,
                     ts_code=ts_code, fields="end_date,roe,debt_to_assets")
    cf = _safe_call("cashflow", pro.cashflow,
                    ts_code=ts_code, fields="end_date,n_cashflow_act")
    inc = _safe_call("income", pro.income,
                     ts_code=ts_code, fields="end_date,n_income")
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
        out = out.join(cf[["n_cashflow_act"]].rename(columns={"n_cashflow_act": "cfo"}), how="outer")
    if inc is not None and not inc.empty:
        inc = inc[inc["end_date"].str.endswith("1231")].drop_duplicates("end_date").set_index("end_date")
        out = out.join(inc[["n_income"]].rename(columns={"n_income": "net_profit"}), how="outer")
    out = out.sort_index(ascending=False).reset_index(drop=True)
    return out if not out.empty else None


def industry_averages_full(spot: pd.DataFrame, universe: pd.DataFrame) -> dict[str, dict]:
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


def build_stock(pro, row, industry_avg, concept_map, kline_map, mf_map) -> StockData:
    ts_code = row["ts_code"]
    code = row["symbol"]
    name = row["name"]
    industry = row.get("industry") or ""

    concepts = concept_map.get(ts_code, [])
    if not concepts:
        concepts = match_themes_by_industry(industry)

    s = StockData(
        code=code, name=name, industry=industry, concepts=concepts,
        market_cap=float(row.get("total_mv") or 0) * 1e4,
        pe_ttm=float(row["pe_ttm"]) if pd.notna(row.get("pe_ttm")) else None,
        pb=float(row["pb"]) if pd.notna(row.get("pb")) else None,
        dv_ttm=float(row["dv_ttm"]) if pd.notna(row.get("dv_ttm")) else None,
        industry_pb_avg=industry_avg.get(industry, {}).get("pb_avg"),
    )
    s.kline_30d = kline_map.get(ts_code)
    s.fund_flow_3d = mf_map.get(ts_code)
    s.pe_history_5y = get_pe_history(pro, ts_code)
    s.annual_reports = get_annual_reports(pro, ts_code)

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

    trading_dates = get_trade_calendar(pro, days_back=30)
    trade_date = trading_dates[-1] if trading_dates else None
    log.info(f"最新已收盘交易日: {trade_date}; 共 {len(trading_dates)} 天 K 线")

    universe = get_universe(pro)
    spot = get_spot(pro, trade_date) if universe is not None and trade_date else None
    if universe is None or spot is None or not trade_date:
        out_path = Path(args.out)
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8")) if out_path.exists() else None
        except Exception:
            existing = None
        if existing and existing.get("stocks"):
            log.warning("当日数据不可用,保留上次 stocks.json")
            return
        out = {
            "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M CST"),
            "trade_date": trade_date,
            "elapsed_sec": round(time.time() - started, 1),
            "unavailable_endpoints": sorted(UNAVAILABLE | {"daily_basic"}),
            "criteria_meta": CRITERIA_META,
            "stocks": [],
            "error": "Tushare 关键接口权限不足。请完善个人资料获取积分。",
        }
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    industry_avg = industry_averages_full(spot, universe)
    concept_map = get_concept_map(pro)

    df = universe.merge(spot, on="ts_code", how="inner")

    def hits_theme(row):
        if row["ts_code"] in concept_map:
            return True
        return bool(match_themes_by_industry(row.get("industry") or ""))

    df = df[df.apply(hits_theme, axis=1)]
    log.info(f"主题命中(唯一硬筛):{len(df)} 只")

    if args.limit:
        df = df.head(args.limit)
        log.info(f"开发模式限制为前 {len(df)} 只")

    rows = list(df.to_dict("records"))

    kline_map = batch_kline(pro, trading_dates)
    mf_map = batch_moneyflow(pro, trading_dates)

    stocks: list[StockData] = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(build_stock, pro, r, industry_avg, concept_map, kline_map, mf_map): r for r in rows}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                s = fut.result()
                if s is not None:
                    stocks.append(s)
            except Exception as e:
                log.warning(f"  build_stock 异常: {e}")
            if i % 50 == 0:
                log.info(f"  per-stock 已抓 {i}/{len(rows)}")

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
        kline_close = []
        if s.kline_30d is not None and not s.kline_30d.empty and "收盘" in s.kline_30d.columns:
            kline_close = [
                round(float(x), 3) for x in s.kline_30d["收盘"].tail(30).tolist()
                if pd.notna(x)
            ]
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
            "tunable_values": extract_tunable_values(s, result),
            "criteria": result["passed"],
            "kline_close": kline_close,
            "n_pass": n_pass,
            "advice": "",
        })

    output_stocks.sort(key=lambda x: -x["n_pass"])
    log.info(f"全主题命中股票输出: {len(output_stocks)} 只")

    if not args.skip_advice and output_stocks:
        log.info(f"并行生成 {len(output_stocks)} 只投资建议(含 intro/bull/bear, workers={ADVICE_WORKERS})...")
        client = make_client()
        done_count = 0

        def gen_one(stock):
            return generate_advice(client, stock)

        with ThreadPoolExecutor(max_workers=ADVICE_WORKERS) as ex:
            futs = {ex.submit(gen_one, s): s for s in output_stocks}
            for fut in as_completed(futs):
                s = futs[fut]
                try:
                    payload = fut.result()
                    s["advice"] = payload.get("advice_text", "")
                    s["intro"] = payload.get("intro", "")
                    s["bull"] = payload.get("bull", [])
                    s["bear"] = payload.get("bear", [])
                except Exception as e:
                    s["advice"] = f"(生成失败: {e!s})"
                    s["intro"] = ""
                    s["bull"] = []
                    s["bear"] = []
                done_count += 1
                if done_count % 100 == 0:
                    log.info(f"  advice {done_count}/{len(output_stocks)}")

    # 行业聚合(热力图用):每个行业总数、平均通过条数、命中率
    from collections import Counter
    industry_stats: dict[str, dict] = {}
    for s in output_stocks:
        ind = s.get("industry") or "未分类"
        if ind not in industry_stats:
            industry_stats[ind] = {"name": ind, "count": 0, "n_pass_sum": 0, "perfect": 0}
        industry_stats[ind]["count"] += 1
        industry_stats[ind]["n_pass_sum"] += s["n_pass"]
        if s["n_pass"] >= 11:
            industry_stats[ind]["perfect"] += 1
    industries = []
    for ind, st in industry_stats.items():
        c = st["count"]
        if c < 3:
            continue  # 样本太小不进热力图,避免被冷门小行业刷榜
        industries.append({
            "name": st["name"],
            "count": c,
            "avg_pass": round(st["n_pass_sum"] / c, 1) if c else 0,
            "strong_ratio": round(st["perfect"] / c, 3) if c else 0,
        })
    industries.sort(key=lambda x: -x["avg_pass"])

    # 信号稀缺度:同样通过条数的股票数量分布
    npass_dist = dict(Counter(s["n_pass"] for s in output_stocks))

    daily_script = ""
    if not args.skip_advice and output_stocks:
        log.info("生成 AI 主播日报...")
        client_d = make_client()
        daily_script = generate_daily_script(client_d, output_stocks, trade_date)
        if daily_script:
            log.info(f"  日报 {len(daily_script)} 字 ✓")

    out = {
        "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M CST"),
        "trade_date": trade_date,
        "elapsed_sec": round(time.time() - started, 1),
        "unavailable_endpoints": sorted(UNAVAILABLE),
        "criteria_meta": CRITERIA_META,
        "daily_script": daily_script,
        "industries": industries,
        "npass_distribution": npass_dist,
        "stocks": output_stocks,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"已写入 {out_path} (耗时 {out['elapsed_sec']}s)")
    if UNAVAILABLE:
        log.warning(f"权限不足接口: {sorted(UNAVAILABLE)} -- 相关条件按缺失评估")


if __name__ == "__main__":
    main()
