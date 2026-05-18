"""大盘天气仪表盘:沪深300 SMA200 / PE 历史分位 / HV30 / 风险档位 / 估值带

借鉴 QQQ Tier 3 策略思路,但适配 A 股(无 VIX 时用 HV30 替代,无 TQQQ 时用仓位建议代替杠杆)。
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

CST = timezone(timedelta(hours=8))
log = logging.getLogger(__name__)

# 施密特触发器缓冲带:防止牛熊频繁切换
REGIME_BUFFER_UP = 1.04   # close > SMA × 1.04 → 牛市
REGIME_BUFFER_DOWN = 0.97 # close < SMA × 0.97 → 熊市


def fetch_market_regime(pro, safe_call) -> dict | None:
    """返回大盘状态字典。失败时返回 None,前端走降级路径。"""
    now = datetime.now(CST)
    end = now.strftime("%Y%m%d")
    start_long = (now - timedelta(days=365 * 5 + 30)).strftime("%Y%m%d")
    start_short = (now - timedelta(days=320)).strftime("%Y%m%d")

    daily = safe_call(
        "index_daily", pro.index_daily,
        ts_code="000300.SH", start_date=start_short, end_date=end,
        fields="trade_date,close",
    )
    if daily is None or daily.empty:
        log.warning("index_daily 拉取失败,大盘仪表盘不可用")
        return None
    daily = daily.sort_values("trade_date").reset_index(drop=True)

    if len(daily) < 200:
        log.warning("沪深300 K 线不足 200 天,SMA200 无法计算")
        return None

    sma200 = float(daily["close"].rolling(200).mean().iloc[-1])
    current = float(daily["close"].iloc[-1])
    ratio = current / sma200 if sma200 > 0 else None

    if ratio is None:
        regime = "unknown"
    elif ratio >= REGIME_BUFFER_UP:
        regime = "bull"
    elif ratio <= REGIME_BUFFER_DOWN:
        regime = "bear"
    else:
        regime = "transition"

    # HV30: 近 30 日年化历史波动率
    returns = daily["close"].pct_change().dropna()
    last30 = returns.tail(30)
    hv30 = float(last30.std() * np.sqrt(252) * 100) if len(last30) >= 20 else None

    # 仓位档位建议(借鉴文章的 VIX 分档,改用 A 股 HV30 阈值)
    if hv30 is None:
        risk_tier = "unknown"
        position_pct = 75
        tier_label = "数据缺失"
        tier_desc = "—"
    elif hv30 < 15:
        risk_tier = "low"
        position_pct = 100
        tier_label = "激进"
        tier_desc = "波动低 · 全仓位"
    elif hv30 < 22:
        risk_tier = "mid-low"
        position_pct = 75
        tier_label = "标准"
        tier_desc = "波动适中 · 75% 仓位"
    elif hv30 < 30:
        risk_tier = "mid"
        position_pct = 50
        tier_label = "谨慎"
        tier_desc = "波动偏高 · 50% 仓位"
    elif hv30 < 40:
        risk_tier = "high"
        position_pct = 25
        tier_label = "防御"
        tier_desc = "波动剧烈 · 25% 仓位"
    else:
        risk_tier = "extreme"
        position_pct = 0
        tier_label = "空仓"
        tier_desc = "极端波动 · 建议空仓避险"

    # 熊市强制覆盖
    if regime == "bear":
        position_pct = min(position_pct, 25)
        tier_label = "防御"
        tier_desc = f"沪深300 跌破 200 日均线 · 仓位 ≤ {position_pct}%"

    # PE 历史分位
    pe_daily = safe_call(
        "index_dailybasic", pro.index_dailybasic,
        ts_code="000300.SH", start_date=start_long, end_date=end,
        fields="trade_date,pe_ttm",
    )
    pe_current = None
    pe_percentile = None
    valuation_zone = None
    if pe_daily is not None and not pe_daily.empty:
        pe_daily = pe_daily.sort_values("trade_date")
        pe_series = pe_daily["pe_ttm"].dropna()
        pe_series = pe_series[pe_series > 0]
        if len(pe_series) >= 100:
            pe_current = float(pe_series.iloc[-1])
            pe_percentile = float((pe_series < pe_current).mean() * 100)
            if pe_percentile < 20:
                valuation_zone = "历史底部"
            elif pe_percentile < 40:
                valuation_zone = "低估区"
            elif pe_percentile < 60:
                valuation_zone = "正常"
            elif pe_percentile < 80:
                valuation_zone = "偏高"
            else:
                valuation_zone = "过热区"

    return {
        "csi300_close": current,
        "csi300_sma200": sma200,
        "csi300_sma_ratio": float(ratio) if ratio else None,
        "regime": regime,
        "hv30": hv30,
        "risk_tier": risk_tier,
        "position_pct": position_pct,
        "tier_label": tier_label,
        "tier_desc": tier_desc,
        "csi300_pe": pe_current,
        "csi300_pe_percentile_5y": pe_percentile,
        "valuation_zone": valuation_zone,
    }


def batch_annual_closes(pro, safe_call, years: int = 10) -> dict[str, dict[int, float]]:
    """按年末交易日 batch 拉取所有 A 股收盘价。返回 {ts_code: {year: close}}。
    用于年度收益率条形图。"""
    now = datetime.now(CST)
    current_year = now.year
    cal = safe_call(
        "trade_cal_annual", pro.trade_cal,
        exchange="SSE",
        start_date=f"{current_year - years}0101",
        end_date=now.strftime("%Y%m%d"),
        is_open="1",
    )
    if cal is None or cal.empty:
        log.warning("trade_cal 失败,年度收益不可用")
        return {}
    cal["cal_date"] = cal["cal_date"].astype(str)
    cal["year"] = cal["cal_date"].str[:4].astype(int)
    last_per_year = cal.groupby("year")["cal_date"].max().to_dict()

    result: dict[str, dict[int, float]] = {}
    for year, date in sorted(last_per_year.items()):
        df = safe_call(
            f"daily_y{year}", pro.daily,
            trade_date=date, fields="ts_code,close",
        )
        if df is None or df.empty:
            continue
        for ts_code, close in zip(df["ts_code"], df["close"]):
            try:
                v = float(close)
            except (TypeError, ValueError):
                continue
            result.setdefault(ts_code, {})[int(year)] = v
    log.info(f"  年度收盘价覆盖 {len(result)} 只(共 {len(last_per_year)} 年)")
    return result


def to_annual_returns_list(annual_map: dict[int, float]) -> list[dict]:
    """从 {year: close} 转为 [{year, close, return_pct}] 按年升序。第一年 return_pct = null。"""
    if not annual_map:
        return []
    years = sorted(annual_map.keys())
    out = []
    prev_close = None
    for y in years:
        c = annual_map[y]
        ret = None
        if prev_close is not None and prev_close > 0:
            ret = round((c - prev_close) / prev_close * 100, 2)
        out.append({"year": y, "close": round(c, 3), "return_pct": ret})
        prev_close = c
    return out
