"""每条筛选条件一个函数。每个函数接收一个 StockData,返回 (passed: bool, value: Any)。"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd


@dataclass
class StockData:
    code: str
    name: str
    industry: str = ""
    concepts: list[str] = field(default_factory=list)

    market_cap: float = 0.0
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dv_ttm: Optional[float] = None

    kline_30d: Optional[pd.DataFrame] = None
    pe_history_5y: Optional[pd.Series] = None
    annual_reports: Optional[pd.DataFrame] = None
    fund_flow_3d: Optional[pd.DataFrame] = None

    debt_ratio: Optional[float] = None
    industry_debt_avg: Optional[float] = None
    industry_pb_avg: Optional[float] = None


def c1_market_cap(s: StockData):
    return s.market_cap > 0 and s.market_cap < 2e10, s.market_cap


def c2_turnover_3d(s: StockData):
    if s.kline_30d is None or len(s.kline_30d) < 3:
        return False, None
    last3 = s.kline_30d.tail(3)["换手率"]
    return bool((last3 >= 5).all()), float(last3.min())


def c3_pb_lt_10(s: StockData):
    if s.pb is None:
        return False, None
    return s.pb < 10, s.pb


def c4_roe_3y(s: StockData):
    if s.annual_reports is None or len(s.annual_reports) < 3:
        return False, None
    roe = s.annual_reports.head(3)["roe"]
    return bool((roe > 12).all()), float(roe.min())


def c5_cfo_vs_ni_2y(s: StockData):
    if s.annual_reports is None or len(s.annual_reports) < 2:
        return False, None
    df = s.annual_reports.head(2)
    if df["cfo"].isna().any() or df["net_profit"].isna().any():
        return False, None
    passed = bool((df["cfo"] > df["net_profit"]).all())
    val = float((df["cfo"] - df["net_profit"]).min())
    return passed, val


def c6_debt_vs_industry(s: StockData):
    if s.debt_ratio is None or s.industry_debt_avg is None:
        return False, None
    return s.debt_ratio < s.industry_debt_avg * 0.8, s.debt_ratio


CONCEPT_KEYWORDS = {
    "能源": ["能源", "石油", "煤炭", "新能源", "电力", "光伏", "风电", "储能"],
    "安全": ["网络安全", "信息安全", "国防", "军工"],
    "高股息": ["高股息", "红利"],
    "硬科技": ["硬科技", "专精特新"],
    "AI": ["人工智能", "AI", "AIGC", "大模型", "ChatGPT"],
    "存储": ["存储", "存储芯片", "DDR", "HBM"],
    "芯片": ["芯片", "半导体", "集成电路", "国产芯片"],
    "光通信": ["光通信", "光模块", "光器件", "CPO"],
    "算力租赁": ["算力", "算力租赁", "智算", "IDC", "数据中心"],
}


def c7_concept_match(s: StockData):
    matched = []
    for theme, keywords in CONCEPT_KEYWORDS.items():
        for c in s.concepts:
            if any(k in c for k in keywords):
                matched.append(theme)
                break
    return len(matched) > 0, matched


def c8_pe_percentile(s: StockData):
    if s.pe_history_5y is None or s.pe_ttm is None or len(s.pe_history_5y) < 100:
        return False, None
    series = s.pe_history_5y.dropna()
    series = series[series > 0]
    if len(series) < 100:
        return False, None
    pct = (series < s.pe_ttm).mean() * 100
    return pct < 50, float(pct)


def c9_dividend_yield(s: StockData):
    if s.dv_ttm is None:
        return False, None
    return s.dv_ttm > 2, s.dv_ttm


def c10_pb_vs_industry(s: StockData):
    if s.pb is None or s.industry_pb_avg is None:
        return False, None
    return s.pb < s.industry_pb_avg, s.pb


def c11_limit_up_1m(s: StockData):
    if s.kline_30d is None or s.kline_30d.empty:
        return False, None
    threshold = 19.5 if s.code.startswith(("30", "68")) else 9.8
    pct = s.kline_30d["涨跌幅"]
    return bool((pct >= threshold).any()), float(pct.max())


def c12_ma_alignment(s: StockData):
    if s.kline_30d is None or len(s.kline_30d) < 20:
        return False, None
    close = s.kline_30d["收盘"]
    ma5 = close.rolling(5).mean().iloc[-1]
    ma10 = close.rolling(10).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    last = close.iloc[-1]
    if pd.isna(ma5) or pd.isna(ma10) or pd.isna(ma20):
        return False, None
    return bool(last > ma20 and ma5 > ma10 > ma20), {
        "close": float(last), "ma5": float(ma5), "ma10": float(ma10), "ma20": float(ma20)
    }


def c13_main_fund_inflow(s: StockData):
    if s.fund_flow_3d is None or len(s.fund_flow_3d) < 3:
        return False, None
    last3 = s.fund_flow_3d.tail(3)["main_net"]
    return bool((last3 > 0).all()), float(last3.sum())


def c14_volume_expansion(s: StockData):
    if s.kline_30d is None or len(s.kline_30d) < 6:
        return False, None
    vol = s.kline_30d["成交量"]
    recent = vol.tail(3).sum()
    prev = vol.iloc[-6:-3].sum()
    if prev <= 0:
        return False, None
    ratio = recent / prev
    return ratio > 1.5, float(ratio)


CRITERIA = [
    ("market_cap", "市值<200亿", c1_market_cap),
    ("turnover_3d", "近3日换手率每天≥5%", c2_turnover_3d),
    ("pb_lt_10", "PB<10", c3_pb_lt_10),
    ("roe_3y", "ROE连续3年>12%", c4_roe_3y),
    ("cfo_vs_ni", "经营现金流连续2年>净利润", c5_cfo_vs_ni_2y),
    ("debt_vs_industry", "资产负债率低于行业均值20%+", c6_debt_vs_industry),
    ("concept_match", "命中目标行业/概念", c7_concept_match),
    ("pe_percentile", "PE 5年百分位<50%", c8_pe_percentile),
    ("dividend_yield", "股息率>2%", c9_dividend_yield),
    ("pb_vs_industry", "PB<行业均值", c10_pb_vs_industry),
    ("limit_up_1m", "近1月有过涨停", c11_limit_up_1m),
    ("ma_alignment", "5/10/20日多头排列", c12_ma_alignment),
    ("main_fund_inflow", "近3日主力连续净流入", c13_main_fund_inflow),
    ("volume_expansion", "近3日成交量较前3日放大50%+", c14_volume_expansion),
]


def evaluate_all(s: StockData) -> dict:
    result = {"passed": {}, "values": {}}
    for key, _label, fn in CRITERIA:
        try:
            passed, val = fn(s)
        except Exception as e:
            passed, val = False, f"error: {e!s}"
        result["passed"][key] = bool(passed)
        result["values"][key] = val
    return result
