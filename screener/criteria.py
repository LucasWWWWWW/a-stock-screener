"""每条筛选条件一个函数,返回 (passed, raw_value)。CRITERIA_META 定义可调阈值的元数据。"""

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
    if s.debt_ratio is None or s.industry_debt_avg is None or s.industry_debt_avg <= 0:
        return False, None
    gap_pct = (1 - s.debt_ratio / s.industry_debt_avg) * 100
    return gap_pct >= 20, float(gap_pct)


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
    ("pe_percentile", "PE 5年百分位<50%", c8_pe_percentile),
    ("dividend_yield", "股息率>2%", c9_dividend_yield),
    ("pb_vs_industry", "PB<行业均值", c10_pb_vs_industry),
    ("limit_up_1m", "近1月有过涨停", c11_limit_up_1m),
    ("ma_alignment", "5/10/20日多头排列", c12_ma_alignment),
    ("main_fund_inflow", "近3日主力连续净流入", c13_main_fund_inflow),
    ("volume_expansion", "近3日成交量较前3日放大50%+", c14_volume_expansion),
]


# 用于前端可调阈值 UI 的元数据。
# - tunable: 前端可改阈值;否则只能勾选/取消。
# - operator: 比较方向(影响 UI 显示和过滤逻辑)。
# - scale: 后端值除以 scale 得到展示值(例如市值原始单位元,scale=1e8 显示为亿)。
# - value_key: stock.tunable_values 的字段名。
# - default: 用户最初要求的阈值(必含在 presets 内)。
# - presets: 内置候选值,展示用单位(亿、%、倍等)。
CRITERIA_META = [
    {
        "key": "market_cap",
        "label": "市值",
        "tunable": True,
        "operator": "<",
        "unit": "亿",
        "scale": 1e8,
        "value_key": "market_cap",
        "default": 200,
        "presets": [50, 100, 150, 200, 300, 500],
        "why": "小市值股估值起点低、价格弹性大;遇上行情可能爆发。但流动性弱、波动大,适合愿意承担风险换收益的投资者。",
    },
    {
        "key": "turnover_3d",
        "label": "近 3 日换手率每天",
        "tunable": True,
        "operator": ">=",
        "unit": "%",
        "scale": 1,
        "value_key": "turnover_3d_min",
        "default": 5,
        "presets": [3, 5, 7, 10],
        "why": "换手率高说明交投活跃、资金在持续进出。常预示着即将出现新的趋势或方向变化;过低则说明无人关注。",
    },
    {
        "key": "pb_lt_10",
        "label": "PB",
        "tunable": True,
        "operator": "<",
        "unit": "",
        "scale": 1,
        "value_key": "pb",
        "default": 10,
        "presets": [1, 2, 3, 5, 10],
        "why": "PB 即市净率,是股价相对公司账面净资产的倍数。PB 越高溢价越大,越低越接近\"清算价格\"。过低有时是\"便宜\",有时是\"陷阱\"。",
    },
    {
        "key": "roe_3y",
        "label": "近 3 年 ROE 最低",
        "tunable": True,
        "operator": ">",
        "unit": "%",
        "scale": 1,
        "value_key": "roe_3y_min",
        "default": 12,
        "presets": [8, 10, 12, 15, 20],
        "why": "ROE 是每年用股东的钱赚回多少%。连续 3 年 >12% 说明公司持续高效赚钱,是优质企业的核心标志。巴菲特最看重的指标之一。",
    },
    {
        "key": "cfo_vs_ni",
        "label": "经营现金流>净利润(近 2 年)",
        "tunable": False,
        "why": "经营现金流大于净利润,说明账面利润是真金白银,而不是赊账或挂账。能过滤掉\"账面赚钱实际不赚\"的水分公司。",
    },
    {
        "key": "debt_vs_industry",
        "label": "资产负债率低于行业均值",
        "tunable": True,
        "operator": ">=",
        "unit": "%",
        "scale": 1,
        "value_key": "debt_vs_industry_gap",
        "default": 20,
        "presets": [10, 20, 30, 40],
        "why": "负债率低于同行越多,说明公司财务越保守、越抗风险。在经济下行或行业危机时,这种公司的存活率明显更高。",
    },
    {
        "key": "pe_percentile",
        "label": "PE 近 5 年百分位",
        "tunable": True,
        "operator": "<",
        "unit": "%",
        "scale": 1,
        "value_key": "pe_percentile",
        "default": 50,
        "presets": [20, 30, 50, 70],
        "why": "把这只股近 5 年所有交易日的 PE 排序,看现在处于什么位置。低于 50% 即\"比过去一半时候都便宜\",有估值修复空间。",
    },
    {
        "key": "dividend_yield",
        "label": "股息率",
        "tunable": True,
        "operator": ">",
        "unit": "%",
        "scale": 1,
        "value_key": "dv_ttm",
        "default": 2,
        "presets": [1, 2, 3, 5],
        "why": "股息率高意味着即使不涨,光是分红也能持续收益,在熊市起到底部支撑作用。但要警惕一次性高分红的\"假高息\"。",
    },
    {
        "key": "pb_vs_industry",
        "label": "PB 低于行业均值",
        "tunable": False,
        "why": "在同一行业里,PB 低于平均水平的公司更可能被低估。比单纯看 PB 数字更精准,因为不同行业的合理 PB 差异很大。",
    },
    {
        "key": "limit_up_1m",
        "label": "近 1 月有过涨停",
        "tunable": False,
        "why": "近期出现过涨停,说明有过强势资金推动,通常意味着主力关注或者有事件催化剂。是\"被发现\"的标志。",
    },
    {
        "key": "ma_alignment",
        "label": "5/10/20 日均线多头排列",
        "tunable": False,
        "why": "短期均线在长期均线之上(5>10>20),表示价格走势正处于健康上升通道,趋势已经被技术面确认。",
    },
    {
        "key": "main_fund_inflow",
        "label": "近 3 日主力连续净流入",
        "tunable": False,
        "why": "主力(大单、特大单)连续 3 日净买入,说明大资金在持续吸筹。\"散户跟着主力走\"的逻辑,趋势资金面强。",
    },
    {
        "key": "volume_expansion",
        "label": "近 3 日成交量较前 3 日放大",
        "tunable": True,
        "operator": ">",
        "unit": "%",
        "scale": 1,
        "value_key": "volume_expansion_pct",
        "default": 50,
        "presets": [20, 50, 100, 200],
        "why": "成交量突然放大,往往预示着拐点或者新趋势的启动。\"量在价先\",资金动作早于价格突破。",
    },
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


def extract_tunable_values(s: StockData, eval_result: dict) -> dict:
    """从已评估的 stock + values 中提取前端用的可调字段。"""
    vals = eval_result["values"]
    gap = None
    if s.debt_ratio is not None and s.industry_debt_avg and s.industry_debt_avg > 0:
        gap = (1 - s.debt_ratio / s.industry_debt_avg) * 100
    vol_pct = None
    v_ratio = vals.get("volume_expansion")
    if isinstance(v_ratio, (int, float)):
        vol_pct = (v_ratio - 1) * 100
    return {
        "market_cap": s.market_cap if s.market_cap > 0 else None,
        "turnover_3d_min": vals.get("turnover_3d"),
        "pb": s.pb,
        "roe_3y_min": vals.get("roe_3y"),
        "debt_vs_industry_gap": gap,
        "pe_percentile": vals.get("pe_percentile"),
        "dv_ttm": s.dv_ttm,
        "volume_expansion_pct": vol_pct,
    }
