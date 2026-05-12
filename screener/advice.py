"""调用 Claude Haiku 4.5 为每只通过筛选的股票生成中文投资建议(组合输出 + 看多看空)。"""

from __future__ import annotations
import json
import os
import logging
import re
import threading
import time
from anthropic import Anthropic

log = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
RPM = 45  # 留出对 50 RPM 限制的 headroom

SYSTEM_PROMPT = """你是一位中国 A 股市场分析师,基于用户提供的量化指标输出结构化 JSON。

必须输出**严格合法的 JSON**(不要 markdown 代码块,不要前后多余文字),格式:
{
  "intro": "<25 字内说清这家公司主营业务,例如 \\"做光伏组件的硅片厂\\">",
  "highlight": "<一句亮点 20 字内>",
  "risk": "<一句风险 20 字内>",
  "position": "<轻仓/标准/重仓> · <一句理由 15 字内>",
  "bull": ["<买入理由1 25字内>", "<买入理由2 25字内>", "<买入理由3 25字内>"],
  "bear": ["<反对理由1 25字内>", "<反对理由2 25字内>", "<反对理由3 25字内>"]
}

要求:
- 仅基于提供的指标推理,不编造消息面/政策/具体业务细节
- intro 必须是公司主营业务的大白话,30 字内,小白能懂
- bull/bear 各 3 条,从数据出发的真实利弊,不要凑数空话
- 不要 emoji、不要叹号、不要 markdown
- 严格只输出 JSON,不要任何其他文字"""


class _RateLimiter:
    """跨线程的全局节流器:保证两次 acquire() 之间的间隔 ≥ 60/RPM 秒。"""

    def __init__(self, rpm: int):
        self.interval = 60.0 / rpm
        self.next_allowed = 0.0
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            wait = self.next_allowed - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self.next_allowed = now + self.interval


_limiter = _RateLimiter(RPM)


def make_client() -> Anthropic | None:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        log.warning("ANTHROPIC_API_KEY 未设置,投资建议将留空")
        return None
    return Anthropic(api_key=key, max_retries=5)


def _empty_payload() -> dict:
    return {
        "intro": "",
        "highlight": "",
        "risk": "",
        "position": "",
        "bull": [],
        "bear": [],
        "advice_text": "",
    }


def _format_advice_text(p: dict) -> str:
    parts = []
    if p.get("highlight"):
        parts.append(f"亮点: {p['highlight']}")
    if p.get("risk"):
        parts.append(f"风险: {p['risk']}")
    if p.get("position"):
        parts.append(f"仓位: {p['position']}")
    return "\n".join(parts)


def _parse_json_loose(text: str) -> dict | None:
    """容忍 Claude 偶尔加 markdown 代码块或多余文字。"""
    if not text:
        return None
    # strip markdown fences
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def generate_advice(client: Anthropic | None, stock: dict) -> dict:
    """返回 dict 含 intro/highlight/risk/position/bull/bear/advice_text。"""
    if client is None:
        return _empty_payload()

    passed_conditions = [k for k, v in stock["criteria"].items() if v]
    metrics = stock["metrics"]
    concepts = stock.get("concepts", [])

    user_msg = f"""股票: {stock['code']} {stock['name']}
行业: {stock.get('industry', '未知')}
命中概念: {', '.join(concepts) if concepts else '无'}

关键指标:
- 市值: {metrics.get('market_cap', 0)/1e8:.1f}亿
- PE-TTM: {metrics.get('pe_ttm')}
- PB: {metrics.get('pb')}
- 股息率: {metrics.get('dv_ttm')}%
- 资产负债率: {metrics.get('debt_ratio')}

通过的条件 ({len(passed_conditions)}/13): {', '.join(passed_conditions)}

请按系统提示输出 JSON。"""

    _limiter.acquire()
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=800,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_msg}],
        )
        text = resp.content[0].text.strip()
        parsed = _parse_json_loose(text)
        if not parsed:
            log.warning(f"  {stock['code']} JSON 解析失败,raw: {text[:120]}")
            return _empty_payload()
        out = {
            "intro": str(parsed.get("intro", "")).strip()[:60],
            "highlight": str(parsed.get("highlight", "")).strip(),
            "risk": str(parsed.get("risk", "")).strip(),
            "position": str(parsed.get("position", "")).strip(),
            "bull": [str(x).strip() for x in (parsed.get("bull") or [])[:3]],
            "bear": [str(x).strip() for x in (parsed.get("bear") or [])[:3]],
        }
        out["advice_text"] = _format_advice_text(out)
        return out
    except Exception as e:
        log.error(f"生成建议失败 {stock['code']}: {str(e)[:120]}")
        return _empty_payload()


DAILY_SCRIPT_SYSTEM = """你是 AI 选股助手的主播,根据今日筛选结果用口语化中文播报。
要求:
- 长度严格在 150~200 字之间(对应朗读 60 秒左右)
- 自然流畅,像电台主播,不要罗列指标
- 包含:今日命中股票总数、最强主题/行业、1~2 只重点股票(代码+名称+一句亮点)、整体市场情绪、免责一句
- 不要 emoji、不要 markdown、不要书名号引号
- 句末用句号或问号,不要叹号"""


def generate_daily_script(client: Anthropic | None, stocks: list[dict], trade_date: str) -> str:
    if client is None or not stocks:
        return ""
    from collections import Counter

    top10 = stocks[:10]
    industries = Counter(s.get("industry", "") for s in stocks).most_common(3)
    concepts = Counter(c for s in stocks for c in (s.get("concepts") or [])).most_common(3)

    top_lines = []
    for s in top10:
        m = s.get("metrics", {})
        top_lines.append(
            f"{s['code']} {s['name']} ({s.get('industry', '')}): 通过 {s['n_pass']}/13, "
            f"PE={m.get('pe_ttm')}, PB={m.get('pb')}, 股息率={m.get('dv_ttm')}%"
        )
    user_msg = f"""今天是 {trade_date},筛选条件命中股票 {len(stocks)} 只。

最强 10 只:
{chr(10).join(top_lines)}

集中行业 top3: {industries}
集中概念 top3: {concepts}

请按系统提示要求,写一段 150~200 字的口语化播报,作为今日 A 股选股日报。"""

    _limiter.acquire()
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=500,
            system=[{
                "type": "text",
                "text": DAILY_SCRIPT_SYSTEM,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": user_msg}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        log.error(f"生成日报失败: {str(e)[:120]}")
        return ""
