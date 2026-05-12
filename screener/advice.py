"""调用 Claude Haiku 4.5 为每只通过筛选的股票生成中文投资建议。"""

from __future__ import annotations
import os
import logging
import threading
import time
from anthropic import Anthropic

log = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
RPM = 45  # 留出对 50 RPM 限制的 headroom

SYSTEM_PROMPT = """你是一位中国 A 股市场分析师,根据用户提供的量化指标给出简洁的投资建议。

输出格式必须严格遵守:
亮点: <一句话,20字内>
风险: <一句话,20字内>
仓位: <轻仓/标准/重仓> · <一句话理由,20字内>

注意:
- 仅基于提供的指标推理,不要编造消息面/政策/具体业务进展
- 仓位建议依据:多数条件触发且估值低 → 重仓;条件参半 → 标准;仅少量触发或风险信号 → 轻仓
- 总字数控制在 80 字以内
- 这是辅助参考,不构成投资建议"""


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


def generate_advice(client: Anthropic | None, stock: dict) -> str:
    if client is None:
        return ""

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

请按系统提示格式输出建议。"""

    _limiter.acquire()
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=300,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_msg}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        log.error(f"生成建议失败 {stock['code']}: {str(e)[:120]}")
        return ""


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
