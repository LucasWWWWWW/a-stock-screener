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
