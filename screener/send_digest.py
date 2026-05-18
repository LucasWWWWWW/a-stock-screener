"""每日运行后发送 HTML 邮件简报给 subscribers.json 中的订阅者。

环境变量(GitHub Actions Secret):
  SMTP_FROM      发件邮箱(例如 you@gmail.com)
  SMTP_PASSWORD  邮箱应用密码(Gmail App Password)
  SMTP_HOST      默认 smtp.gmail.com
  SMTP_PORT      默认 587

任一缺失则跳过发送。
"""

from __future__ import annotations
import json
import os
import smtplib
import ssl
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SUBSCRIBERS_FILE = ROOT / "subscribers.json"
STOCKS_FILE = ROOT / "web" / "data" / "stocks.json"
SITE_URL = os.environ.get("SITE_URL", "https://lucaswwwwww.github.io/a-stock-screener/")


def build_email(data: dict) -> tuple[str, str]:
    trade_date = data.get("trade_date", "")
    total = len(data.get("stocks", []))
    mr = data.get("market_regime") or {}
    daily_script = data.get("daily_script", "")
    top10 = data.get("stocks", [])[:10]

    regime_label = {
        "bull": "🟢 牛市", "bear": "🔴 熊市",
        "transition": "🟡 中性", "unknown": "—",
    }.get(mr.get("regime", "unknown"), "—")
    tier_label = mr.get("tier_label", "")
    position_pct = mr.get("position_pct")
    pe_pct = mr.get("csi300_pe_percentile_5y")
    val_zone = mr.get("valuation_zone", "")

    market_html = ""
    if mr:
        market_html = f"""
        <table style="margin:14px 0; border-collapse:collapse; font-size:14px;">
          <tr>
            <td style="padding:6px 14px; background:#f0f2f5;">大盘状态</td>
            <td style="padding:6px 14px;">{regime_label} (SMA 比 {mr.get('csi300_sma_ratio', 0):.3f})</td>
          </tr>
          <tr>
            <td style="padding:6px 14px; background:#f0f2f5;">建议仓位</td>
            <td style="padding:6px 14px;"><b>{position_pct}%</b> · {tier_label} · {mr.get('tier_desc','')}</td>
          </tr>
          <tr>
            <td style="padding:6px 14px; background:#f0f2f5;">PE 5 年分位</td>
            <td style="padding:6px 14px;">{pe_pct:.1f}% ({val_zone})</td>
          </tr>
        </table>"""

    rows = []
    for s in top10:
        m = s.get("metrics", {})
        mc = (m.get("market_cap") or 0) / 1e8
        rows.append(
            f"""<tr>
                <td style="padding:6px 10px; font-family:monospace;">{s['code']}</td>
                <td style="padding:6px 10px;">{s['name']}</td>
                <td style="padding:6px 10px; color:#666;">{s.get('industry','')}</td>
                <td style="padding:6px 10px; text-align:right;">{s['n_pass']}/13</td>
                <td style="padding:6px 10px; text-align:right;">{mc:.1f}亿</td>
                <td style="padding:6px 10px; text-align:right;">{m.get('pe_ttm','—')}</td>
                <td style="padding:6px 10px; text-align:right;">{m.get('pb','—')}</td>
                <td style="padding:6px 10px; text-align:right;">{m.get('dv_ttm','—')}%</td>
              </tr>"""
        )

    subject = f"A股选股日报 · {trade_date} · 入选 {total} 只 · 建议仓位 {position_pct}%"
    html = f"""<!DOCTYPE html>
<html><body style="font-family:-apple-system,Helvetica,sans-serif; max-width:720px; margin:auto; padding:20px; color:#1a1f2e;">
  <h1 style="border-bottom:2px solid #ff6a3d; padding-bottom:10px;">A股选股 · 每日简报</h1>
  <p style="color:#666; font-size:13px;">{trade_date} · 共 {total} 只主题命中股</p>

  <h3 style="margin-top:24px;">📊 大盘天气</h3>
  {market_html}

  <h3 style="margin-top:24px;">🎙️ AI 主播日报</h3>
  <div style="background:#fff8f4; border-left:3px solid #ff6a3d; padding:12px 16px; line-height:1.8; font-size:14px;">
    {daily_script or '今日 AI 日报未生成。'}
  </div>

  <h3 style="margin-top:24px;">🏆 通过条数 Top 10</h3>
  <table style="width:100%; border-collapse:collapse; font-size:13px;">
    <thead><tr style="background:#f0f2f5;">
      <th style="padding:6px 10px;">代码</th>
      <th style="padding:6px 10px;">名称</th>
      <th style="padding:6px 10px;">行业</th>
      <th style="padding:6px 10px;">通过</th>
      <th style="padding:6px 10px;">市值</th>
      <th style="padding:6px 10px;">PE</th>
      <th style="padding:6px 10px;">PB</th>
      <th style="padding:6px 10px;">股息率</th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>

  <p style="margin-top:30px;">
    <a href="{SITE_URL}" style="background:#ff6a3d; color:#fff; padding:10px 18px; border-radius:4px; text-decoration:none;">查看完整网站 →</a>
  </p>

  <p style="margin-top:30px; font-size:11px; color:#888;">
    本邮件由 GitHub Actions 每日自动发送。
    本站仅提供量化信号汇总,投资有风险,决策需自负。
  </p>
</body></html>"""
    return subject, html


def main():
    smtp_from = os.environ.get("SMTP_FROM")
    smtp_pw = os.environ.get("SMTP_PASSWORD")
    if not smtp_from or not smtp_pw:
        print("SMTP_FROM / SMTP_PASSWORD 未配置,跳过邮件发送")
        return 0
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    if not SUBSCRIBERS_FILE.exists():
        print("subscribers.json 不存在,跳过")
        return 0
    subs = json.loads(SUBSCRIBERS_FILE.read_text(encoding="utf-8"))
    if not subs:
        print("订阅者列表为空,跳过")
        return 0

    if not STOCKS_FILE.exists():
        print("stocks.json 不存在,跳过")
        return 0
    data = json.loads(STOCKS_FILE.read_text(encoding="utf-8"))
    if not data.get("stocks"):
        print("stocks 为空,跳过")
        return 0

    subject, html = build_email(data)

    ctx = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port) as srv:
        srv.starttls(context=ctx)
        srv.login(smtp_from, smtp_pw)
        for to in subs:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = smtp_from
            msg["To"] = to
            msg.attach(MIMEText(html, "html", "utf-8"))
            srv.send_message(msg)
            print(f"sent: {to}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
