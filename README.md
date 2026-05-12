# A股选股网站

按 14 项量化条件每天自动筛选 A股,网页展示并支持条件勾选/取消,带 Claude 生成的中文投资建议。

## 14 项筛选条件

1. 市值 < 200 亿
2. 近 3 天换手率每天 ≥ 5%
3. PB < 10
4. ROE 连续 3 年 > 12%
5. 经营现金流连续 2 年 > 净利润(用每股口径对比)
6. 资产负债率低于行业均值 20%+
7. 命中目标行业/概念(能源/安全/高股息/硬科技/AI/存储/芯片/光通信/算力租赁 任一)
8. PE 5 年百分位 < 50%
9. 股息率 > 2%
10. PB < 行业均值
11. 近 1 月有过涨停
12. 5/10/20 日均线多头排列且收盘 > MA20
13. 近 3 日主力资金连续净流入
14. 近 3 日成交量较前 3 日放大 50%+

## 架构

- **数据**: Tushare Pro(需 token,从全球任意网络可访问)
- **筛选**: GitHub Actions 每日北京时间 18:00 跑 `screener/fetch.py`
- **投资建议**: Claude Haiku 4.5,在 Actions 内生成,baked 进 JSON
- **前端**: 静态 SPA,读 `web/data/stocks.json`,客户端按 checkbox 过滤
- **部署**: Netlify(`web/` 为发布目录),仓库 push 自动重建

## 关于 Tushare 积分

部分高级接口(ROE / 现金流 / 净利润 / 主力资金流)需要 ≥5000 积分。
免费用户(默认 2000 积分,完善资料可加到 ~2500)调用这些接口会被拒,
对应的筛选条件会被跳过(JSON 里 `unavailable_endpoints` 字段会标出)。

提高积分的免费途径:在 tushare.pro 个人主页填写学校/单位/手机/邮箱 → 加 100~500 积分。
仍不够时可以付费(约 ¥200/年解锁全部)。

## 本地开发

```powershell
cd C:\股票线上网站定制

# 安装依赖
python -m pip install -r screener/requirements.txt

# 配置 token(到 tushare.pro 注册后在个人中心拿)
Copy-Item .env.example .env
# 编辑 .env,填入 TUSHARE_TOKEN 和 ANTHROPIC_API_KEY

# 跑筛选(开发模式,只处理前 20 只候选)
python screener/fetch.py --limit 20

# 跳过 Claude API(零成本测试)
python screener/fetch.py --limit 20 --skip-advice

# 本地预览
python -m http.server 8000 -d web
# 浏览器打开 http://localhost:8000
```

## 部署到 Netlify

1. `git init && git add . && git commit -m "init"`
2. 在 GitHub 创建仓库,`git remote add origin <url>`,`git push -u origin main`
3. 在 GitHub 仓库 Settings → Secrets and variables → Actions 添加:
   - `TUSHARE_TOKEN`
   - `ANTHROPIC_API_KEY`
4. 在 Netlify 中 "Import from Git" 选这个仓库,publish 目录会从 `netlify.toml` 自动读到 `web/`
5. 触发一次 Actions 手动运行,等 push 完成后 Netlify 自动重建

## 自定义

- **改条件阈值**: `screener/criteria.py` 里每个 `c*` 函数
- **加/删行业概念**: `screener/criteria.py` 顶部 `CONCEPT_KEYWORDS`
- **改建议风格**: `screener/advice.py` 里的 `SYSTEM_PROMPT`
- **改更新时间**: `.github/workflows/daily.yml` 的 `cron`(UTC,+8h 得北京时间)

## 风险提示

本站仅基于公开数据做量化信号汇总,不构成投资建议。市场有风险,决策需自负。
