const LS_KEY = "ashare-screener-disabled-criteria";

const state = {
  data: null,
  disabled: new Set(JSON.parse(localStorage.getItem(LS_KEY) || "[]")),
};

const fmtCN = (n) => {
  if (n == null || isNaN(n)) return "—";
  if (Math.abs(n) >= 1e8) return (n / 1e8).toFixed(1) + "亿";
  if (Math.abs(n) >= 1e4) return (n / 1e4).toFixed(1) + "万";
  return Number(n).toLocaleString("zh-CN", { maximumFractionDigits: 2 });
};

const fmtNum = (n, digits = 2) =>
  n == null || isNaN(n) ? "—" : Number(n).toFixed(digits);

function saveDisabled() {
  localStorage.setItem(LS_KEY, JSON.stringify([...state.disabled]));
}

function renderCriteria() {
  const list = document.getElementById("criteria-list");
  list.innerHTML = "";
  for (const c of state.data.criteria_meta) {
    const enabled = !state.disabled.has(c.key);
    const li = document.createElement("li");
    li.innerHTML = `
      <label>
        <input type="checkbox" data-key="${c.key}" ${enabled ? "checked" : ""}>
        <span>${c.label}</span>
      </label>
    `;
    list.appendChild(li);
  }
  list.querySelectorAll("input[type=checkbox]").forEach((cb) => {
    cb.addEventListener("change", (e) => {
      const k = e.target.dataset.key;
      if (e.target.checked) state.disabled.delete(k);
      else state.disabled.add(k);
      saveDisabled();
      renderStocks();
    });
  });
}

function stockPasses(stock) {
  for (const c of state.data.criteria_meta) {
    if (state.disabled.has(c.key)) continue;
    if (!stock.criteria[c.key]) return false;
  }
  return true;
}

function renderStocks() {
  const list = document.getElementById("stocks-list");
  const filtered = state.data.stocks.filter(stockPasses);

  document.getElementById("result-count").textContent =
    `匹配 ${filtered.length} 只`;

  if (filtered.length === 0) {
    list.innerHTML = `<div class="empty">没有满足当前条件的股票。尝试取消勾选几个条件。</div>`;
    return;
  }

  const html = filtered.map((s) => stockCard(s)).join("");
  list.innerHTML = html;
}

function stockCard(s) {
  const m = s.metrics;
  const concepts = (s.concepts || [])
    .slice(0, 6)
    .map((c) => `<span class="concept-tag">${escapeHtml(c)}</span>`)
    .join("");

  const critChips = state.data.criteria_meta
    .map((c) => {
      const cls = s.criteria[c.key] ? "pass" : "fail";
      const mark = s.criteria[c.key] ? "✓" : "✗";
      return `<span class="crit-chip ${cls}">${mark} ${escapeHtml(c.label)}</span>`;
    })
    .join("");

  const adviceBlock = s.advice
    ? `<div class="advice"><span class="advice-label">投资建议</span>${escapeHtml(s.advice)}</div>`
    : "";

  return `
    <div class="stock-card">
      <div class="stock-head">
        <div class="stock-title">
          <span class="stock-code">${s.code}</span>
          <span class="stock-name">${escapeHtml(s.name)}</span>
          <span class="stock-industry">${escapeHtml(s.industry || "")}</span>
        </div>
        <span class="pass-badge">${s.n_pass}/14 项</span>
      </div>
      <div class="concept-tags">${concepts}</div>
      <div class="metrics">
        <div class="metric"><span class="metric-label">市值</span><span class="metric-value">${fmtCN(m.market_cap)}</span></div>
        <div class="metric"><span class="metric-label">PE-TTM</span><span class="metric-value">${fmtNum(m.pe_ttm)}</span></div>
        <div class="metric"><span class="metric-label">PB</span><span class="metric-value">${fmtNum(m.pb)}</span></div>
        <div class="metric"><span class="metric-label">股息率</span><span class="metric-value">${fmtNum(m.dv_ttm)}%</span></div>
        <div class="metric"><span class="metric-label">负债率</span><span class="metric-value">${fmtNum(m.debt_ratio)}%</span></div>
        <div class="metric"><span class="metric-label">行业均值PB</span><span class="metric-value">${fmtNum(m.industry_pb_avg)}</span></div>
      </div>
      <div class="criteria-summary">${critChips}</div>
      ${adviceBlock}
    </div>
  `;
}

function escapeHtml(s) {
  if (s == null) return "";
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}

async function init() {
  try {
    const res = await fetch("data/stocks.json", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.data = await res.json();
  } catch (e) {
    document.getElementById("generated-at").textContent = "数据未生成";
    document.getElementById("stocks-list").innerHTML =
      `<div class="empty">尚未生成数据文件。<br>本地开发请运行:<code>python screener/fetch.py</code></div>`;
    return;
  }
  document.getElementById("generated-at").textContent =
    `更新: ${state.data.generated_at}`;

  renderCriteria();
  if (state.data.error) {
    document.getElementById("stocks-list").innerHTML =
      `<div class="empty"><strong>暂无数据</strong><br><br>${escapeHtml(state.data.error)}</div>`;
    document.getElementById("result-count").textContent = "0 只";
    return;
  }
  renderStocks();

  document.getElementById("check-all").addEventListener("click", () => {
    state.disabled.clear();
    saveDisabled();
    renderCriteria();
    renderStocks();
  });
  document.getElementById("uncheck-all").addEventListener("click", () => {
    state.disabled = new Set(state.data.criteria_meta.map((c) => c.key));
    saveDisabled();
    renderCriteria();
    renderStocks();
  });
}

init();
