const LS_DISABLED = "screener.disabled.v2";
const LS_THRESHOLDS = "screener.thresholds.v2";
const LS_USAGE = "screener.usage.v2";
const MIN_USAGE_TO_PROMOTE = 3;

const state = {
  data: null,
  disabled: new Set(JSON.parse(localStorage.getItem(LS_DISABLED) || "[]")),
  thresholds: JSON.parse(localStorage.getItem(LS_THRESHOLDS) || "{}"),
  usage: JSON.parse(localStorage.getItem(LS_USAGE) || "{}"),
};

const persist = () => {
  localStorage.setItem(LS_DISABLED, JSON.stringify([...state.disabled]));
  localStorage.setItem(LS_THRESHOLDS, JSON.stringify(state.thresholds));
  localStorage.setItem(LS_USAGE, JSON.stringify(state.usage));
};

const OP_FN = {
  "<": (a, b) => a < b,
  "<=": (a, b) => a <= b,
  ">": (a, b) => a > b,
  ">=": (a, b) => a >= b,
};

const fmtCN = (n) => {
  if (n == null || isNaN(n)) return "—";
  if (Math.abs(n) >= 1e8) return (n / 1e8).toFixed(1) + "亿";
  if (Math.abs(n) >= 1e4) return (n / 1e4).toFixed(1) + "万";
  return Number(n).toLocaleString("zh-CN", { maximumFractionDigits: 2 });
};
const fmtNum = (n, d = 2) =>
  n == null || isNaN(n) ? "—" : Number(n).toFixed(d);

function thresholdOf(meta) {
  if (!meta.tunable) return null;
  return state.thresholds[meta.key] ?? meta.default;
}

function presetsFor(meta) {
  const usage = state.usage[meta.key] || {};
  const learned = Object.entries(usage)
    .filter(([v, c]) => c >= MIN_USAGE_TO_PROMOTE && !meta.presets.includes(Number(v)))
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([v]) => Number(v));
  return [...new Set([...meta.presets, ...learned])].sort((a, b) => a - b);
}

function bumpUsage(key, value) {
  if (!state.usage[key]) state.usage[key] = {};
  const k = String(value);
  state.usage[key][k] = (state.usage[key][k] || 0) + 1;
}

function stockPassesCriterion(stock, meta) {
  if (!meta.tunable) {
    return !!stock.criteria[meta.key];
  }
  const raw = stock.tunable_values?.[meta.value_key];
  if (raw == null) return false;
  const threshold = thresholdOf(meta) * meta.scale;
  return OP_FN[meta.operator](raw, threshold);
}

function passCount(stock) {
  let n = 0;
  for (const meta of state.data.criteria_meta) {
    if (state.disabled.has(meta.key)) continue;
    if (stockPassesCriterion(stock, meta)) n++;
  }
  return n;
}

function activeCount() {
  return state.data.criteria_meta.filter((m) => !state.disabled.has(m.key)).length;
}

function stockPasses(stock) {
  for (const meta of state.data.criteria_meta) {
    if (state.disabled.has(meta.key)) continue;
    if (!stockPassesCriterion(stock, meta)) return false;
  }
  return true;
}

function renderCriteria() {
  const list = document.getElementById("criteria-list");
  list.innerHTML = "";
  for (const meta of state.data.criteria_meta) {
    const enabled = !state.disabled.has(meta.key);
    const li = document.createElement("li");
    li.className = "criterion" + (enabled ? "" : " off");

    let inner = `
      <div class="crit-head">
        <label class="crit-toggle">
          <input type="checkbox" data-toggle="${meta.key}" ${enabled ? "checked" : ""}>
          <span class="crit-label">${escapeHtml(meta.label)}${meta.tunable ? ` <span class="crit-op">${meta.operator}</span>` : ""}</span>
        </label>`;

    if (meta.tunable) {
      const current = thresholdOf(meta);
      const presets = presetsFor(meta);
      const chips = presets.map((v) => {
        const sel = Number(v) === Number(current);
        return `<button class="chip${sel ? " sel" : ""}" data-set="${meta.key}" data-v="${v}">${v}${meta.unit}</button>`;
      }).join("");
      inner += `
        <div class="crit-controls">
          <div class="chips">${chips}</div>
          <div class="custom-input">
            <input type="number" step="any" placeholder="自定义" data-custom-input="${meta.key}">
            <span class="unit">${meta.unit}</span>
            <button class="apply" data-custom-apply="${meta.key}">应用</button>
          </div>
        </div>`;
    }
    inner += `</div>`;
    li.innerHTML = inner;
    list.appendChild(li);
  }

  list.querySelectorAll("input[data-toggle]").forEach((cb) => {
    cb.addEventListener("change", (e) => {
      const k = e.target.dataset.toggle;
      if (e.target.checked) state.disabled.delete(k);
      else state.disabled.add(k);
      persist();
      renderCriteria();
      renderStocks();
    });
  });
  list.querySelectorAll("button[data-set]").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      const k = e.currentTarget.dataset.set;
      const v = Number(e.currentTarget.dataset.v);
      state.thresholds[k] = v;
      persist();
      renderCriteria();
      renderStocks();
    });
  });
  list.querySelectorAll("button[data-custom-apply]").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      const k = e.currentTarget.dataset.customApply;
      const input = list.querySelector(`input[data-custom-input="${k}"]`);
      const v = Number(input.value);
      if (!input.value || isNaN(v)) return;
      const meta = state.data.criteria_meta.find((m) => m.key === k);
      state.thresholds[k] = v;
      if (!meta.presets.includes(v)) bumpUsage(k, v);
      persist();
      input.value = "";
      renderCriteria();
      renderStocks();
    });
  });
  list.querySelectorAll("input[data-custom-input]").forEach((inp) => {
    inp.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        const k = e.target.dataset.customInput;
        list.querySelector(`button[data-custom-apply="${k}"]`).click();
      }
    });
  });
}

function renderStocks() {
  const list = document.getElementById("stocks-list");
  const filtered = state.data.stocks
    .filter(stockPasses)
    .sort((a, b) => passCount(b) - passCount(a));

  document.getElementById("result-count").textContent =
    `匹配 ${filtered.length} / ${state.data.stocks.length} 只`;

  if (filtered.length === 0) {
    list.innerHTML = `<div class="empty">没有满足当前条件的股票。尝试放宽某些阈值或取消勾选。</div>`;
    return;
  }
  list.innerHTML = filtered.map(stockCard).join("");
}

function stockCard(s) {
  const m = s.metrics;
  const concepts = (s.concepts || [])
    .slice(0, 6)
    .map((c) => `<span class="concept-tag">${escapeHtml(c)}</span>`)
    .join("");

  const total = activeCount();
  const n = passCount(s);

  const critChips = state.data.criteria_meta
    .filter((meta) => !state.disabled.has(meta.key))
    .map((meta) => {
      const pass = stockPassesCriterion(s, meta);
      return `<span class="crit-chip ${pass ? "pass" : "fail"}">${pass ? "✓" : "✗"} ${escapeHtml(meta.label)}</span>`;
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
        <span class="pass-badge">${n}/${total} 项</span>
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

  if (state.data.error) {
    document.getElementById("criteria-list").innerHTML = "";
    document.getElementById("stocks-list").innerHTML =
      `<div class="empty"><strong>暂无数据</strong><br><br>${escapeHtml(state.data.error)}</div>`;
    document.getElementById("result-count").textContent = "0 只";
    return;
  }

  renderCriteria();
  renderStocks();

  document.getElementById("check-all").addEventListener("click", () => {
    state.disabled.clear();
    persist();
    renderCriteria();
    renderStocks();
  });
  document.getElementById("uncheck-all").addEventListener("click", () => {
    state.disabled = new Set(state.data.criteria_meta.map((m) => m.key));
    persist();
    renderCriteria();
    renderStocks();
  });
  const resetBtn = document.getElementById("reset-thresholds");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      state.thresholds = {};
      persist();
      renderCriteria();
      renderStocks();
    });
  }
}

init();
