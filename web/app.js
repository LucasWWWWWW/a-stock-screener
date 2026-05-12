const LS_DISABLED = "screener.disabled.v3";
const LS_THRESHOLDS = "screener.thresholds.v3";
const LS_USAGE = "screener.usage.v3";
const MIN_USAGE_TO_PROMOTE = 3;

const state = {
  data: null,
  disabled: new Set(JSON.parse(localStorage.getItem(LS_DISABLED) || "[]")),
  thresholds: JSON.parse(localStorage.getItem(LS_THRESHOLDS) || "{}"),
  usage: JSON.parse(localStorage.getItem(LS_USAGE) || "{}"),
  byCode: new Map(),
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
const fmtNum = (n, d = 2) => n == null || isNaN(n) ? "—" : Number(n).toFixed(d);

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
  if (!meta.tunable) return !!stock.criteria[meta.key];
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

function escapeHtml(s) {
  if (s == null) return "";
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}

// ====== Filter sidebar ======
function renderCriteria() {
  const list = document.getElementById("criteria-list");
  if (!list) return;
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
      renderListMain();
    });
  });
  list.querySelectorAll("button[data-set]").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      const k = e.currentTarget.dataset.set;
      const v = Number(e.currentTarget.dataset.v);
      state.thresholds[k] = v;
      persist();
      renderCriteria();
      renderListMain();
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
      renderListMain();
    });
  });
  list.querySelectorAll("input[data-custom-input]").forEach((inp) => {
    inp.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        list.querySelector(`button[data-custom-apply="${e.target.dataset.customInput}"]`).click();
      }
    });
  });
}

// ====== List view ======
function renderListMain() {
  const main = document.getElementById("main-content");
  if (!main) return;
  const filtered = state.data.stocks
    .filter(stockPasses)
    .sort((a, b) => passCount(b) - passCount(a));

  const resultCount = document.getElementById("result-count");
  if (resultCount) resultCount.textContent = `匹配 ${filtered.length} / ${state.data.stocks.length} 只`;

  if (filtered.length === 0) {
    main.innerHTML = `<div class="empty">没有满足当前条件的股票。尝试放宽阈值或取消勾选。</div>`;
    return;
  }
  const total = activeCount();
  main.innerHTML = `<div id="stocks-list">${
    filtered.map((s) => stockRow(s, total)).join("")
  }</div>`;
}

function stockRow(s, total) {
  const n = passCount(s);
  const m = s.metrics;
  const conceptsPreview = (s.concepts || []).slice(0, 3).map(c => escapeHtml(c)).join(" · ");
  return `
    <a class="stock-row" href="#/stock/${encodeURIComponent(s.code)}">
      <div class="row-left">
        <div class="row-title">
          <span class="stock-code">${s.code}</span>
          <span class="stock-name">${escapeHtml(s.name)}</span>
        </div>
        <div class="row-sub">
          <span class="stock-industry">${escapeHtml(s.industry || "未分类")}</span>
          ${conceptsPreview ? `<span class="row-concepts">${conceptsPreview}</span>` : ""}
        </div>
      </div>
      <div class="row-metrics">
        <div><label>市值</label><b>${fmtCN(m.market_cap)}</b></div>
        <div><label>PE</label><b>${fmtNum(m.pe_ttm)}</b></div>
        <div><label>PB</label><b>${fmtNum(m.pb)}</b></div>
        <div><label>股息</label><b>${fmtNum(m.dv_ttm)}%</b></div>
      </div>
      <div class="row-right">
        <span class="pass-badge">${n}/${total}</span>
        <span class="row-arrow">›</span>
      </div>
    </a>
  `;
}

// ====== Detail view ======
function renderDetailMain(code) {
  const main = document.getElementById("main-content");
  if (!main) return;
  const s = state.byCode.get(code);
  if (!s) {
    main.innerHTML = `<div class="empty">未找到股票 ${escapeHtml(code)}。<a href="#/">返回列表</a></div>`;
    return;
  }
  const m = s.metrics;
  const conceptTags = (s.concepts || []).map(c => `<span class="concept-tag">${escapeHtml(c)}</span>`).join("");

  const allChips = state.data.criteria_meta.map((meta) => {
    const pass = stockPassesCriterion(s, meta);
    let detail = "";
    if (meta.tunable) {
      const t = thresholdOf(meta);
      const raw = s.tunable_values?.[meta.value_key];
      const displayed = raw == null ? "—" : (raw / meta.scale).toFixed(2);
      detail = `<span class="chip-detail">${displayed} ${meta.operator} ${t}${meta.unit}</span>`;
    }
    return `<div class="crit-chip-row ${pass ? "pass" : "fail"}">
      <span class="crit-mark">${pass ? "✓" : "✗"}</span>
      <span class="crit-name">${escapeHtml(meta.label)}</span>
      ${detail}
    </div>`;
  }).join("");

  const adviceHtml = s.advice
    ? `<div class="advice-card">
         <h3>AI 投资建议</h3>
         <div class="advice-body">${escapeHtml(s.advice)}</div>
       </div>`
    : `<div class="advice-card empty-advice">AI 建议未生成 — 检查 ANTHROPIC_API_KEY 是否已配置。</div>`;

  main.innerHTML = `
    <div class="detail-page">
      <a class="back-link" href="#/">‹ 返回列表</a>
      <div class="detail-head">
        <div class="detail-title">
          <span class="stock-code">${s.code}</span>
          <span class="stock-name">${escapeHtml(s.name)}</span>
          <span class="stock-industry">${escapeHtml(s.industry || "未分类")}</span>
        </div>
        <div class="concept-tags">${conceptTags}</div>
      </div>

      <div class="detail-metrics">
        <div class="metric"><label>市值</label><b>${fmtCN(m.market_cap)}</b></div>
        <div class="metric"><label>PE-TTM</label><b>${fmtNum(m.pe_ttm)}</b></div>
        <div class="metric"><label>PB</label><b>${fmtNum(m.pb)}</b></div>
        <div class="metric"><label>股息率</label><b>${fmtNum(m.dv_ttm)}%</b></div>
        <div class="metric"><label>负债率</label><b>${fmtNum(m.debt_ratio)}%</b></div>
        <div class="metric"><label>行业均值PB</label><b>${fmtNum(m.industry_pb_avg)}</b></div>
        <div class="metric"><label>行业均值负债</label><b>${fmtNum(m.industry_debt_avg)}%</b></div>
      </div>

      ${adviceHtml}

      <div class="detail-criteria">
        <h3>13 项条件评估</h3>
        ${allChips}
      </div>
    </div>
  `;
}

// ====== Router ======
function route() {
  const hash = window.location.hash || "";
  const m = hash.match(/^#\/stock\/(.+)$/);
  if (m) {
    document.body.classList.add("detail-mode");
    renderDetailMain(decodeURIComponent(m[1]));
  } else {
    document.body.classList.remove("detail-mode");
    renderListMain();
  }
  window.scrollTo(0, 0);
}

async function init() {
  try {
    const res = await fetch("data/stocks.json", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.data = await res.json();
  } catch (e) {
    document.getElementById("generated-at").textContent = "数据未生成";
    document.getElementById("main-content").innerHTML =
      `<div class="empty">尚未生成数据文件。本地开发请运行 <code>python screener/fetch.py</code></div>`;
    return;
  }
  state.byCode = new Map((state.data.stocks || []).map(s => [s.code, s]));

  document.getElementById("generated-at").textContent = `更新: ${state.data.generated_at}`;

  if (state.data.error) {
    const cl = document.getElementById("criteria-list");
    if (cl) cl.innerHTML = "";
    document.getElementById("main-content").innerHTML =
      `<div class="empty"><strong>暂无数据</strong><br><br>${escapeHtml(state.data.error)}</div>`;
    document.getElementById("result-count").textContent = "0 只";
    return;
  }

  renderCriteria();
  route();

  window.addEventListener("hashchange", route);

  document.getElementById("check-all").addEventListener("click", () => {
    state.disabled.clear();
    persist();
    renderCriteria();
    if (!window.location.hash.startsWith("#/stock/")) renderListMain();
  });
  document.getElementById("uncheck-all").addEventListener("click", () => {
    state.disabled = new Set(state.data.criteria_meta.map((m) => m.key));
    persist();
    renderCriteria();
    if (!window.location.hash.startsWith("#/stock/")) renderListMain();
  });
  const resetBtn = document.getElementById("reset-thresholds");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      state.thresholds = {};
      persist();
      renderCriteria();
      if (!window.location.hash.startsWith("#/stock/")) renderListMain();
    });
  }
}

init();
