const LS_DISABLED = "screener.disabled.v3";
const LS_THRESHOLDS = "screener.thresholds.v3";
const LS_USAGE = "screener.usage.v3";
const LS_FAVORITES = "screener.fav.v3";
const LS_PORTFOLIO = "screener.portfolio.v3";
const LS_LAST_SEEN = "screener.lastseen.v3";
const MIN_USAGE_TO_PROMOTE = 3;

const state = {
  data: null,
  disabled: new Set(JSON.parse(localStorage.getItem(LS_DISABLED) || "[]")),
  thresholds: JSON.parse(localStorage.getItem(LS_THRESHOLDS) || "{}"),
  usage: JSON.parse(localStorage.getItem(LS_USAGE) || "{}"),
  favorites: new Set(JSON.parse(localStorage.getItem(LS_FAVORITES) || "[]")),
  portfolio: (localStorage.getItem(LS_PORTFOLIO) || "").split(/[\s,，\n]+/).filter(Boolean),
  byCode: new Map(),
  search: "",
  sort: "n_pass",
  tab: "all", // "all" | "fav" | "portfolio"
  currentList: [], // for detail page next/prev nav
};

const persist = () => {
  localStorage.setItem(LS_DISABLED, JSON.stringify([...state.disabled]));
  localStorage.setItem(LS_THRESHOLDS, JSON.stringify(state.thresholds));
  localStorage.setItem(LS_USAGE, JSON.stringify(state.usage));
  localStorage.setItem(LS_FAVORITES, JSON.stringify([...state.favorites]));
  localStorage.setItem(LS_PORTFOLIO, state.portfolio.join(","));
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

// ============== URL SHARE ==============
function encodeShareState() {
  const payload = {
    d: [...state.disabled],
    t: state.thresholds,
  };
  const json = JSON.stringify(payload);
  return btoa(unescape(encodeURIComponent(json))).replace(/=+$/, "").replace(/\+/g, "-").replace(/\//g, "_");
}
function decodeShareState(s) {
  try {
    const b64 = s.replace(/-/g, "+").replace(/_/g, "/");
    const pad = b64.length % 4;
    const padded = b64 + "===".slice(0, pad ? 4 - pad : 0);
    const json = decodeURIComponent(escape(atob(padded)));
    const p = JSON.parse(json);
    if (Array.isArray(p.d)) state.disabled = new Set(p.d);
    if (p.t && typeof p.t === "object") state.thresholds = p.t;
    persist();
    return true;
  } catch (e) {
    console.warn("decode share state failed", e);
    return false;
  }
}
function shareLink() {
  const url = new URL(window.location.href);
  url.hash = "#/s/" + encodeShareState();
  return url.toString();
}
async function copyShare() {
  const url = shareLink();
  try {
    await navigator.clipboard.writeText(url);
    flashBtn(document.getElementById("share-btn"), "✓ 已复制");
  } catch {
    prompt("复制以下链接:", url);
  }
}
function flashBtn(btn, txt) {
  const orig = btn.innerHTML;
  btn.innerHTML = txt;
  btn.disabled = true;
  setTimeout(() => {
    btn.innerHTML = orig;
    btn.disabled = false;
  }, 1500);
}

// ============== AI 主播日报 ==============
const tts = {
  utter: null,
  playing: false,
};
function pickChineseVoice() {
  const voices = window.speechSynthesis.getVoices();
  return voices.find(v => /zh|Chinese|Mandarin/i.test(v.lang + v.name))
      || voices.find(v => v.lang.startsWith("zh"))
      || voices[0];
}
function toggleBroadcast() {
  const btn = document.getElementById("broadcast-btn");
  const transcript = document.getElementById("broadcast-transcript");
  if (!state.data?.daily_script) {
    transcript.classList.remove("hidden");
    transcript.textContent = "今日 AI 日报暂未生成(可能 ANTHROPIC_API_KEY 未配置或调用失败)。";
    return;
  }
  if (tts.playing) {
    window.speechSynthesis.cancel();
    tts.playing = false;
    btn.querySelector(".bc-icon").textContent = "▶";
    btn.querySelector(".bc-text").textContent = "AI 主播日报";
    return;
  }
  if (!window.speechSynthesis) {
    transcript.classList.remove("hidden");
    transcript.textContent = "浏览器不支持语音合成,请阅读以下文本:\n\n" + state.data.daily_script;
    return;
  }
  const u = new SpeechSynthesisUtterance(state.data.daily_script);
  u.lang = "zh-CN";
  u.rate = 1.05;
  u.pitch = 1;
  const voice = pickChineseVoice();
  if (voice) u.voice = voice;
  u.onstart = () => {
    tts.playing = true;
    btn.querySelector(".bc-icon").textContent = "⏸";
    btn.querySelector(".bc-text").textContent = "正在播报…";
    transcript.classList.remove("hidden");
    transcript.textContent = state.data.daily_script;
  };
  u.onend = () => {
    tts.playing = false;
    btn.querySelector(".bc-icon").textContent = "▶";
    btn.querySelector(".bc-text").textContent = "AI 主播日报";
  };
  tts.utter = u;
  window.speechSynthesis.speak(u);
}

// ============== Filter sidebar ==============
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

// ============== Sparkline SVG ==============
function sparkline(values, w = 90, h = 26) {
  if (!values || values.length < 2) return "";
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const stepX = w / (values.length - 1);
  const points = values
    .map((v, i) => `${(i * stepX).toFixed(1)},${(h - ((v - min) / range) * h).toFixed(1)}`)
    .join(" ");
  const last = values[values.length - 1];
  const first = values[0];
  const up = last >= first;
  const stroke = up ? "var(--green)" : "var(--red)";
  return `<svg class="sparkline" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
    <polyline fill="none" stroke="${stroke}" stroke-width="1.4" points="${points}"/>
  </svg>`;
}

// ============== Radar (DNA) ==============
function radarSVG(stock) {
  const metas = state.data.criteria_meta;
  const n = metas.length;
  const size = 260;
  const cx = size / 2;
  const cy = size / 2;
  const R = size / 2 - 38;

  const points = [];
  const labels = [];
  metas.forEach((meta, i) => {
    const angle = (-Math.PI / 2) + (2 * Math.PI * i / n);
    const pass = stockPassesCriterion(stock, meta) ? 1 : 0.15;
    const x = cx + Math.cos(angle) * R * pass;
    const y = cy + Math.sin(angle) * R * pass;
    points.push(`${x.toFixed(1)},${y.toFixed(1)}`);
    const lx = cx + Math.cos(angle) * (R + 18);
    const ly = cy + Math.sin(angle) * (R + 18);
    labels.push({ x: lx, y: ly, text: meta.label, pass: pass === 1 });
  });

  const grid = [0.2, 0.4, 0.6, 0.8, 1.0].map(scale => {
    const pts = [];
    for (let i = 0; i < n; i++) {
      const a = (-Math.PI / 2) + (2 * Math.PI * i / n);
      pts.push(`${cx + Math.cos(a) * R * scale},${cy + Math.sin(a) * R * scale}`);
    }
    return `<polygon points="${pts.join(" ")}" fill="none" stroke="var(--border)" stroke-width="0.7"/>`;
  }).join("");

  const axisLines = metas.map((_, i) => {
    const a = (-Math.PI / 2) + (2 * Math.PI * i / n);
    const x2 = cx + Math.cos(a) * R;
    const y2 = cy + Math.sin(a) * R;
    return `<line x1="${cx}" y1="${cy}" x2="${x2}" y2="${y2}" stroke="var(--border)" stroke-width="0.5"/>`;
  }).join("");

  const labelsHTML = labels.map(l => {
    const anchor = Math.abs(l.x - cx) < 5 ? "middle" : (l.x < cx ? "end" : "start");
    return `<text x="${l.x}" y="${l.y}" text-anchor="${anchor}" dominant-baseline="middle" font-size="10" fill="${l.pass ? 'var(--text)' : 'var(--muted)'}">${escapeHtml(l.text)}</text>`;
  }).join("");

  return `<svg class="radar" viewBox="0 0 ${size} ${size}" width="${size}" height="${size}">
    ${grid}
    ${axisLines}
    <polygon points="${points.join(" ")}" fill="var(--accent)" fill-opacity="0.25" stroke="var(--accent)" stroke-width="1.5"/>
    ${labelsHTML}
  </svg>`;
}

// ============== List view ==============
function sortFn(a, b) {
  switch (state.sort) {
    case "n_pass": return passCount(b) - passCount(a);
    case "market_cap_asc": return (a.metrics.market_cap || 0) - (b.metrics.market_cap || 0);
    case "market_cap_desc": return (b.metrics.market_cap || 0) - (a.metrics.market_cap || 0);
    case "pe_asc": {
      const av = a.metrics.pe_ttm ?? Infinity;
      const bv = b.metrics.pe_ttm ?? Infinity;
      return av - bv;
    }
    case "pb_asc": {
      const av = a.metrics.pb ?? Infinity;
      const bv = b.metrics.pb ?? Infinity;
      return av - bv;
    }
    case "dv_desc": return (b.metrics.dv_ttm ?? 0) - (a.metrics.dv_ttm ?? 0);
    case "code": return a.code.localeCompare(b.code);
    default: return 0;
  }
}

function renderListMain() {
  const main = document.getElementById("main-content");
  if (!main) return;
  const toolbar = document.getElementById("list-toolbar");
  if (toolbar) toolbar.classList.remove("hidden");

  document.getElementById("fav-count").textContent = state.favorites.size;

  // Portfolio tab is a different UI
  if (state.tab === "portfolio") {
    renderPortfolioTab(main);
    return;
  }

  const q = state.search.trim().toLowerCase();
  let stocks = state.data.stocks;
  if (state.tab === "fav") {
    stocks = stocks.filter(s => state.favorites.has(s.code));
  }
  let filtered = stocks.filter(stockPasses);
  if (q) {
    filtered = filtered.filter(s =>
      s.code.toLowerCase().includes(q) ||
      (s.name || "").toLowerCase().includes(q) ||
      (s.industry || "").toLowerCase().includes(q)
    );
  }
  filtered.sort(sortFn);

  // Remember currentList order for detail page swipe navigation
  state.currentList = filtered.map(s => s.code);

  const resultCount = document.getElementById("result-count");
  if (resultCount) {
    const base = state.tab === "fav" ? state.favorites.size : state.data.stocks.length;
    resultCount.textContent = `匹配 ${filtered.length} / ${base} 只`;
  }

  if (filtered.length === 0) {
    main.innerHTML = `<div class="empty">${state.tab === "fav" ? "你还没有收藏任何股票。点击列表中的 ⭐ 加入收藏。" : "没有满足当前条件的股票。尝试放宽阈值或取消勾选。"}</div>`;
    return;
  }
  const total = activeCount();
  const heatmap = state.tab === "all" && !q ? heatmapHTML() : "";
  main.innerHTML = `${heatmap}<div id="stocks-list">${
    filtered.map((s) => stockRow(s, total)).join("")
  }</div>`;
  main.querySelectorAll("[data-fav]").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      const code = e.currentTarget.dataset.fav;
      if (state.favorites.has(code)) state.favorites.delete(code);
      else state.favorites.add(code);
      persist();
      renderListMain();
    });
  });
  main.querySelectorAll("[data-heat-industry]").forEach((el) => {
    el.addEventListener("click", (e) => {
      const ind = e.currentTarget.dataset.heatIndustry;
      document.getElementById("search-input").value = ind;
      state.search = ind;
      renderListMain();
    });
  });
}

// ============== Industry heatmap ==============
function heatmapHTML() {
  const inds = (state.data.industries || []).slice(0, 18);
  if (!inds.length) return "";
  const maxAvg = Math.max(...inds.map(i => i.avg_pass));
  const minAvg = Math.min(...inds.map(i => i.avg_pass));
  const range = maxAvg - minAvg || 1;
  const cells = inds.map(i => {
    const t = (i.avg_pass - minAvg) / range; // 0..1
    const opacity = 0.18 + 0.65 * t;
    return `<button class="heat-cell" data-heat-industry="${escapeHtml(i.name)}"
      style="background: rgba(255, 106, 61, ${opacity.toFixed(2)});">
      <span class="heat-name">${escapeHtml(i.name)}</span>
      <span class="heat-meta">${i.count} 只 · 均通过 ${i.avg_pass}</span>
    </button>`;
  }).join("");
  return `<div class="heatmap-section">
    <div class="heatmap-head">
      <h3>🔥 行业热力(主题命中股按平均通过条数排序)</h3>
      <span class="heatmap-hint">点击行业名筛选</span>
    </div>
    <div class="heatmap-grid">${cells}</div>
  </div>`;
}

// ============== Portfolio tab ==============
function renderPortfolioTab(main) {
  const codes = state.portfolio;
  const rows = codes.map(code => {
    const s = state.byCode.get(code);
    if (!s) {
      return `<div class="port-row not-found">
        <span class="stock-code">${escapeHtml(code)}</span>
        <span class="port-status">⚠️ 不在追踪范围(非主题命中股)</span>
      </div>`;
    }
    const n = passCount(s);
    const total = activeCount();
    const passNow = stockPasses(s);
    const statusClass = passNow ? "ok" : "warn";
    const statusText = passNow ? `✓ 仍符合筛选 (${n}/${total} 项)` : `✗ 已跌出筛选 (${n}/${total} 项)`;
    return `<a class="port-row ${statusClass}" href="#/stock/${encodeURIComponent(s.code)}">
      <span class="stock-code">${s.code}</span>
      <span class="stock-name">${escapeHtml(s.name)}</span>
      <span class="stock-industry">${escapeHtml(s.industry || "")}</span>
      <span class="port-status">${statusText}</span>
      <span class="row-arrow">›</span>
    </a>`;
  }).join("");

  const okCount = codes.filter(c => {
    const s = state.byCode.get(c);
    return s && stockPasses(s);
  }).length;

  main.innerHTML = `
    <div class="portfolio-page">
      <div class="port-intro">
        <h3>📋 持仓自检</h3>
        <p>输入你持有的股票代码(逗号、空格或换行分隔),系统每天告诉你哪些仍符合筛选,哪些已经掉队。</p>
        <textarea id="portfolio-input" placeholder="例如:600000 000001 002230">${state.portfolio.join(", ")}</textarea>
        <div class="port-actions">
          <button id="portfolio-save">保存</button>
          <span class="port-summary">${codes.length ? `共 ${codes.length} 只,符合 ${okCount} / 掉队 ${codes.length - okCount}` : ""}</span>
        </div>
      </div>
      ${codes.length ? `<div class="port-list">${rows}</div>` : ""}
    </div>
  `;
  document.getElementById("portfolio-save").addEventListener("click", () => {
    const txt = document.getElementById("portfolio-input").value;
    const codes = [...new Set(txt.split(/[\s,，\n;；]+/).filter(Boolean).map(s => s.trim()))];
    state.portfolio = codes;
    persist();
    renderListMain();
  });

  document.getElementById("result-count").textContent =
    codes.length ? `持仓 ${codes.length} 只,符合 ${okCount} 只` : "未设置持仓";
}

function stockRow(s, total) {
  const n = passCount(s);
  const m = s.metrics;
  const fav = state.favorites.has(s.code);
  const conceptsPreview = (s.concepts || []).slice(0, 3).map(c => escapeHtml(c)).join(" · ");
  return `
    <a class="stock-row" href="#/stock/${encodeURIComponent(s.code)}">
      <button class="fav-btn${fav ? " on" : ""}" data-fav="${s.code}" title="${fav ? "取消收藏" : "加入收藏"}">${fav ? "★" : "☆"}</button>
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
      <div class="row-spark">${sparkline(s.kline_close)}</div>
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

// ============== Detail view ==============
function rarityInfo(stock) {
  const dist = state.data.npass_distribution || {};
  const n = stock.n_pass;
  const same = dist[n] || dist[String(n)] || 0;
  const total = state.data.stocks.length || 1;
  let level = "普通";
  if (same <= 3) level = "极罕见";
  else if (same <= 10) level = "罕见";
  else if (same <= 30) level = "较少";
  const pct = ((same / total) * 100).toFixed(1);
  return { same, total, level, pct, n };
}

function renderDetailMain(code) {
  const main = document.getElementById("main-content");
  const toolbar = document.getElementById("list-toolbar");
  if (toolbar) toolbar.classList.add("hidden");
  if (!main) return;
  const s = state.byCode.get(code);
  if (!s) {
    main.innerHTML = `<div class="empty">未找到股票 ${escapeHtml(code)}。<a href="#/">返回列表</a></div>`;
    return;
  }
  const m = s.metrics;
  const conceptTags = (s.concepts || []).map(c => `<span class="concept-tag">${escapeHtml(c)}</span>`).join("");
  const fav = state.favorites.has(s.code);
  const rarity = rarityInfo(s);

  // prev/next from currentList
  let prevCode = null, nextCode = null;
  if (state.currentList?.length) {
    const idx = state.currentList.indexOf(code);
    if (idx > 0) prevCode = state.currentList[idx - 1];
    if (idx >= 0 && idx < state.currentList.length - 1) nextCode = state.currentList[idx + 1];
  }
  const navHTML = (prevCode || nextCode) ? `
    <div class="detail-nav">
      ${prevCode ? `<a class="nav-arrow" href="#/stock/${prevCode}" title="上一只 (← / 右滑)">‹ ${state.byCode.get(prevCode)?.name || prevCode}</a>` : '<span></span>'}
      ${nextCode ? `<a class="nav-arrow" href="#/stock/${nextCode}" title="下一只 (→ / 左滑)">${state.byCode.get(nextCode)?.name || nextCode} ›</a>` : '<span></span>'}
    </div>` : "";

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

  const introHTML = s.intro
    ? `<div class="intro-card"><span class="intro-icon">💡</span><span class="intro-text">${escapeHtml(s.intro)}</span></div>`
    : "";

  const adviceHtml = s.advice
    ? `<div class="advice-card">
         <h3>AI 投资建议</h3>
         <div class="advice-body">${escapeHtml(s.advice)}</div>
       </div>`
    : `<div class="advice-card empty-advice">AI 建议未生成。</div>`;

  const bullList = (s.bull || []).map((b, i) => `<li>${escapeHtml(b)}</li>`).join("");
  const bearList = (s.bear || []).map((b, i) => `<li>${escapeHtml(b)}</li>`).join("");
  const debateHTML = (bullList || bearList) ? `
    <div class="debate-card">
      <h3>看多 vs 看空</h3>
      <div class="debate-tabs">
        <button class="debate-tab active" data-debate="bull">💚 买入派</button>
        <button class="debate-tab" data-debate="bear">💔 反对派</button>
      </div>
      <ul class="debate-list bull active">${bullList || "<li>暂无看多观点</li>"}</ul>
      <ul class="debate-list bear">${bearList || "<li>暂无看空观点</li>"}</ul>
    </div>` : "";

  const externalLinks = `
    <div class="external-links">
      <a href="https://quote.eastmoney.com/${s.code.startsWith('6') ? 'sh' : 'sz'}${s.code}.html" target="_blank" rel="noopener">东方财富</a>
      <a href="https://xueqiu.com/S/${s.code.startsWith('6') ? 'SH' : 'SZ'}${s.code}" target="_blank" rel="noopener">雪球</a>
      <a href="http://stockpage.10jqka.com.cn/${s.code}/" target="_blank" rel="noopener">同花顺F10</a>
    </div>`;

  const klineSparkBig = sparkline(s.kline_close, 320, 60);
  const rarityBadge = `<span class="rarity-badge ${rarity.level === '极罕见' ? 'very-rare' : rarity.level === '罕见' ? 'rare' : ''}" title="共 ${rarity.total} 只主题股,通过 ${rarity.n}/13 的有 ${rarity.same} 只(${rarity.pct}%)">🎯 ${rarity.level}信号 · 同档 ${rarity.same} 只</span>`;

  main.innerHTML = `
    <div class="detail-page" id="detail-root">
      ${navHTML}
      <a class="back-link" href="#/">‹ 返回列表</a>
      <div class="detail-head">
        <div class="detail-title">
          <span class="stock-code">${s.code}</span>
          <span class="stock-name">${escapeHtml(s.name)}</span>
          <span class="stock-industry">${escapeHtml(s.industry || "未分类")}</span>
          <button class="fav-btn big${fav ? " on" : ""}" id="detail-fav">${fav ? "★" : "☆"}</button>
        </div>
        ${introHTML}
        <div class="badges-row">${rarityBadge}</div>
        <div class="concept-tags">${conceptTags}</div>
        ${externalLinks}
      </div>

      <div class="detail-grid">
        <div class="detail-radar-wrap">
          <h3>股票 DNA 雷达图</h3>
          <p class="hint">13 项条件命中可视化 — 越向外延展越优。</p>
          ${radarSVG(s)}
        </div>
        <div class="detail-spark-wrap">
          <h3>近 30 日价格</h3>
          ${klineSparkBig || '<div class="empty-spark">无 K 线数据</div>'}
        </div>
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
      ${debateHTML}

      <div class="detail-criteria">
        <h3>13 项条件评估</h3>
        ${allChips}
      </div>
    </div>
  `;

  // Fav button
  const dfav = document.getElementById("detail-fav");
  if (dfav) {
    dfav.addEventListener("click", () => {
      if (state.favorites.has(s.code)) state.favorites.delete(s.code);
      else state.favorites.add(s.code);
      persist();
      renderDetailMain(s.code);
    });
  }
  // Debate tab toggle
  main.querySelectorAll(".debate-tab").forEach(btn => {
    btn.addEventListener("click", (e) => {
      const which = e.currentTarget.dataset.debate;
      main.querySelectorAll(".debate-tab").forEach(t => t.classList.toggle("active", t.dataset.debate === which));
      main.querySelectorAll(".debate-list").forEach(l => l.classList.toggle("active", l.classList.contains(which)));
    });
  });

  // Swipe + arrow key navigation
  attachSwipeNav(prevCode, nextCode);
}

let _navHandlers = null;
function attachSwipeNav(prevCode, nextCode) {
  // Clear previous listeners
  if (_navHandlers) {
    document.removeEventListener("keydown", _navHandlers.key);
    const root = document.getElementById("detail-root");
    if (root) {
      root.removeEventListener("touchstart", _navHandlers.ts);
      root.removeEventListener("touchend", _navHandlers.te);
    }
  }
  const root = document.getElementById("detail-root");
  if (!root) return;

  let startX = null;
  const ts = (e) => { startX = e.touches[0].clientX; };
  const te = (e) => {
    if (startX == null) return;
    const dx = e.changedTouches[0].clientX - startX;
    startX = null;
    if (Math.abs(dx) < 60) return;
    if (dx > 0 && prevCode) window.location.hash = `#/stock/${prevCode}`;
    else if (dx < 0 && nextCode) window.location.hash = `#/stock/${nextCode}`;
  };
  const key = (e) => {
    if (e.key === "ArrowLeft" && prevCode) window.location.hash = `#/stock/${prevCode}`;
    else if (e.key === "ArrowRight" && nextCode) window.location.hash = `#/stock/${nextCode}`;
  };
  root.addEventListener("touchstart", ts, { passive: true });
  root.addEventListener("touchend", te, { passive: true });
  document.addEventListener("keydown", key);
  _navHandlers = { ts, te, key };
}

// ============== Router ==============
function route() {
  const hash = window.location.hash || "";

  // 共享状态恢复:#/s/<base64>
  const ms = hash.match(/^#\/s\/(.+)$/);
  if (ms) {
    if (decodeShareState(ms[1])) {
      window.history.replaceState(null, "", "#/");
      renderCriteria();
      renderListMain();
      flashHeader("✓ 已应用分享的筛选条件");
    }
    return;
  }

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

function flashHeader(text) {
  const tr = document.getElementById("broadcast-transcript");
  if (!tr) return;
  tr.classList.remove("hidden");
  tr.textContent = text;
  setTimeout(() => tr.classList.add("hidden"), 3000);
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

  // sidebar buttons
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

  // header buttons
  document.getElementById("broadcast-btn").addEventListener("click", toggleBroadcast);
  document.getElementById("share-btn").addEventListener("click", copyShare);

  // toolbar
  const searchInput = document.getElementById("search-input");
  let searchTimer = null;
  searchInput.addEventListener("input", (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.search = e.target.value;
      renderListMain();
    }, 120);
  });
  document.getElementById("sort-select").addEventListener("change", (e) => {
    state.sort = e.target.value;
    renderListMain();
  });
  document.querySelectorAll(".tab-switch .tab").forEach((tab) => {
    tab.addEventListener("click", (e) => {
      document.querySelectorAll(".tab-switch .tab").forEach(t => t.classList.remove("active"));
      e.currentTarget.classList.add("active");
      state.tab = e.currentTarget.dataset.tab;
      renderListMain();
    });
  });

  // preload Chinese voices (some browsers need a touch)
  if (window.speechSynthesis) {
    window.speechSynthesis.getVoices();
    window.speechSynthesis.onvoiceschanged = () => window.speechSynthesis.getVoices();
  }

  // Daily data change notification
  maybeNotifyDataChange();
}

function maybeNotifyDataChange() {
  const generated = state.data?.generated_at;
  if (!generated) return;
  const lastSeen = localStorage.getItem(LS_LAST_SEEN);
  if (lastSeen === generated) return;

  // Compute simple diff vs previous lastSeen entry: just announce today's totals
  const total = state.data.stocks.length;
  const top = state.data.stocks[0];
  const msg = `今日 ${total} 只股票入选,通过条数最高的是 ${top?.name || ""}(${top?.code || ""})。`;

  const showHeaderToast = () => {
    flashHeader(msg);
  };

  if ("Notification" in window) {
    if (Notification.permission === "granted") {
      try {
        new Notification("A股选股 · 今日更新", { body: msg, icon: "icon.svg" });
      } catch { showHeaderToast(); }
    } else if (Notification.permission === "default") {
      // ask quietly only after a short delay so user has context
      setTimeout(() => {
        Notification.requestPermission().then((p) => {
          if (p === "granted") {
            try { new Notification("A股选股 · 今日更新", { body: msg, icon: "icon.svg" }); }
            catch { showHeaderToast(); }
          } else {
            showHeaderToast();
          }
        });
      }, 3000);
      showHeaderToast(); // also show inline as fallback
    } else {
      showHeaderToast();
    }
  } else {
    showHeaderToast();
  }

  localStorage.setItem(LS_LAST_SEEN, generated);
}

init();
