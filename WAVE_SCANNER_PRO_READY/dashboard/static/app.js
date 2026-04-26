// WAVE Scanner Dashboard front-end.
// Live metrics + log feed via /ws, AI Council via SSE on /api/ollama/{chat,council},
// sandboxed local-files browser, conversations stored on the server.

(() => {
  "use strict";

  const $  = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  // Default sampling profile. Local Ollama models default to temperature
  // 0.7-0.8 which makes them ramble; for code review 0.2 + lower top_p gives
  // far more usable output. num_ctx=16384 is a sane middle ground (qwen3-coder
  // 30B at q4 fits ~32k easily; smaller models will clamp internally).
  const SAMPLING_DEFAULT = Object.freeze({ temperature: 0.2, top_p: 0.9, num_ctx: 16384, repeat_penalty: 1.1 });
  const SAMPLING_PRESETS = Object.freeze({
    code:     { temperature: 0.2,  top_p: 0.9,  num_ctx: 16384, repeat_penalty: 1.1 },
    quick:    { temperature: 0.5,  top_p: 0.9,  num_ctx: 8192,  repeat_penalty: 1.1 },
    creative: { temperature: 0.8,  top_p: 0.95, num_ctx: 16384, repeat_penalty: 1.05 },
    deep:     { temperature: 0.15, top_p: 0.85, num_ctx: 32768, repeat_penalty: 1.1 },
  });

  // Default system prompt baked in. Plenty of users won't open the
  // ⌘ System prompt panel; we want the council to behave reasonably out of
  // the box. Few-shot examples matter more than rules — small local models
  // copy the demonstrated format much more reliably than they follow rules.
  const DEFAULT_SYSTEM_PROMPT = [
    "Ты — senior backend/quant-разработчик и code-reviewer. Контекст: торговый бот WAVE Scanner",
    "(Python, async, pandas/numpy, ccxt, Bybit, Telegram-нотификации). Пользователь",
    "даёт код или вопрос — ты сразу анализируешь и отвечаешь.",
    "",
    "ЖЁСТКИЕ ПРАВИЛА:",
    "1. НЕ задавай уточняющих вопросов. Делай 1-3 разумных предположения, явно называй их («предполагаю, что…») и продолжай.",
    "2. НЕ объясняй, что такое «бот» / «API» / «функция». Собеседник — опытный разработчик.",
    "3. НЕ перечисляй «категории ботов» / «давайте определимся». Анализируй то, что дано.",
    "4. НЕ извиняйся, не пиши «конечно!» / «хороший вопрос». Сразу к делу.",
    "5. Если в контексте НЕТ блока '=== Attached files ===' — НЕ выдумывай содержимое.",
    "   Скажи явно: «прицепленных файлов не вижу, прикрепи их кнопкой + attach» и остановись.",
    "   НЕ пиши выдуманные имена файлов и номера строк — это враньё.",
    "",
    "ФОРМАТ ОТВЕТА:",
    "- Резюме (1-3 строки): что именно я вижу.",
    "- Проблемы/находки списком, каждая со ссылкой на file:line.",
    "- Правки в блоках ```python```. Готовый код, не 'можно бы'.",
    "- 1 строка trade-off, выбор с обоснованием.",
    "- 1 строка 'Что проверить дальше'.",
    "",
    "ПРИМЕР ХОРОШЕГО ОТВЕТА (копируй формат):",
    "",
    "Вопрос: 'бот жрёт память, глянь' + прицеплен data_fetcher.py",
    "Ответ:",
    "Резюме: _data_cache — безлимитный dict, TTL проверяется только при чтении. Растёт вечно.",
    "Проблемы:",
    "- data_fetcher.py:28 — `_data_cache: Dict` без cap.",
    "- data_fetcher.py:81 — лишний `.copy()` на каждой записи.",
    "Правка:",
    "```python",
    "from collections import OrderedDict",
    "_data_cache: OrderedDict = OrderedDict()",
    "def _cache_set(key, df):",
    "    _data_cache[key] = (time.time(), df)",
    "    while len(_data_cache) > 96:",
    "        _data_cache.popitem(last=False)",
    "```",
    "Trade-off: убрал .copy() — безопасно, все consumers только читают (проверено в wave_analyzer.py).",
    "Что проверить: `LOW_RAM_MODE=1 python main.py` и смотри `MEM:` в логах.",
    "",
    "ПРИМЕР ПЛОХОГО ОТВЕТА (ТАК НЕ ДЕЛАЙ):",
    "«Конечно! Однако для того чтобы проанализировать бота, давайте сначала определимся, что вы понимаете под этим термином…» — запрещено.",
  ].join("\n");

  const state = {
    ws: null,
    cpuHistory: [],
    memHistory: [],
    historyMax: 60,
    termHistory: [],
    termHistoryIdx: -1,
    autoscrollLogs: true,
    logFilter: "",
    reconnectAttempts: 0,
    // chat
    selectedModels: new Set(),
    availableModels: [],
    chatMode: "council",
    synthesize: true,
    conversationId: null,
    conversations: [],
    attachedFiles: [], // [{path, content, size, source: "ws"|"upload"}]
    streaming: false,
    streamAbort: null,
    pendingPanes: {}, // model -> {pane, body, status, text}
    lastRound: null,  // {question, models, answers: {model: text}}
    sampling: { ...SAMPLING_DEFAULT }, // sent in `options` to /api/ollama/*
    // files
    cwd: "",
    filePreview: null,
    workspaceRoot: "",
  };

  const VALID_THEMES = new Set(["dark", "matrix", "light"]);

  // ---------- helpers ----------
  function setText(id, val) { const el = document.getElementById(id); if (el) el.textContent = val; }
  function escapeHtml(s) {
    return String(s).replace(/[&<>'"]/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;",
    }[c]));
  }
  function fmtUptime(sec) {
    sec = Math.max(0, Math.floor(sec || 0));
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    return `${h}h ${m.toString().padStart(2, "0")}m ${s.toString().padStart(2, "0")}s`;
  }
  function fmtSize(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
  function tickClock() {
    const now = new Date();
    setText("utc-clock", now.toISOString().replace("T", " ").slice(0, 19) + " UTC");
  }
  setInterval(tickClock, 1000);
  tickClock();

  // ---------- view switching ----------
  function setView(view) {
    if (!view) return;
    document.body.setAttribute("data-view", view);
    $$(".nav-item[data-view]").forEach((n) => n.classList.toggle("active", n.dataset.view === view));
    try { localStorage.setItem("dash:view", view); } catch {}
  }

  // ---------- sparklines ----------
  function drawSpark(svgId, history, color, viewBoxH = 36) {
    const svg = document.getElementById(svgId);
    if (!svg) return;
    const w = 100, h = viewBoxH;
    if (!history.length) { svg.innerHTML = ""; return; }
    svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
    const xs = history.map((_, i) => (i / Math.max(1, state.historyMax - 1)) * w);
    const ys = history.map((v) => h - (Math.min(100, Math.max(0, v)) / 100) * (h - 2) - 1);
    const points = xs.map((x, i) => `${x.toFixed(1)},${ys[i].toFixed(1)}`).join(" ");
    const fillPts = `0,${h} ${points} ${w},${h}`;
    svg.innerHTML = `
      <defs>
        <linearGradient id="${svgId}-g" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stop-color="${color}" stop-opacity=".55"/>
          <stop offset="100%" stop-color="${color}" stop-opacity="0"/>
        </linearGradient>
      </defs>
      <polygon points="${fillPts}" fill="url(#${svgId}-g)"/>
      <polyline points="${points}" fill="none" stroke="${color}" stroke-width="1.5"/>
    `;
  }

  // ---------- metrics ----------
  function applyMetrics(m) {
    if (!m) return;
    setText("cpu-pct", m.cpu_percent.toFixed(1));
    setText("mem-pct", m.memory_percent.toFixed(1));
    setText("disk-pct", m.disk_percent.toFixed(1));
    setText("cpu-pct-mini", m.cpu_percent.toFixed(0));
    setText("mem-pct-mini", m.memory_percent.toFixed(0));
    setText("load-1", m.load_avg && m.load_avg[0] != null ? m.load_avg[0].toFixed(2) : "—");
    setText("procs", m.process_count);
    setText("uptime", fmtUptime(m.uptime_sec));
    const cpuBar = $("#cpu-bar"); if (cpuBar) cpuBar.style.width = `${Math.min(100, m.cpu_percent)}%`;
    const memBar = $("#mem-bar"); if (memBar) memBar.style.width = `${Math.min(100, m.memory_percent)}%`;
    state.cpuHistory.push(m.cpu_percent);
    state.memHistory.push(m.memory_percent);
    if (state.cpuHistory.length > state.historyMax) state.cpuHistory.shift();
    if (state.memHistory.length > state.historyMax) state.memHistory.shift();
    drawSpark("cpu-spark", state.cpuHistory, "var(--accent)");
    drawSpark("mem-spark", state.memHistory, "var(--accent-2)");
    drawSpark("cpu-spark-big", state.cpuHistory, "var(--accent)", 36);
  }

  // ---------- log feed ----------
  const LEVEL_RE = /\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]/;
  function appendLogLine(line) {
    if (state.logFilter && !line.toLowerCase().includes(state.logFilter)) return;
    const container = $("#log-container");
    if (!container) return;
    const m = LEVEL_RE.exec(line);
    const level = m ? m[1] : "INFO";
    let ts = "", msg = line;
    const tsMatch = line.match(/^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)/);
    if (tsMatch) {
      ts = tsMatch[1].replace("T", " ");
      msg = line.slice(tsMatch[0].length).trimStart();
    }
    const row = document.createElement("div");
    row.className = `log-line lvl-${level}`;
    row.innerHTML = `<span class="ts">${escapeHtml(ts)}</span><span class="msg">${escapeHtml(msg)}</span>`;
    container.appendChild(row);
    while (container.childElementCount > 1500) container.removeChild(container.firstElementChild);
    if (state.autoscrollLogs) container.scrollTop = container.scrollHeight;
  }
  function applyLogSnapshot(lines) {
    const container = $("#log-container"); if (!container) return;
    container.innerHTML = "";
    for (const line of lines) appendLogLine(line);
  }

  // ---------- signals ----------
  async function refreshSignals() {
    try {
      const r = await fetch("/api/signals?limit=200", { cache: "no-store" });
      const j = await r.json();
      renderSignals(j.signals || []);
    } catch (e) { console.warn("signals fetch failed", e); }
  }
  function renderSignals(rows) {
    const tbody = $("#signals-body");
    setText("signals-count", `${rows.length}`);
    if (!tbody) return;
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="12" class="text-center text-slate-500 py-6">no signals yet</td></tr>`;
      return;
    }
    const fmt = (v, d = 4) => { const n = Number(v); return Number.isFinite(n) ? n.toFixed(d) : (v ?? ""); };
    tbody.innerHTML = rows.map((r) => {
      const dir = (r.direction || "").toUpperCase();
      return `
        <tr>
          <td class="text-slate-400">${escapeHtml((r.timestamp || "").slice(0, 19))}</td>
          <td>${escapeHtml(r.symbol || "")}</td>
          <td class="dir-${escapeHtml(dir)}">${escapeHtml(dir)}</td>
          <td>${escapeHtml(r.score || "")}</td>
          <td>${fmt(r.rr_ratio, 2)}</td>
          <td>${fmt(r.entry_price)}</td>
          <td>${fmt(r.stop_loss)}</td>
          <td>${fmt(r.tp1)}</td>
          <td>${fmt(r.tp2)}</td>
          <td>${fmt(r.tp3)}</td>
          <td class="text-slate-400">${escapeHtml(r.session || "")}</td>
          <td class="text-slate-400">${escapeHtml(r.label || "")}</td>
        </tr>`;
    }).join("");
  }

  // ---------- AI command center terminal ----------
  function termPrint(text, cls) {
    const out = $("#term-out"); if (!out) return;
    const div = document.createElement("div");
    if (cls) div.className = cls;
    div.textContent = text;
    out.appendChild(div);
    out.scrollTop = out.scrollHeight;
  }
  function termPrompt(cmd) {
    const out = $("#term-out"); if (!out) return;
    const div = document.createElement("div");
    div.innerHTML = `<span class="term-prompt">›</span> <span>${escapeHtml(cmd)}</span>`;
    out.appendChild(div);
    out.scrollTop = out.scrollHeight;
  }
  function applyUiActions(actions) {
    if (!Array.isArray(actions)) return;
    for (const a of actions) {
      if (!a || !a.action) continue;
      if (a.action === "theme")  setTheme(a.theme);
      if (a.action === "layout") setLayoutMode(a.mode);
      if (a.action === "focus")  setView(a.target === "logs" ? "logs"
                                       : a.target === "signals" ? "signals"
                                       : a.target === "terminal" ? "terminal"
                                       : a.target === "health" ? "health" : "council");
      if (a.action === "clear") { const out = $("#term-out"); if (out) out.innerHTML = ""; }
    }
  }
  async function runTerminalCommand(cmd) {
    const trimmed = cmd.trim(); if (!trimmed) return;
    state.termHistory.push(trimmed); state.termHistoryIdx = state.termHistory.length;
    termPrompt(trimmed);
    const lower = trimmed.toLowerCase();
    if (lower === "clear" || lower === "cls") { const out = $("#term-out"); if (out) out.innerHTML = ""; return; }
    try {
      const r = await fetch("/api/command", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: trimmed }),
      });
      const j = await r.json();
      if (j.output) termPrint(j.output, j.ok ? "" : "text-rose-400");
      applyUiActions(j.ui);
    } catch (e) {
      termPrint(`[client] command failed: ${e}`, "text-rose-400");
    }
  }
  function setupTerminal() {
    const input = $("#term-input"); if (!input) return;
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { const v = input.value; input.value = ""; runTerminalCommand(v); }
      else if (e.key === "ArrowUp") {
        if (state.termHistory.length === 0) return;
        state.termHistoryIdx = Math.max(0, state.termHistoryIdx - 1);
        input.value = state.termHistory[state.termHistoryIdx] || "";
        e.preventDefault();
      } else if (e.key === "ArrowDown") {
        if (state.termHistory.length === 0) return;
        state.termHistoryIdx = Math.min(state.termHistory.length, state.termHistoryIdx + 1);
        input.value = state.termHistory[state.termHistoryIdx] || "";
        e.preventDefault();
      }
    });
    termPrint(
      "WAVE Scanner AI Command Center · type 'help' for commands.\n" +
      "Tip: try 'clean mode', 'theme matrix', 'get signals 20', 'status'."
    );
  }

  // ---------- GenUI: theme + layout ----------
  function setTheme(name) {
    if (!VALID_THEMES.has(name)) return;
    document.documentElement.setAttribute("data-theme", name);
    try { localStorage.setItem("dash:theme", name); } catch {}
    $$(".btn[data-theme]").forEach((b) => b.classList.toggle("border-cyan-400", b.dataset.theme === name));
  }
  function setLayoutMode(mode) {
    if (mode !== "clean" && mode !== "full") return;
    document.body.setAttribute("data-mode", mode);
    try { localStorage.setItem("dash:mode", mode); } catch {}
    $$(".btn[data-mode]").forEach((b) => b.classList.toggle("border-cyan-400", b.dataset.mode === mode));
  }

  // ---------- WebSocket ----------
  function connectWS() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws`);
    state.ws = ws;
    setWsStatus("connecting", "warn");
    ws.addEventListener("open",    () => { state.reconnectAttempts = 0; setWsStatus("online", "ok"); });
    ws.addEventListener("close",   () => {
      setWsStatus("offline · reconnecting…", "err");
      const delay = Math.min(15000, 500 * Math.pow(2, state.reconnectAttempts++));
      setTimeout(connectWS, delay);
    });
    ws.addEventListener("error",   () => setWsStatus("error", "err"));
    ws.addEventListener("message", (ev) => {
      let msg; try { msg = JSON.parse(ev.data); } catch { return; }
      switch (msg.type) {
        case "hello":        if (msg.data) applyMetrics(msg.data); break;
        case "metrics":      applyMetrics(msg.data); break;
        case "log":          appendLogLine(msg.line); break;
        case "log_snapshot": applyLogSnapshot(msg.lines || []); break;
        case "command_result":
          if (msg.data) {
            if (msg.data.output) termPrint(msg.data.output);
            applyUiActions(msg.data.ui);
          }
          break;
      }
    });
  }
  function setWsStatus(text, kind) {
    setText("ws-status", text);
    const dot = $("#ws-dot"); if (!dot) return;
    dot.classList.remove("ok", "warn", "err");
    dot.classList.add(kind || "warn");
  }

  // ---------- Ollama models ----------
  async function refreshModels() {
    const list = $("#model-list");
    if (list) list.innerHTML = `<div class="text-xs text-slate-400 px-1 py-1">loading…</div>`;
    setText("ollama-state", "checking…");
    $("#model-help")?.classList.add("hidden");
    try {
      const r = await fetch("/api/ollama/status", { cache: "no-store" });
      const j = await r.json();
      if (!j.ok) {
        setText("ollama-state", "offline");
        if (list) list.innerHTML = `<div class="text-xs text-rose-300 px-1 py-1">${escapeHtml(j.error || "Ollama unavailable")}</div>`;
        const help = $("#model-help");
        if (help) {
          help.classList.remove("hidden");
          help.textContent = j.hint || "Ollama is not running.";
        }
        state.availableModels = [];
        renderSelectedModelsLabel();
        return;
      }
      state.availableModels = j.models || [];
      setText("ollama-state", `${state.availableModels.length} models`);
      renderModelList();
    } catch (e) {
      setText("ollama-state", "error");
      if (list) list.innerHTML = `<div class="text-xs text-rose-300 px-1 py-1">${escapeHtml(String(e))}</div>`;
    }
  }
  function renderModelList() {
    const list = $("#model-list"); if (!list) return;
    if (!state.availableModels.length) {
      list.innerHTML = `<div class="text-xs text-slate-400 px-1 py-1">no models · run <code class="text-cyan-300">ollama pull llama3</code></div>`;
      return;
    }
    list.innerHTML = state.availableModels.map((m) => {
      const checked = state.selectedModels.has(m.name) ? "checked" : "";
      const meta = [m.parameter_size, m.quantization_level].filter(Boolean).join(" · ");
      return `
        <label class="flex items-center gap-2 px-1 py-1 rounded hover:bg-white/5 cursor-pointer">
          <input type="checkbox" class="accent-cyan-400 model-cb" value="${escapeHtml(m.name)}" ${checked} />
          <span class="font-mono text-xs flex-1 truncate">${escapeHtml(m.name)}</span>
          <span class="text-[10px] text-slate-400">${escapeHtml(meta)}</span>
        </label>`;
    }).join("");
    list.querySelectorAll(".model-cb").forEach((cb) => {
      cb.addEventListener("change", (e) => {
        const name = e.target.value;
        if (e.target.checked) {
          if (state.chatMode === "solo") state.selectedModels.clear();
          state.selectedModels.add(name);
        } else {
          state.selectedModels.delete(name);
        }
        if (state.chatMode === "solo") renderModelList();
        renderSelectedModelsLabel();
        try { localStorage.setItem("dash:models", JSON.stringify(Array.from(state.selectedModels))); } catch {}
      });
    });
  }
  function renderSelectedModelsLabel() {
    const n = state.selectedModels.size;
    setText("model-picker-label", `${state.chatMode === "solo" ? "Model" : "Models"} (${n})`);
  }

  // ---------- Files ----------
  async function loadFiles(rel) {
    try {
      const r = await fetch(`/api/files/list?path=${encodeURIComponent(rel || "")}`, { cache: "no-store" });
      if (!r.ok) {
        const err = await r.json().catch(() => ({}));
        $("#files-list").innerHTML = `<div class="text-xs text-rose-300 px-1">${escapeHtml(err.detail || r.statusText)}</div>`;
        return;
      }
      const j = await r.json();
      state.cwd = j.path || "";
      state.workspaceRoot = j.root || "";
      setText("ws-root", j.root || "");
      setText("files-path", "/" + (j.path || ""));
      const list = $("#files-list");
      list.innerHTML = "";
      if (j.parent !== null) {
        const up = document.createElement("div");
        up.className = "file-item dir";
        up.innerHTML = `<span class="icon">↑</span><span>..</span>`;
        up.addEventListener("click", () => loadFiles(j.parent));
        list.appendChild(up);
      }
      for (const e of j.entries) {
        const row = document.createElement("div");
        row.className = "file-item" + (e.is_dir ? " dir" : "");
        row.innerHTML = `
          <span class="icon">${e.is_dir ? "📁" : (e.text ? "📄" : "🔒")}</span>
          <span class="name">${escapeHtml(e.name)}</span>
          <span class="size">${e.is_dir ? "" : fmtSize(e.size)}</span>`;
        row.addEventListener("click", () => {
          if (e.is_dir) loadFiles(e.path);
          else if (e.text) previewFile(e.path);
        });
        list.appendChild(row);
      }
    } catch (e) {
      $("#files-list").innerHTML = `<div class="text-xs text-rose-300 px-1">${escapeHtml(String(e))}</div>`;
    }
  }
  async function previewFile(path) {
    try {
      const r = await fetch(`/api/files/read?path=${encodeURIComponent(path)}`);
      if (!r.ok) { const err = await r.json().catch(() => ({})); throw new Error(err.detail || r.statusText); }
      const j = await r.json();
      state.filePreview = j;
      $("#files-preview").classList.remove("hidden");
      setText("files-preview-name", `${j.path} · ${fmtSize(j.size)}${j.truncated ? " · truncated" : ""}`);
      $("#files-preview-body").textContent = j.content;
    } catch (e) {
      $("#files-preview").classList.remove("hidden");
      setText("files-preview-name", "error");
      $("#files-preview-body").textContent = String(e);
    }
  }
  function attachCurrentFile() {
    if (!state.filePreview) return;
    const f = state.filePreview;
    if (state.attachedFiles.find((x) => x.path === f.path)) return;
    state.attachedFiles.push({ path: f.path, content: f.content, size: f.size, truncated: !!f.truncated });
    renderAttached();
  }
  function detachFile(path) {
    state.attachedFiles = state.attachedFiles.filter((x) => x.path !== path);
    renderAttached();
  }
  function renderAttached() {
    const list = $("#attached-list");
    const n = state.attachedFiles.length;
    setText("attached-count", `${n} files attached`);
    const pill = $("#attached-count");
    if (pill) {
      // 0 → amber so users notice; ≥1 → violet (default).
      if (n === 0) {
        pill.style.background = "rgba(245,158,11,.15)";
        pill.style.color = "#fcd34d";
      } else {
        pill.style.background = "rgba(167,139,250,.15)";
        pill.style.color = ""; // fall back to var(--accent-2)
        pill.style.removeProperty("color");
        pill.style.color = "var(--accent-2)";
      }
    }
    const warn = $("#no-attach-warn");
    if (warn) warn.classList.toggle("hidden", n > 0);
    if (!list) return;
    list.innerHTML = state.attachedFiles.map((f) => `
      <span class="attached-chip">
        📎 ${escapeHtml(f.path)}<button data-detach="${escapeHtml(f.path)}" title="remove">×</button>
      </span>`).join("");
    list.querySelectorAll("button[data-detach]").forEach((b) =>
      b.addEventListener("click", () => detachFile(b.dataset.detach))
    );
  }

  // ---------- Conversations ----------
  async function loadConversations() {
    try {
      const r = await fetch("/api/conversations", { cache: "no-store" });
      const j = await r.json();
      state.conversations = j.conversations || [];
      renderConversations();
    } catch (e) {
      console.warn("conversations failed", e);
    }
  }
  function renderConversations() {
    const list = $("#conv-list"); if (!list) return;
    if (!state.conversations.length) {
      list.innerHTML = `<div class="text-xs text-slate-500 px-1 py-2">No chats yet · ask the council a question</div>`;
      return;
    }
    list.innerHTML = state.conversations.map((c) => {
      const active = state.conversationId === c.id ? "active" : "";
      const when = new Date(c.updated_at * 1000).toLocaleString();
      return `
        <div class="conv-item ${active}" data-conv="${escapeHtml(c.id)}">
          <div class="title">${escapeHtml(c.title || c.id)}</div>
          <div class="meta"><span>${escapeHtml(when)}</span><span>${c.messages} msg</span></div>
        </div>`;
    }).join("");
    list.querySelectorAll(".conv-item").forEach((row) =>
      row.addEventListener("click", () => openConversation(row.dataset.conv))
    );
  }
  async function openConversation(id) {
    try {
      const r = await fetch(`/api/conversations/${encodeURIComponent(id)}`);
      if (!r.ok) return;
      const j = await r.json();
      state.conversationId = j.conversation.id;
      const chat = $("#chat-stream"); chat.innerHTML = "";
      for (const m of j.conversation.messages) renderMessage(m);
      renderConversations();
    } catch (e) { console.warn(e); }
  }
  async function newConversation() {
    try {
      const r = await fetch("/api/conversations", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: "" }),
      });
      const j = await r.json();
      state.conversationId = j.conversation.id;
      $("#chat-stream").innerHTML = "";
      await loadConversations();
    } catch (e) { console.warn(e); }
  }

  // ---------- Chat helpers: colors, markdown, copy ----------
  function modelColor(name) {
    const n = String(name || "").toLowerCase();
    if (n.startsWith("qwen"))     return "qwen";
    if (n.startsWith("deepseek")) return "deepseek";
    if (n.startsWith("llama") || n.startsWith("meta"))    return "llama";
    if (n.startsWith("mistral") || n.startsWith("mixtral")) return "mistral";
    if (n.startsWith("gemma") || n.startsWith("phi"))      return "gemma";
    return "";
  }

  // Minimal Markdown → HTML for chat bubbles. We intentionally support
  // only fenced code blocks, inline code, **bold**, *italic*, headings —
  // enough for typical AI replies, no full markdown grammar.
  const MD_KEYWORDS = new Set([
    "def","class","return","if","elif","else","for","while","import","from","as","with",
    "try","except","finally","raise","yield","lambda","pass","break","continue","async","await",
    "True","False","None","function","var","let","const","new","this","null","undefined","true",
    "false","public","private","protected","static","void","int","float","double","string","bool",
    "interface","type","enum","struct","fn","mut","pub","use","mod","impl","Self","self","in",
    "of","throw","switch","case","default","extends","implements","package","go","defer","chan",
  ]);
  // Single-pass tokenizer. We MUST NOT chain multiple .replace() calls here:
  // earlier passes insert spans like `<span class="tk-str">`, and later passes
  // (e.g. the keyword pass) would re-match the literal word `class` *inside*
  // those spans, producing nested broken markup that the browser renders as
  // visible text (`class="tk-com">…`). One regex with alternatives, one pass.
  function highlightCode(src) {
    const escaped = escapeHtml(src);
    const tokenRe = new RegExp(
      [
        '(&quot;[^&\\n]*?&quot;|&#39;[^&\\n]*?&#39;|`[^`\\n]*?`)', // 1: strings
        '(#[^\\n]*|\\/\\/[^\\n]*|--[^\\n]*)',                      // 2: line comments
        '(\\b\\d+(?:\\.\\d+)?\\b)',                                 // 3: numbers
        '\\b([A-Za-z_][A-Za-z0-9_]*)(\\s*\\()',                     // 4+5: fn call name + tail
        '(\\b[A-Za-z_][A-Za-z0-9_]*\\b)',                           // 6: identifier (maybe keyword)
      ].join('|'),
      'g'
    );
    return escaped.replace(tokenRe, (m, str, com, num, fnName, fnTail, ident) => {
      if (str !== undefined) return `<span class="tk-str">${str}</span>`;
      if (com !== undefined) return `<span class="tk-com">${com}</span>`;
      if (num !== undefined) return `<span class="tk-num">${num}</span>`;
      if (fnName !== undefined) {
        // Keyword used as a call (rare in real code: `if(`, `for(` in C-like)
        // — render as keyword, not function, so coloring stays accurate.
        if (MD_KEYWORDS.has(fnName)) return `<span class="tk-kw">${fnName}</span>${fnTail}`;
        return `<span class="tk-fn">${fnName}</span>${fnTail}`;
      }
      if (ident !== undefined) {
        return MD_KEYWORDS.has(ident) ? `<span class="tk-kw">${ident}</span>` : ident;
      }
      return m;
    });
  }
  function renderMarkdown(text) {
    const placeholders = [];
    let work = String(text || "");
    work = work.replace(/```(\w+)?\n([\s\S]*?)```/g, (_, lang, code) => {
      const i = placeholders.length;
      const langLabel = lang ? lang.toLowerCase() : "";
      placeholders.push(
        `<pre><span class="lang">${escapeHtml(langLabel)}</span>` +
        `<button class="code-copy" type="button">copy</button>` +
        `<code>${highlightCode(code.replace(/\n$/, ""))}</code></pre>`
      );
      return `\u0000\u0000PLACE${i}\u0000\u0000`;
    });
    work = escapeHtml(work)
      .replace(/`([^`\n]+?)`/g, '<code>$1</code>')
      .replace(/\*\*([^*\n]+)\*\*/g, '<strong>$1</strong>')
      .replace(/(^|\s)\*([^*\n]+)\*(?=\s|$)/g, '$1<em>$2</em>')
      .replace(/^(#{1,3})\s+(.*)$/gm, (_, hs, t) => `<strong>${escapeHtml(t)}</strong>`)
      .replace(/\n/g, "<br>");
    return work.replace(/\u0000\u0000PLACE(\d+)\u0000\u0000/g, (_, i) => placeholders[Number(i)]);
  }
  function attachCodeCopyHandlers(root) {
    root.querySelectorAll(".code-copy").forEach((btn) => {
      btn.addEventListener("click", async (e) => {
        e.preventDefault(); e.stopPropagation();
        const code = btn.parentElement.querySelector("code")?.innerText || "";
        try { await navigator.clipboard.writeText(code); btn.textContent = "copied"; setTimeout(() => (btn.textContent = "copy"), 1200); }
        catch { btn.textContent = "?"; }
      });
    });
  }

  // ---------- Sampling controls ----------
  function applySamplingToUI() {
    const s = state.sampling;
    const set = (id, val) => { const el = $(id); if (el) el.value = String(val); };
    set("#sm-temp", s.temperature); setText("sm-temp-val", s.temperature.toFixed(2));
    set("#sm-topp", s.top_p);       setText("sm-topp-val", s.top_p.toFixed(2));
    set("#sm-ctx",  s.num_ctx);     setText("sm-ctx-val",  String(s.num_ctx));
    set("#sm-rep",  s.repeat_penalty); setText("sm-rep-val", s.repeat_penalty.toFixed(2));
  }
  function wireSampling() {
    const bind = (sel, key, fmt, parse) => {
      const el = $(sel); if (!el) return;
      el.addEventListener("input", () => {
        const v = parse(el.value);
        state.sampling = { ...state.sampling, [key]: v };
        const lbl = $(sel + "-val"); if (lbl) lbl.textContent = fmt(v);
        try { localStorage.setItem("dash:sampling", JSON.stringify(state.sampling)); } catch {}
      });
    };
    bind("#sm-temp", "temperature",    (v) => v.toFixed(2), parseFloat);
    bind("#sm-topp", "top_p",          (v) => v.toFixed(2), parseFloat);
    bind("#sm-ctx",  "num_ctx",        (v) => String(v),    (s) => parseInt(s, 10));
    bind("#sm-rep",  "repeat_penalty", (v) => v.toFixed(2), parseFloat);
    $$(".sm-preset").forEach((b) => b.addEventListener("click", (e) => {
      e.preventDefault();
      const preset = SAMPLING_PRESETS[b.dataset.preset];
      if (!preset) return;
      state.sampling = { ...preset };
      applySamplingToUI();
      try { localStorage.setItem("dash:sampling", JSON.stringify(state.sampling)); } catch {}
    }));
    // Restore from localStorage if present.
    try {
      const raw = localStorage.getItem("dash:sampling");
      if (raw) {
        const parsed = JSON.parse(raw);
        state.sampling = { ...SAMPLING_DEFAULT, ...parsed };
      }
    } catch {}
    applySamplingToUI();
  }

  function makeCopyButton(getText, label = "📋") {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = label;
    b.title = "Copy to clipboard";
    b.addEventListener("click", async (e) => {
      e.preventDefault(); e.stopPropagation();
      try {
        await navigator.clipboard.writeText(getText());
        b.textContent = "✓"; setTimeout(() => (b.textContent = label), 1200);
      } catch { b.textContent = "✗"; }
    });
    return b;
  }

  // ---------- Chat / Council ----------
  function setChatMode(mode) {
    state.chatMode = mode;
    $$(".btn-mode[data-chat-mode]").forEach((b) => b.classList.toggle("active", b.dataset.chatMode === mode));
    if (mode === "solo" && state.selectedModels.size > 1) {
      const first = state.selectedModels.values().next().value;
      state.selectedModels.clear();
      if (first) state.selectedModels.add(first);
      renderModelList();
    }
    renderSelectedModelsLabel();
    try { localStorage.setItem("dash:chatMode", mode); } catch {}
  }

  function buildSystemPrompt() {
    const sysText = $("#system-prompt")?.value?.trim() || "";
    let attached = "";
    if (state.attachedFiles.length) {
      attached = "\n\n=== Attached files ===\n";
      for (const f of state.attachedFiles) {
        attached += `\n--- ${f.path}${f.truncated ? " (truncated)" : ""} ---\n${f.content}\n`;
      }
    } else {
      // Make the "no files" state explicit — stops models from inventing
      // file paths and line numbers.
      attached = "\n\n=== Attached files ===\n(none — пользователь не прицепил файлы. НЕ выдумывай их содержимое.)\n";
    }
    const base = sysText || DEFAULT_SYSTEM_PROMPT;
    return base + attached;
  }

  function renderMessage(m) {
    const chat = $("#chat-stream"); if (!chat) return;
    if (m.role === "user") {
      const row = document.createElement("div");
      row.className = "msg-row user";
      row.innerHTML = `
        <div>
          <div class="msg-meta justify-end">you · ${new Date((m.ts || 0) * 1000).toLocaleTimeString()}</div>
          <div class="msg-bubble">${escapeHtml(m.content).replace(/\n/g, "<br>")}</div>
        </div>`;
      chat.appendChild(row);
    } else {
      const wrap = document.createElement("div");
      wrap.className = "msg-row";
      const color = modelColor(m.model);
      const colorAttr = color ? ` data-color="${color}"` : "";
      const ts = new Date((m.ts || 0) * 1000).toLocaleTimeString();
      wrap.innerHTML = `
        <div class="flex-1 min-w-0">
          <div class="council-pane"${colorAttr}>
            <div class="head">
              <span><span class="msg-model">${escapeHtml(m.model || "assistant")}</span> <span class="text-slate-500">${ts}</span></span>
              <span class="bubble-actions"></span>
            </div>
            <div class="body"></div>
          </div>
        </div>`;
      const body = wrap.querySelector(".body");
      body.innerHTML = renderMarkdown(m.content || "");
      attachCodeCopyHandlers(body);
      const actions = wrap.querySelector(".bubble-actions");
      actions.appendChild(makeCopyButton(() => m.content || "", "📋"));
      chat.appendChild(wrap);
    }
    chat.scrollTop = chat.scrollHeight;
  }

  function startCouncilRound(models, userText) {
    const chat = $("#chat-stream");
    const userTs = Date.now() / 1000;
    renderMessage({ role: "user", content: userText, ts: userTs });
    const grid = document.createElement("div");
    grid.className = "council-grid";
    state.pendingPanes = {};
    for (const model of models) {
      const pane = document.createElement("div");
      pane.className = "council-pane streaming";
      pane.dataset.model = model;
      const c = modelColor(model);
      if (c) pane.dataset.color = c;
      pane.innerHTML = `
        <div class="head">
          <span><span class="msg-model">${escapeHtml(model)}</span> <span class="text-slate-500" data-status>streaming…</span></span>
          <span class="bubble-actions"></span>
        </div>
        <div class="body"></div>`;
      grid.appendChild(pane);
      const rec = {
        pane,
        body: pane.querySelector(".body"),
        status: pane.querySelector("[data-status]"),
        actions: pane.querySelector(".bubble-actions"),
        text: "",
      };
      rec.actions.appendChild(makeCopyButton(() => rec.text, "📋"));
      state.pendingPanes[model] = rec;
    }
    chat.appendChild(grid);
    chat.scrollTop = chat.scrollHeight;
    state.lastRound = { question: userText, models: [...models], answers: {}, started: Date.now() };
  }

  function pushDelta(model, delta) {
    const p = state.pendingPanes[model]; if (!p) return;
    p.text += delta;
    p.body.innerHTML = renderMarkdown(p.text);
    attachCodeCopyHandlers(p.body);
    const chat = $("#chat-stream");
    chat.scrollTop = chat.scrollHeight;
  }

  function endPane(model, info = {}) {
    const p = state.pendingPanes[model]; if (!p) return;
    p.pane.classList.remove("streaming");
    if (info.error) p.status.textContent = "error";
    else p.status.textContent = info.eval_count ? `${info.eval_count} tokens` : "done";
    if (state.lastRound && !info.error) state.lastRound.answers[model] = p.text;
  }

  // After all council panes finish, ask the first model to synthesize.
  async function runSynthesis() {
    if (!state.lastRound) return;
    const { question, models, answers } = state.lastRound;
    const valid = Object.entries(answers).filter(([, v]) => (v || "").trim().length);
    if (valid.length < 2) return; // nothing to synthesize
    const synth = models.find((m) => answers[m]) || models[0];
    const chat = $("#chat-stream");
    const pane = document.createElement("div");
    pane.className = "council-pane synth streaming";
    pane.dataset.model = `${synth} · synthesis`;
    pane.innerHTML = `
      <div class="head">
        <span><span class="msg-model">🧠 synthesis · ${escapeHtml(synth)}</span> <span class="text-slate-500" data-status>analyzing…</span></span>
        <span class="bubble-actions"></span>
      </div>
      <div class="body"></div>`;
    chat.appendChild(pane);
    chat.scrollTop = chat.scrollHeight;
    const body = pane.querySelector(".body");
    const status = pane.querySelector("[data-status]");
    const actions = pane.querySelector(".bubble-actions");
    let text = "";
    actions.appendChild(makeCopyButton(() => text, "📋"));

    let prompt = `Ты получил несколько ответов от разных AI на вопрос:\n\n"${question}"\n\n`;
    valid.forEach(([m, ans], i) => { prompt += `Ответ ${i + 1} (${m}):\n${ans}\n\n`; });
    prompt += "Проанализируй ответы и дай итог:\n" +
      "1. Что общего во всех ответах\n" +
      "2. Ключевые различия\n" +
      "3. Лучший подход и почему\n" +
      "4. Финальная рекомендация\n";

    try {
      const r = await fetch("/api/ollama/chat", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: synth,
          messages: [
            { role: "system", content: "Ты — арбитр совета AI. Дай структурированный, краткий итог по русски." },
            { role: "user", content: prompt },
          ],
          // Synthesis benefits from slightly lower temperature than the round
          // itself — we want a tight summary, not more creativity on top.
          options: { ...state.sampling, temperature: Math.min(state.sampling.temperature, 0.2) },
          conversation_id: state.conversationId, save: true,
        }),
      });
      if (!r.ok || !r.body) {
        status.textContent = "error";
        body.textContent = `synthesis failed (${r.status})`;
        return;
      }
      await consumeSSE(r.body, (evt) => {
        if (evt.type === "delta") {
          text += evt.delta;
          body.innerHTML = renderMarkdown(text);
          attachCodeCopyHandlers(body);
          chat.scrollTop = chat.scrollHeight;
        } else if (evt.type === "end") {
          pane.classList.remove("streaming");
          status.textContent = evt.eval_count ? `${evt.eval_count} tokens` : "done";
        } else if (evt.type === "error") {
          pane.classList.remove("streaming");
          status.textContent = "error";
          body.textContent = `${evt.error}\n${evt.hint || ""}`;
        }
      });
    } catch (e) {
      pane.classList.remove("streaming");
      status.textContent = "error";
      body.textContent = String(e);
    }
  }

  function exportChatMarkdown() {
    const chat = $("#chat-stream");
    if (!chat || !chat.children.length) { alert("Чат пуст."); return; }
    const lines = ["# WAVE Scanner — chat export", "", `_${new Date().toISOString()}_`, ""];
    chat.querySelectorAll(".msg-row, .council-grid").forEach((node) => {
      if (node.classList.contains("msg-row")) {
        const isUser = node.classList.contains("user");
        const bubble = node.querySelector(".msg-bubble");
        const pane = node.querySelector(".council-pane");
        if (bubble) {
          lines.push(`### ${isUser ? "🧑 you" : "🤖 assistant"}`);
          lines.push("", bubble.innerText.trim(), "");
        } else if (pane) {
          const model = pane.querySelector(".msg-model")?.innerText || "assistant";
          lines.push(`### 🤖 ${model}`);
          lines.push("", pane.querySelector(".body")?.innerText.trim() || "", "");
        }
      } else {
        node.querySelectorAll(".council-pane").forEach((pane) => {
          const model = pane.querySelector(".msg-model")?.innerText || "assistant";
          lines.push(`### 🤖 ${model}`);
          lines.push("", pane.querySelector(".body")?.innerText.trim() || "", "");
        });
      }
    });
    const md = lines.join("\n");
    const blob = new Blob([md], { type: "text/markdown;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `wave-chat-${new Date().toISOString().replace(/[:.]/g, "-")}.md`;
    document.body.appendChild(a); a.click();
    setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 200);
  }

  async function sendChat() {
    if (state.streaming) return;
    const input = $("#chat-input");
    const text = (input?.value || "").trim();
    if (!text) return;
    if (state.selectedModels.size === 0) {
      alert("Выбери хотя бы одну модель в выпадашке Models.");
      return;
    }

    if (!state.conversationId) await newConversation();

    const models = Array.from(state.selectedModels);
    if (state.chatMode === "solo") models.length = 1;

    const sysContent = buildSystemPrompt();
    const messages = [{ role: "system", content: sysContent }, { role: "user", content: text }];

    input.value = "";
    state.streaming = true;
    $("#chat-send").disabled = true;
    $("#chat-stop").classList.remove("hidden");

    const url = state.chatMode === "council" ? "/api/ollama/council" : "/api/ollama/chat";
    const opts = { ...state.sampling };
    const body = state.chatMode === "council"
      ? { models, messages, options: opts, conversation_id: state.conversationId, save: true }
      : { model: models[0], messages, options: opts, conversation_id: state.conversationId, save: true };

    startCouncilRound(models, text);

    const ctrl = new AbortController();
    state.streamAbort = ctrl;

    try {
      const r = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: ctrl.signal,
      });
      if (!r.ok || !r.body) {
        const err = await r.json().catch(() => ({}));
        for (const m of models) endPane(m, { error: true });
        renderMessage({
          role: "assistant", model: "system",
          content: `error: ${err.detail?.error || err.detail || r.statusText}\n${err.detail?.hint || ""}`,
          ts: Date.now() / 1000,
        });
      } else {
        await consumeSSE(r.body, (evt) => {
          if (evt.type === "delta") pushDelta(evt.model, evt.delta);
          else if (evt.type === "end") endPane(evt.model, evt);
          else if (evt.type === "error") {
            const p = state.pendingPanes[evt.model];
            if (p) {
              p.body.textContent = `error: ${evt.error}\n${evt.hint || ""}`;
              endPane(evt.model, { error: true });
            }
          }
        });
      }
    } catch (e) {
      if (e.name !== "AbortError") {
        for (const m of models) endPane(m, { error: true });
        console.warn("stream error", e);
      }
    } finally {
      state.streaming = false;
      $("#chat-send").disabled = false;
      $("#chat-stop").classList.add("hidden");
      state.streamAbort = null;
      loadConversations();
      if (state.chatMode === "council" && state.synthesize) {
        runSynthesis().catch((e) => console.warn("synthesis failed", e));
      }
    }
  }

  async function consumeSSE(body, onEvent) {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf("\n\n")) !== -1) {
        const block = buf.slice(0, idx); buf = buf.slice(idx + 2);
        const lines = block.split("\n").filter((l) => l.startsWith("data:"));
        if (!lines.length) continue;
        const data = lines.map((l) => l.slice(5).replace(/^ /, "")).join("\n");
        try { onEvent(JSON.parse(data)); } catch (e) { /* skip */ }
      }
    }
  }

  function abortStream() {
    if (state.streamAbort) state.streamAbort.abort();
  }

  // ---------- Local file upload (no server round-trip) ----------
  const UPLOAD_MAX_BYTES = 512 * 1024;
  const UPLOAD_TEXT_RE = /\.(py|js|jsx|ts|tsx|html?|css|scss|less|json|ya?ml|toml|ini|cfg|env|txt|md|rst|csv|tsv|sh|bash|zsh|bat|ps1|sql|r|rb|go|rs|c|h|cpp|hpp|java|kt|swift|pine)$/i;

  async function uploadLocalFiles(fileList) {
    const files = Array.from(fileList || []);
    for (const f of files) {
      if (!UPLOAD_TEXT_RE.test(f.name) && f.type && !f.type.startsWith("text/")) {
        console.warn("skipped non-text file:", f.name); continue;
      }
      const sliced = f.size > UPLOAD_MAX_BYTES ? f.slice(0, UPLOAD_MAX_BYTES) : f;
      const content = await sliced.text();
      const path = `upload/${f.name}`;
      const existing = state.attachedFiles.find((x) => x.path === path);
      if (existing) { existing.content = content; existing.size = f.size; existing.truncated = f.size > UPLOAD_MAX_BYTES; }
      else state.attachedFiles.push({ path, content, size: f.size, truncated: f.size > UPLOAD_MAX_BYTES, source: "upload" });
    }
    renderAttached();
  }

  function setupDragDrop() {
    const panel = $("#files-panel");
    if (!panel) return;
    const flash = (on) => panel.style.outline = on ? "2px dashed var(--accent)" : "";
    ["dragenter", "dragover"].forEach((ev) => panel.addEventListener(ev, (e) => { e.preventDefault(); flash(true); }));
    ["dragleave", "drop"].forEach((ev) => panel.addEventListener(ev, () => flash(false)));
    panel.addEventListener("drop", (e) => {
      e.preventDefault(); flash(false);
      if (e.dataTransfer?.files?.length) uploadLocalFiles(e.dataTransfer.files);
    });
  }

  // ---------- wiring ----------
  function wireUI() {
    $("#sidebar-toggle")?.addEventListener("click", () => $("#sidebar")?.classList.toggle("collapsed"));
    $("#sidebar-burger")?.addEventListener("click", () => $("#sidebar")?.classList.toggle("hidden"));
    $$(".nav-item[data-view]").forEach((n) => n.addEventListener("click", () => setView(n.dataset.view)));
    $$(".btn[data-theme]").forEach((b) => b.addEventListener("click", () => setTheme(b.dataset.theme)));
    $$(".btn[data-mode]").forEach((b) => b.addEventListener("click", () => setLayoutMode(b.dataset.mode)));
    $$(".btn-mode[data-chat-mode]").forEach((b) => b.addEventListener("click", () => setChatMode(b.dataset.chatMode)));

    $("#log-clear")?.addEventListener("click", () => { const c = $("#log-container"); if (c) c.innerHTML = ""; });
    $("#log-autoscroll")?.addEventListener("change", (e) => { state.autoscrollLogs = e.target.checked; });
    $("#log-filter")?.addEventListener("input", (e) => { state.logFilter = e.target.value.toLowerCase(); });
    $("#signals-refresh")?.addEventListener("click", refreshSignals);

    // Model picker
    $("#model-picker-btn")?.addEventListener("click", (e) => {
      e.stopPropagation();
      $("#model-picker")?.classList.toggle("hidden");
    });
    document.addEventListener("click", (e) => {
      const picker = $("#model-picker"); const btn = $("#model-picker-btn");
      if (!picker || picker.classList.contains("hidden")) return;
      if (!picker.contains(e.target) && e.target !== btn && !btn.contains(e.target)) picker.classList.add("hidden");
    });
    $("#model-refresh")?.addEventListener("click", refreshModels);
    $("#prompt-toggle")?.addEventListener("click", () => $("#prompt-block")?.classList.toggle("hidden"));
    $("#sampling-toggle")?.addEventListener("click", () => $("#sampling-block")?.classList.toggle("hidden"));
    $("#prompt-reset")?.addEventListener("click", () => {
      const ta = $("#system-prompt");
      if (ta) { ta.value = ""; try { localStorage.removeItem("dash:sysprompt"); } catch {} }
    });
    $("#system-prompt")?.addEventListener("input", (e) => {
      try { localStorage.setItem("dash:sysprompt", e.target.value); } catch {}
    });
    wireSampling();

    // Files
    $("#files-up")?.addEventListener("click", () => {
      if (!state.cwd) return;
      const parts = state.cwd.split("/").filter(Boolean); parts.pop();
      loadFiles(parts.join("/"));
    });
    $("#files-refresh")?.addEventListener("click", () => loadFiles(state.cwd));
    $("#files-preview-close")?.addEventListener("click", () => {
      $("#files-preview")?.classList.add("hidden");
      state.filePreview = null;
    });
    $("#files-attach")?.addEventListener("click", attachCurrentFile);

    // Conversations
    $("#conv-new")?.addEventListener("click", newConversation);

    // File upload
    $("#files-upload-btn")?.addEventListener("click", () => $("#files-upload-input")?.click());
    $("#files-upload-input")?.addEventListener("change", (e) => {
      uploadLocalFiles(e.target.files);
      e.target.value = "";
    });
    setupDragDrop();

    // Chat
    $("#chat-send")?.addEventListener("click", sendChat);
    $("#chat-stop")?.addEventListener("click", abortStream);
    $("#chat-clear")?.addEventListener("click", () => { $("#chat-stream").innerHTML = ""; state.lastRound = null; });
    $("#chat-export")?.addEventListener("click", exportChatMarkdown);
    $("#synth-toggle")?.addEventListener("change", (e) => {
      state.synthesize = !!e.target.checked;
      try { localStorage.setItem("dash:synth", state.synthesize ? "1" : "0"); } catch {}
    });
    $("#chat-input")?.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendChat(); }
    });

    // restore prefs
    try {
      const t = localStorage.getItem("dash:theme"); if (t) setTheme(t);
      const m = localStorage.getItem("dash:mode");  if (m) setLayoutMode(m);
      const v = localStorage.getItem("dash:view");  if (v) setView(v);
      const cm = localStorage.getItem("dash:chatMode"); if (cm) setChatMode(cm);
      const ms = localStorage.getItem("dash:models");
      if (ms) { try { JSON.parse(ms).forEach((x) => state.selectedModels.add(x)); } catch {} }
      const s = localStorage.getItem("dash:synth");
      if (s !== null) {
        state.synthesize = s === "1";
        const cb = $("#synth-toggle"); if (cb) cb.checked = state.synthesize;
      }
      const sp = localStorage.getItem("dash:sysprompt");
      if (sp !== null) {
        const ta = $("#system-prompt"); if (ta) ta.value = sp;
      }
    } catch {}
  }

  // ---------- boot ----------
  document.addEventListener("DOMContentLoaded", async () => {
    wireUI();
    setupTerminal();
    connectWS();
    await Promise.all([refreshSignals(), refreshModels(), loadFiles(""), loadConversations()]);
    setInterval(refreshSignals, 30_000);
  });
})();
