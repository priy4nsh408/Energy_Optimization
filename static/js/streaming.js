/* Shared helpers: JSON API, escaping, formatting, Socket.IO live updates, status bar, simulation controls.
   All text coming from the database (alert descriptions, LLM explanations) is escaped before insertion. */
"use strict";

const App = (() => {
  const handlers = {};

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
  }

  async function api(path, options = {}) {
    const response = await fetch(path, {headers: {"Accept": "application/json"}, ...options});
    let body = null;
    try { body = await response.json(); } catch (e) { body = null; }
    if (!response.ok) {
      const message = (body && (body.error || body.detail)) || `HTTP ${response.status}`;
      if (response.status === 503) banner(`Data unavailable: ${message}. Start PostgreSQL and the workers.`);
      throw new Error(message + (body && body.details ? ": " + body.details.join("; ") : ""));
    }
    banner(null);
    return body;
  }

  function postJson(path, payload) {
    return api(path, {method: "POST", headers: {"Content-Type": "application/json", "Accept": "application/json"},
                      body: JSON.stringify(payload || {})});
  }

  function banner(text) {
    const el = document.getElementById("banner");
    if (!el) return;
    el.textContent = text || "";
    el.classList.toggle("d-none", !text);
  }

  function toast(text, kind = "secondary") {
    const container = document.getElementById("toasts");
    const el = document.createElement("div");
    el.className = `toast align-items-center text-bg-${kind} border-0`;
    el.setAttribute("role", "status");
    el.innerHTML = `<div class="d-flex"><div class="toast-body">${escapeHtml(text)}</div>
      <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button></div>`;
    container.appendChild(el);
    new bootstrap.Toast(el, {delay: 5000}).show();
    el.addEventListener("hidden.bs.toast", () => el.remove());
  }

  const fmt = {
    num: (v, d = 1) => (v === null || v === undefined || Number.isNaN(Number(v))) ? "—" : Number(v).toFixed(d),
    time: (v) => v ? String(v).replace("T", " ").slice(0, 16) : "—",
    pct: (v, d = 1) => (v === null || v === undefined) ? "—" : `${Number(v).toFixed(d)} %`,
    label: (v) => String(v ?? "").replaceAll("_", " "),
  };

  function severityBadge(severity) {
    return `<span class="sev-${escapeHtml(severity)}">${escapeHtml(severity)}</span>`;
  }

  function statusBadge(status) {
    return `<span class="status-${escapeHtml(status)}">${escapeHtml(status)}</span>`;
  }

  function on(event, handler) {
    (handlers[event] = handlers[event] || []).push(handler);
  }

  function emitLocal(event, payload) {
    (handlers[event] || []).forEach((h) => { try { h(payload); } catch (e) { console.error(e); } });
  }

  function updateStatus(status) {
    if (!status || status.error) {
      banner(status && status.error ? "Data unavailable: database unavailable." : null);
      return;
    }
    const sim = status.simulation;
    const state = sim ? sim.state : "not started";
    const el = document.getElementById("sim-state");
    el.textContent = `simulation: ${state}`;
    el.className = "badge " + ({running: "text-bg-success", paused: "text-bg-warning"}[state] || "text-bg-secondary");
    document.getElementById("sim-time").textContent = fmt.time(status.latest_reading);
    const detail = document.getElementById("sim-detail");
    if (detail) {
      detail.textContent = sim ? `${sim.ticks} ticks · ${sim.message || ""}` :
        "start workers.simulation_manager to control the simulation";
    }
    emitLocal("status", status);
  }

  function connect() {
    if (typeof io === "undefined") { banner("Socket.IO client not loaded (no internet for the CDN?)"); return; }
    const socket = io({transports: ["websocket", "polling"]});
    const dot = document.getElementById("live-indicator");
    socket.on("connect", () => { dot.className = "live-dot online"; });
    socket.on("disconnect", () => { dot.className = "live-dot offline"; });
    socket.on("status", updateStatus);
    ["readings", "alerts", "decisions", "actions"].forEach((name) => socket.on(name, (rows) => emitLocal(name, rows)));
    on("alerts", (rows) => rows.filter((a) => ["high", "critical"].includes(a.severity)).slice(0, 3)
      .forEach((a) => toast(`${a.zone_id || "building"}: ${fmt.label(a.classification || a.alert_type)} (${a.severity})`, "danger")));
    on("decisions", (rows) => rows.slice(0, 2).forEach((d) => toast(`Agent: ${d.zone_id || ""} ${d.likely_cause} — ${d.outcome}`, "primary")));
  }

  // -- simulation controls ---------------------------------------------------------------
  function renderParams() {
    const type = document.getElementById("attack-type").value;
    const params = (window.DASHBOARD.attackParameters || {})[type] || {};
    const defaults = {offset_c: 4, rate_c_per_h: 0.5, max_offset_c: 6, count: 25, factor: 3, lag: 288};
    const container = document.getElementById("attack-params");
    container.innerHTML = "";
    Object.entries(params).forEach(([name, rule]) => {
      const col = document.createElement("div");
      col.className = "col-6 col-sm-4";
      const id = `param-${name}`;
      let input;
      if (typeof rule[0] === "string") {
        input = `<select class="form-select form-select-sm" id="${id}" data-param="${name}">` +
          rule.map((o) => `<option>${escapeHtml(o)}</option>`).join("") + "</select>";
      } else {
        const step = Number.isInteger(rule[0]) && Number.isInteger(rule[1]) && name !== "factor" ? 1 : 0.05;
        input = `<input class="form-control form-control-sm" type="number" id="${id}" data-param="${name}"
                  min="${rule[0]}" max="${rule[1]}" step="${step}" value="${defaults[name] ?? rule[0]}">`;
      }
      col.innerHTML = `<label class="form-label small mb-0" for="${id}">${escapeHtml(fmt.label(name))}</label>${input}`;
      container.appendChild(col);
    });
  }

  function initControls() {
    if (!window.DASHBOARD.controlsEnabled || !document.getElementById("btn-start")) return;
    const send = async (path, payload, label) => {
      try {
        await postJson(path, payload);
        toast(`${label} sent to the simulation manager`, "success");
      } catch (e) { toast(`${label} failed: ${e.message}`, "danger"); }
    };
    document.getElementById("btn-start").addEventListener("click", () => send("/api/simulation/start", {}, "Start"));
    document.getElementById("btn-stop").addEventListener("click", () => send("/api/simulation/stop", {}, "Stop"));
    document.getElementById("attack-type").addEventListener("change", renderParams);
    renderParams();
    document.getElementById("attack-form").addEventListener("submit", (event) => {
      event.preventDefault();
      const params = {};
      document.querySelectorAll("#attack-params [data-param]").forEach((el) => {
        params[el.dataset.param] = el.tagName === "SELECT" ? el.value : Number(el.value);
      });
      send("/api/simulation/attack", {
        attack_type: document.getElementById("attack-type").value,
        zone_id: document.getElementById("attack-zone").value,
        duration_min: Number(document.getElementById("attack-duration").value),
        params,
      }, "Attack");
    });
  }

  async function refreshStatus() {
    try { updateStatus(await api("/api/status")); } catch (e) { /* banner already shown */ }
  }

  document.addEventListener("DOMContentLoaded", () => {
    initControls();
    refreshStatus();
    connect();
  });

  return {api, postJson, escapeHtml, fmt, on, toast, severityBadge, statusBadge, banner};
})();
