/* AI agent page: operator approvals, plain-language investigations, verdict summary and action history. */
"use strict";

(() => {
  const {api, escapeHtml, fmt} = App;
  const TOOLS = {
    get_sensor_data: ["thermometer-half", "Read the zone's recent sensor readings"],
    get_energy_data: ["lightning-charge", "Looked at the zone's energy use"],
    get_weather_and_carbon: ["cloud-sun", "Checked the weather and the grid's carbon intensity"],
    run_anomaly_detection: ["search", "Re-ran the attack detectors on recent readings"],
    run_drift_analysis: ["graph-up-arrow", "Checked for slow, gradual changes (drift)"],
    run_energy_optimization: ["sliders", "Asked the energy optimizer for safe settings"],
    adjust_lighting: ["lightbulb", "Proposed a lighting change"],
    isolate_sensor: ["slash-circle", "Proposed to quarantine a sensor"],
    adjust_hvac: ["fan", "Proposed a heating/cooling change"],
    submit_assessment: ["clipboard-check", "Wrote its report"],
  };
  const SOURCES = {agent: "AI agent", optimizer: "Energy optimizer", operator: "Operator", baseline: "Schedule"};
  const STATUS_TEXT = {executed: "done on the building", failed: "failed", approved: "approved, being carried out",
                       rejected: "not carried out", proposed: "proposed", awaiting_approval: "waiting for your approval",
                       declined: "declined by you", expired: "expired (no answer)"};
  const MANUAL = window.DASHBOARD.approvalMode === "manual";
  const PAGE = 6;
  let decisions = [], latest = {}, filter = "", shownCount = PAGE, approvals = [], sending = new Set();

  function verdictGroup(kind) {
    return ["acted", "confirmed", "awaiting"].includes(kind) ? "attack" : kind === "false_alarm" ? "false_alarm" : "other";
  }

  function result(status, text) {
    return `<span class="status-${escapeHtml(status)}">${escapeHtml(STATUS_TEXT[status] || status)}</span>` +
      (text ? `<div class="text-muted">${escapeHtml(text)}</div>` : "");
  }

  /** Proposed → safety check → operator → building, coloured by how far the action got. */
  function stepper(status, gated) {
    const steps = [["lightbulb", "Proposed"], ["shield-check", "Safety check"],
                   ["person-check", gated ? "Your approval" : "Approval: automatic"], ["building-check", "Carried out"]];
    const state = {                     // per step: done | now | fail | skip | off
      proposed: ["now", "off", "off", "off"], rejected: ["done", "fail", "off", "off"],
      awaiting_approval: ["done", "done", "now", "off"], declined: ["done", "done", "fail", "off"],
      expired: ["done", "done", "fail", "off"], approved: ["done", "done", gated ? "done" : "skip", "now"],
      executed: ["done", "done", gated ? "done" : "skip", "done"], failed: ["done", "done", gated ? "done" : "skip", "fail"],
    }[status] || ["done", "off", "off", "off"];
    return `<ol class="stepper">${steps.map(([icon, label], i) =>
      `<li class="${state[i]}"><i class="bi bi-${icon}"></i><span>${escapeHtml(label)}</span></li>`).join("")}</ol>`;
  }

  function current(a) {
    const now = latest[a.action_id] || {};
    const params = now.parameters || {};
    return {status: now.status || a.status, text: params.result || a.result || "",
            safety: now.safety_reason || a.safety_reason || "",
            gated: Boolean(params.approval || a.explanation)};
  }

  function actionRow(a) {
    const c = current(a);
    return `<div class="act"><div class="flex-grow-1"><strong>${escapeHtml(Glossary.action(a.action_type, a.parameters))}</strong>
      ${stepper(c.status, c.gated)}
      <div>${result(c.status, c.text)}</div>
      ${c.status === "rejected" && c.safety ? `<div class="text-danger small">Safety check: ${escapeHtml(c.safety)}</div>` : ""}</div></div>`;
  }

  function decisionCard(d) {
    const v = Verdict.of(d);
    const c = Glossary.classification(d.classification || d.alert_type);
    const tools = [...new Set((d.tool_calls || []).map((t) => t.tool))].map((t) => {
      const [icon, text] = TOOLS[t] || ["dot", fmt.label(t)];
      return `<li><i class="bi bi-${icon}"></i>${escapeHtml(text)}</li>`;
    }).join("");
    const actions = (d.actions || []).map(actionRow).join("");
    const errors = (d.errors || []).length ? `<div class="alert alert-warning py-1 px-2 small mb-2">${d.errors.map(escapeHtml).join("<br>")}</div>` : "";
    const recs = (d.recommendations || []).map((r) => `<li>${escapeHtml(r)}</li>`).join("");
    return `<div class="investigation" style="--verdict:${v.fg}">
      <div class="d-flex flex-wrap justify-content-between align-items-center gap-2">
        <div class="d-flex flex-wrap align-items-center gap-2"><strong class="fs-6">${escapeHtml(d.zone_id || "Building")}</strong>
          <span class="chip" style="background:${c.color}1f;color:${c.color}"><i class="bi bi-${c.icon}"></i> ${escapeHtml(c.label)}</span>
          ${d.severity ? `<span class="small">${App.severityBadge(d.severity)} severity</span>` : ""}</div>
        <span class="chip" style="background:${v.bg};color:${v.fg}"><i class="bi bi-${v.icon}"></i> ${escapeHtml(v.label)}</span></div>
      <div class="story">${escapeHtml(Verdict.story(d))}</div>
      ${errors}
      <div class="row g-3">
        <div class="col-12 col-md-4"><h6>Steps it took</h6><ul class="checklist">${tools || "<li>nothing</li>"}</ul></div>
        <div class="col-12 col-md-4"><h6>Impact</h6><div class="small">${escapeHtml(d.energy_impact)}</div>
          <div class="small text-muted mt-1">${escapeHtml(d.operational_impact)}</div></div>
        <div class="col-12 col-md-4"><h6>Actions</h6>${actions || '<div class="small text-muted">No action needed.</div>'}</div>
      </div>
      <details><summary>Technical details</summary><dl class="row mb-0">
        <dt class="col-sm-3">Alert</dt><dd class="col-sm-9">${escapeHtml(Glossary.humanize(d.alert_description || "—"))}${c.plain ? ` <span class="text-muted">(${escapeHtml(c.plain)})</span>` : ""}</dd>
        <dt class="col-sm-3">Diagnosis</dt><dd class="col-sm-9">${escapeHtml(d.diagnosis)}</dd>
        <dt class="col-sm-3">Likely cause</dt><dd class="col-sm-9">${escapeHtml(d.likely_cause)} (confidence ${escapeHtml(d.confidence)})</dd>
        <dt class="col-sm-3">Recommendations</dt><dd class="col-sm-9"><ul class="mb-0 ps-3">${recs}</ul></dd>
        <dt class="col-sm-3">Decided by</dt><dd class="col-sm-9">${escapeHtml(providerText(d))} · ${fmt.time(d.finished_at)} (real time)</dd>
      </dl></details></div>`;
  }

  function providerText(d) {
    return d.llm_provider === "anthropic" ? `Claude (${d.llm_model})` : `offline rule-based playbook (${d.llm_model}), no LLM`;
  }

  function kpi(icon, color, label, value, sub) {
    return `<div class="col-6 col-md-4 col-xl-2"><div class="card kpi h-100"><div class="card-body">
      <div class="kpi-icon ${color}"><i class="bi bi-${icon}"></i></div><div>
      <div class="kpi-label">${escapeHtml(label)}</div><div class="kpi-value">${escapeHtml(value)}</div>
      <div class="kpi-sub">${escapeHtml(sub)}</div></div></div></div></div>`;
  }

  // -- operator approvals ----------------------------------------------------------------
  function remaining(row) {
    const left = Date.parse(row.waiting_since) + 1000 * window.DASHBOARD.approvalTimeoutS - Date.now();
    if (!Number.isFinite(left)) return "";
    if (left <= 0) return "expiring…";
    const m = Math.floor(left / 60000), s = Math.floor((left % 60000) / 1000);
    return `${m}:${String(s).padStart(2, "0")} left`;
  }

  function approvalCard(row) {
    const e = (row.parameters || {}).approval || {};
    const c = Glossary.classification(row.classification);
    const busy = sending.has(row.action_id);
    const section = (icon, title, text) => text ? `<div class="why"><i class="bi bi-${icon}"></i><div><strong>${title}</strong>
      <div>${escapeHtml(text)}</div></div></div>` : "";
    return `<div class="approval-card" data-id="${escapeHtml(row.action_id)}">
      <div class="d-flex flex-wrap justify-content-between align-items-center gap-2 mb-2">
        <div class="d-flex flex-wrap align-items-center gap-2">
          <strong class="fs-6">${escapeHtml(e.title || Glossary.action(row.action_type, row.parameters))}</strong>
          ${row.classification ? `<span class="chip" style="background:${c.color}1f;color:${c.color}"><i class="bi bi-${c.icon}"></i> ${escapeHtml(c.label)}</span>` : ""}</div>
        <span class="chip countdown" data-since="${escapeHtml(row.waiting_since)}"><i class="bi bi-stopwatch"></i> <span>${escapeHtml(remaining(row))}</span></span></div>
      <div class="summary"><i class="bi bi-chat-quote"></i> ${escapeHtml(e.summary || row.reason || "")}</div>
      <div class="row g-2 mt-1">
        <div class="col-12 col-lg-6">${section("search", "What was found", e.what_happened)}${section("gear", "What this action does", e.what_will_happen)}</div>
        <div class="col-12 col-lg-6">${section("lightning-charge", "Effect on energy and the optimizer", e.energy_effect)}
          ${section("x-circle", "If you decline", e.if_declined)}${section("shield-check", "Safety check", e.safety_check || row.safety_reason)}</div>
      </div>
      <div class="d-flex flex-wrap justify-content-between align-items-center gap-2 mt-2">
        <span class="small text-muted"><i class="bi bi-cpu"></i> Explained by ${escapeHtml(e.explained_by || "built-in template")}</span>
        ${window.DASHBOARD.controlsEnabled ? `<div class="d-flex gap-2">
          <button class="btn btn-outline-secondary btn-sm" data-decision="decline" ${busy ? "disabled" : ""}><i class="bi bi-x-lg"></i> Decline</button>
          <button class="btn btn-success btn-sm" data-decision="approve" ${busy ? "disabled" : ""}><i class="bi bi-check-lg"></i> Approve</button></div>`
          : '<span class="small text-muted">Controls are disabled</span>'}
      </div>
      ${busy ? '<div class="small text-primary mt-1"><i class="bi bi-hourglass-split"></i> Sent, waiting for the agent service…</div>' : ""}</div>`;
  }

  function renderApprovals() {
    const box = document.getElementById("approval-list");
    document.getElementById("approval-count").textContent = approvals.length;
    if (!MANUAL) {
      box.innerHTML = `<div class="empty-state"><i class="bi bi-lightning"></i>Automatic mode: actions that pass the safety check are
        carried out at once. Set <code>AGENT_APPROVAL_MODE=manual</code> in <code>.env</code> and restart to approve each action yourself.</div>`;
      return;
    }
    box.innerHTML = approvals.length ? approvals.map(approvalCard).join("")
      : `<div class="empty-state"><i class="bi bi-person-check"></i>Nothing is waiting for you. When the AI agent wants to act on a
         confirmed threat, its plan appears here with an explanation, and nothing happens until you approve it.</div>`;
  }

  async function decide(id, decision) {
    sending.add(id);
    renderApprovals();
    try {
      await App.postJson(`/api/agent/actions/${encodeURIComponent(id)}/decision`, {decision});
      App.toast(decision === "approve" ? "Approved: the agent service re-checks safety, then carries it out"
                                       : "Declined: nothing will be changed", decision === "approve" ? "success" : "secondary");
    } catch (e) {
      sending.delete(id);
      renderApprovals();
      App.toast(`Could not send your decision: ${e.message}`, "danger");
    }
  }

  async function loadApprovals() {
    const {rows} = await api("/api/agent/approvals");
    approvals = rows;
    const waiting = new Set(rows.map((r) => r.action_id));
    sending = new Set([...sending].filter((id) => waiting.has(id)));
    renderApprovals();
  }

  // -- investigations ----------------------------------------------------------------------
  function renderDecisions() {
    const kinds = decisions.map((d) => Verdict.of(d).kind);
    const count = (k) => kinds.filter((x) => k.includes(x)).length;
    const all = decisions.flatMap((d) => d.actions || []);
    const acts = all.map((a) => a.status);
    // Actions that went through the approval step (they carry an explanation) and are no longer waiting.
    const decided = all.filter((a) => (a.explanation || (latest[a.action_id] || {}).parameters?.approval) &&
                                      a.status !== "awaiting_approval").length;
    document.getElementById("agent-kpis").innerHTML = [
      kpi("search", "bg-grad-blue", "Investigations", decisions.length, "alerts checked"),
      kpi("shield-fill-exclamation", "bg-grad-red", "Attacks confirmed", count(["acted", "confirmed", "awaiting"]), "real problems"),
      kpi("check-circle", "bg-grad-green", "False alarms", count(["false_alarm"]), "no action needed"),
      kpi("person-check", "bg-grad-orange", "Your decisions", decided,
          `${acts.filter((s) => s === "awaiting_approval").length} waiting now`),
      kpi("gear", "bg-grad-purple", "Actions done", acts.filter((s) => s === "executed").length, "carried out on the building"),
      kpi("x-octagon", "bg-grad-gray", "Blocked", acts.filter((s) => s === "rejected").length, "stopped by the safety check"),
    ].join("");
    const present = Object.keys(Verdict.STYLES).filter((k) => kinds.includes(k));
    Charts.render("chart-verdicts", present.length ? [{type: "pie", hole: 0.55, sort: false,
      labels: present.map((k) => Verdict.STYLES[k].label), values: present.map((k) => kinds.filter((x) => x === k).length),
      marker: {colors: present.map((k) => Verdict.STYLES[k].fg)}, textinfo: "value", hoverinfo: "label+value+percent"}] : [],
      {showlegend: true, legend: {orientation: "h", y: -0.1}, margin: {l: 10, r: 10, t: 10, b: 10}}, "No investigations yet");
    const shown = decisions.filter((d) => !filter || verdictGroup(Verdict.of(d).kind) === filter);
    const more = shown.length > shownCount
      ? `<div class="text-center"><button type="button" class="btn btn-outline-primary btn-sm" id="show-more">
           Show more (${shown.length - shownCount} older)</button></div>` : "";
    document.getElementById("decision-list").innerHTML = shown.length ? shown.slice(0, shownCount).map(decisionCard).join("") + more
      : `<div class="empty-state"><i class="bi bi-inbox"></i>${decisions.length ? "No investigations of this kind." :
         "No investigations yet. Trigger a simulated attack on the Security Monitoring page (wait about 5 minutes after Start)."}</div>`;
    const button = document.getElementById("show-more");
    if (button) button.addEventListener("click", () => { shownCount += PAGE * 2; renderDecisions(); });
    if (decisions.length) document.getElementById("provider-note").textContent = `Agent mode: ${providerText(decisions[0])}.`;
  }

  async function loadDecisions() {
    const [{rows}, actions] = await Promise.all([api("/api/agent/decisions?limit=100"),
                                                 api("/api/agent/actions?source=agent&limit=500")]);
    latest = Object.fromEntries(actions.rows.map((a) => [a.action_id, a]));
    // The decision stores each action's status at decision time; use the latest stored status instead.
    decisions = rows.map((d) => ({...d, actions: (d.actions || []).map((a) => ({...a, status: current(a).status}))}));
    renderDecisions();
  }

  async function loadActions() {
    const source = document.getElementById("filter-source").value;
    const {rows} = await api(`/api/agent/actions?limit=200&source=${encodeURIComponent(source)}`);
    // created_at is simulated time; updated_at is the wall-clock time of the latest status change.
    rows.sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)));
    document.getElementById("action-table").innerHTML = rows.length ? rows.map((a) => {
      const gated = Boolean((a.parameters || {}).approval);
      return `<tr>
      <td class="text-nowrap">${fmt.time(a.created_at)}</td><td>${escapeHtml(a.zone_id)}</td>
      <td>${escapeHtml(SOURCES[a.source] || a.source)}</td>
      <td><strong>${escapeHtml(Glossary.action(a.action_type, a.parameters))}</strong><div class="text-muted">${escapeHtml(a.reason || "")}</div></td>
      <td>${stepper(a.status, gated)}${a.status === "rejected" && a.safety_reason ? `<div class="text-danger">${escapeHtml(a.safety_reason)}</div>` : ""}</td>
      <td>${result(a.status, (a.parameters || {}).result || "")}</td></tr>`;
    }).join("") : '<tr><td colspan="6" class="text-muted">No actions yet</td></tr>';
  }

  function renderLegend() {
    const text = {
      acted: "The checks confirmed the attack and the agent changed something, e.g. quarantined the sensor and put the zone back on its safe schedule.",
      awaiting: "The attack is confirmed and the plan passed the safety check; it is carried out only if you approve it.",
      confirmed: "The checks confirmed the problem, but no action was needed, or the actions were blocked, declined or expired.",
      false_alarm: "Re-checking the data did not confirm the alert. Detectors sometimes over-react; nothing is changed.",
      not_attack: "Normal conditions changed (e.g. the season). The ML model should be retrained; no security action.",
      too_early: "The detectors need about one simulated day of history before their results can be trusted.",
      review: "The evidence was unclear; a person should look at it.",
    };
    document.getElementById("verdict-legend").innerHTML = Object.entries(Verdict.STYLES).map(([k, s]) =>
      `<div class="legend-item"><span class="swatch" style="background:${s.fg}"></span><div><strong><i class="bi bi-${s.icon}"></i> ${escapeHtml(s.label)}</strong>
       <span class="text-muted">${escapeHtml(text[k])}</span></div></div>`).join("");
  }

  function renderMode() {
    const step = document.getElementById("flow-approval");
    step.classList.toggle("off", !MANUAL);
    step.querySelector("span").textContent = MANUAL
      ? `You approve or decline. The plan is explained in plain words${window.DASHBOARD.slmName ? ` by ${window.DASHBOARD.slmName}` : ""}; nothing runs without you.`
      : "Off (automatic mode): approved actions run at once.";
    document.getElementById("approval-mode").innerHTML = MANUAL
      ? '<span class="chip" style="background:#fef3c7;color:#92400e"><i class="bi bi-person-check"></i> manual approval</span>'
      : '<span class="chip"><i class="bi bi-lightning"></i> automatic</span>';
  }

  App.on("decisions", () => loadDecisions().catch(() => {}));
  App.on("actions", () => Promise.allSettled([loadActions(), loadDecisions(), loadApprovals()]));
  App.on("status", (s) => {
    if (Number((s.counts || {}).awaiting_approval || 0) !== approvals.length) loadApprovals().catch(() => {});
  });
  document.addEventListener("DOMContentLoaded", () => {
    renderMode();
    renderLegend();
    document.getElementById("approval-list").addEventListener("click", (event) => {
      const button = event.target.closest("button[data-decision]");
      if (button) decide(button.closest(".approval-card").dataset.id, button.dataset.decision);
    });
    document.getElementById("filter-source").addEventListener("change", () => loadActions().catch(() => {}));
    document.querySelectorAll("#verdict-filter button").forEach((b) => b.addEventListener("click", () => {
      document.querySelectorAll("#verdict-filter button").forEach((x) => x.classList.toggle("active", x === b));
      filter = b.dataset.kind;
      shownCount = PAGE;
      renderDecisions();
    }));
    Promise.allSettled([loadApprovals(), loadDecisions(), loadActions()]);
    setInterval(() => document.querySelectorAll(".countdown").forEach((el) => {
      el.querySelector("span").textContent = remaining({waiting_since: el.dataset.since});
    }), 1000);
  });
})();
