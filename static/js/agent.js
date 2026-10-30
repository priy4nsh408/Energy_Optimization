/* AI agent page: plain-language investigations, verdict summary and action history. */
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
  const PAGE = 6;
  let decisions = [], latest = {}, filter = "", shownCount = PAGE;

  function verdictGroup(kind) {
    return ["acted", "confirmed"].includes(kind) ? "attack" : kind === "false_alarm" ? "false_alarm" : "other";
  }

  function safety(status, reason) {
    if (status === "rejected") return `<span class="text-danger"><i class="bi bi-x-circle-fill"></i> blocked</span>` +
      (reason ? `<div class="text-muted">${escapeHtml(reason)}</div>` : "");
    if (status === "proposed") return '<span class="text-muted"><i class="bi bi-hourglass"></i> waiting</span>';
    return '<span class="text-success"><i class="bi bi-check-circle-fill"></i> passed</span>';
  }

  function result(status, text) {
    const label = {executed: "done on the building", failed: "failed", approved: "approved, waiting to run",
                   rejected: "not carried out", proposed: "proposed"}[status] || status;
    return `<span class="status-${escapeHtml(status)}">${escapeHtml(label)}</span>` +
      (text ? `<div class="text-muted">${escapeHtml(text)}</div>` : "");
  }

  function actionRow(a) {
    const now = latest[a.action_id] || {};
    const status = now.status || a.status, text = (now.parameters || {}).result || a.result || "";
    const icon = status === "rejected" ? "x-octagon text-danger" : status === "executed" ? "check2-circle text-success" : "arrow-right-circle text-primary";
    return `<div class="act"><i class="bi bi-${icon}"></i><div><strong>${escapeHtml(Glossary.action(a.action_type, a.parameters))}</strong>
      <div>Safety check: ${safety(status, a.safety_reason)} · Result: ${result(status, text)}</div></div></div>`;
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
        <dt class="col-sm-3">Alert</dt><dd class="col-sm-9">${escapeHtml(d.alert_description || "—")}${c.plain ? ` <span class="text-muted">(${escapeHtml(c.plain)})</span>` : ""}</dd>
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

  function renderDecisions() {
    const kinds = decisions.map((d) => Verdict.of(d).kind);
    const count = (k) => kinds.filter((x) => k.includes(x)).length;
    const acts = decisions.flatMap((d) => d.actions || []).map((a) => (latest[a.action_id] || {}).status || a.status);
    document.getElementById("agent-kpis").innerHTML = [
      kpi("search", "bg-grad-blue", "Investigations", decisions.length, "alerts checked"),
      kpi("shield-fill-exclamation", "bg-grad-red", "Attacks confirmed", count(["acted", "confirmed"]), "real problems"),
      kpi("check-circle", "bg-grad-green", "False alarms", count(["false_alarm"]), "no action needed"),
      kpi("info-circle", "bg-grad-teal", "Other", count(["not_attack", "too_early", "review"]), "drift, too early, review"),
      kpi("gear", "bg-grad-purple", "Actions done", acts.filter((s) => s === "executed").length, "carried out on the building"),
      kpi("x-octagon", "bg-grad-orange", "Blocked", acts.filter((s) => s === "rejected").length, "stopped by the safety check"),
    ].join("");
    const order = Object.keys(Verdict.STYLES);
    const present = order.filter((k) => kinds.includes(k));
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
    decisions = rows;
    renderDecisions();
  }

  async function loadActions() {
    const source = document.getElementById("filter-source").value;
    const {rows} = await api(`/api/agent/actions?limit=200&source=${encodeURIComponent(source)}`);
    // created_at is simulated time; updated_at is the wall-clock time of the latest status change.
    rows.sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)));
    document.getElementById("action-table").innerHTML = rows.length ? rows.map((a) => `<tr>
      <td class="text-nowrap">${fmt.time(a.created_at)}</td><td>${escapeHtml(a.zone_id)}</td>
      <td>${escapeHtml(SOURCES[a.source] || a.source)}</td>
      <td><strong>${escapeHtml(Glossary.action(a.action_type, a.parameters))}</strong><div class="text-muted">${escapeHtml(a.reason || "")}</div></td>
      <td>${safety(a.status, a.status === "rejected" ? a.safety_reason : "")}</td>
      <td>${result(a.status, (a.parameters || {}).result || "")}</td></tr>`).join("")
      : '<tr><td colspan="6" class="text-muted">No actions yet</td></tr>';
  }

  function renderLegend() {
    const text = {
      acted: "The checks confirmed the attack and the agent changed something, e.g. quarantined the sensor and put the zone back on its safe schedule.",
      confirmed: "The checks confirmed the problem, but no action was needed or every action was blocked.",
      false_alarm: "Re-checking the data did not confirm the alert. Detectors sometimes over-react; nothing is changed.",
      not_attack: "Normal conditions changed (e.g. the season). The ML model should be retrained; no security action.",
      too_early: "The detectors need about one simulated day of history before their results can be trusted.",
      review: "The evidence was unclear; a person should look at it.",
    };
    document.getElementById("verdict-legend").innerHTML = Object.entries(Verdict.STYLES).map(([k, s]) =>
      `<div class="legend-item"><span class="swatch" style="background:${s.fg}"></span><div><strong><i class="bi bi-${s.icon}"></i> ${escapeHtml(s.label)}</strong>
       <span class="text-muted">${escapeHtml(text[k])}</span></div></div>`).join("");
  }

  App.on("decisions", () => loadDecisions().catch(() => {}));
  App.on("actions", () => Promise.allSettled([loadActions(), loadDecisions()]));
  document.addEventListener("DOMContentLoaded", () => {
    renderLegend();
    document.getElementById("filter-source").addEventListener("change", () => loadActions().catch(() => {}));
    document.querySelectorAll("#verdict-filter button").forEach((b) => b.addEventListener("click", () => {
      document.querySelectorAll("#verdict-filter button").forEach((x) => x.classList.toggle("active", x === b));
      filter = b.dataset.kind;
      shownCount = PAGE;
      renderDecisions();
    }));
    Promise.allSettled([loadDecisions(), loadActions()]);
  });
})();
