/* Agentic AI activity: explanations (diagnosis, impacts, recommendations, tools) and action history. */
"use strict";

(() => {
  const {api, escapeHtml, fmt} = App;

  let latest = {};                           // action_id -> latest stored state (e.g. executed by the simulator)

  function decisionCard(d) {
    const recs = (d.recommendations || []).map((r) => `<li>${escapeHtml(r)}</li>`).join("");
    const tools = (d.tool_calls || []).map((t) => `<span class="badge text-bg-light border me-1">${escapeHtml(t.tool)}${t.error ? " ⚠" : ""}</span>`).join("");
    const actions = (d.actions || []).map((a) => {
      const now = latest[a.action_id] || {};
      const status = now.status || a.status, result = (now.parameters || {}).result || a.result || "";
      return `<tr><td>${escapeHtml(a.action_type)} ${escapeHtml(a.zone_id)}</td>
      <td><code class="params">${escapeHtml(JSON.stringify(a.parameters))}</code></td><td>${App.statusBadge(status)}</td>
      <td>${escapeHtml(a.safety_reason || "")}</td><td>${escapeHtml(result)}</td></tr>`;
    }).join("");
    const errors = (d.errors || []).length ? `<div class="alert alert-warning py-1 px-2 small mb-2">${d.errors.map(escapeHtml).join("<br>")}</div>` : "";
    return `<div class="border rounded p-3 mb-3 bg-white">
      <div class="d-flex flex-wrap justify-content-between gap-2 mb-2">
        <div><strong>${escapeHtml(d.zone_id || "building")}</strong> · ${escapeHtml(fmt.label(d.classification || d.alert_type || "manual"))}
          ${d.severity ? App.severityBadge(d.severity) : ""}</div>
        <div class="small text-muted">${fmt.time(d.finished_at)} · ${escapeHtml(d.llm_provider)} (${escapeHtml(d.llm_model)})</div></div>
      ${errors}
      <dl class="row small mb-2">
        <dt class="col-sm-3">Diagnosis</dt><dd class="col-sm-9">${escapeHtml(d.diagnosis)}</dd>
        <dt class="col-sm-3">Likely cause</dt><dd class="col-sm-9">${escapeHtml(d.likely_cause)} <span class="text-muted">(confidence ${escapeHtml(d.confidence)})</span></dd>
        <dt class="col-sm-3">Energy impact</dt><dd class="col-sm-9">${escapeHtml(d.energy_impact)}</dd>
        <dt class="col-sm-3">Operational impact</dt><dd class="col-sm-9">${escapeHtml(d.operational_impact)}</dd>
        <dt class="col-sm-3">Recommendations</dt><dd class="col-sm-9"><ul class="mb-0 ps-3">${recs}</ul></dd>
        <dt class="col-sm-3">Tools called</dt><dd class="col-sm-9">${tools || "none"}</dd></dl>
      ${actions ? `<div class="table-responsive"><table class="table table-sm small mb-0"><thead><tr><th>Proposed action</th>
        <th>Parameters</th><th>Status</th><th>Safety validator</th><th>Result</th></tr></thead><tbody>${actions}</tbody></table></div>`
        : '<div class="small text-muted">No actions proposed.</div>'}</div>`;
  }

  async function loadDecisions() {
    const [{rows}, actions] = await Promise.all([api("/api/agent/decisions?limit=20"),
                                                 api("/api/agent/actions?source=agent&limit=500")]);
    latest = Object.fromEntries(actions.rows.map((a) => [a.action_id, a]));
    document.getElementById("decision-list").innerHTML = rows.length ? rows.map(decisionCard).join("")
      : '<p class="text-muted mb-0">No agent decisions yet. Run workers.agent_service (or python -m agent.demo for an offline demo).</p>';
  }

  async function loadActions() {
    const source = document.getElementById("filter-source").value;
    const {rows, summary} = await api(`/api/agent/actions?limit=200&source=${encodeURIComponent(source)}`);
    document.getElementById("action-table").innerHTML = rows.length ? rows.map((a) => `<tr>
      <td>${fmt.time(a.updated_at)}</td><td>${escapeHtml(a.zone_id)}</td><td>${escapeHtml(a.source)}</td><td>${escapeHtml(a.action_type)}</td>
      <td><code class="params">${escapeHtml(JSON.stringify(a.parameters))}</code></td><td>${App.statusBadge(a.status)}</td>
      <td>${escapeHtml(a.reason || "")}</td><td>${escapeHtml(a.safety_reason || "")}</td></tr>`).join("")
      : '<tr><td colspan="8" class="text-muted">No actions</td></tr>';
    const groups = Charts.groupBy(summary, "status");
    const labels = [...new Set(summary.map((s) => `${s.source}: ${s.action_type}`))];
    Charts.render("chart-actions", Object.keys(groups).sort().map((status, i) => ({
      type: "bar", orientation: "h", name: status,
      y: groups[status].map((s) => `${s.source}: ${s.action_type}`), x: groups[status].map((s) => s.n),
      marker: {color: {executed: "#198754", approved: "#0d6efd", rejected: "#dc3545", failed: "#842029", proposed: "#adb5bd"}[status] || Charts.PALETTE[i]}})),
      {barmode: "stack", margin: {l: 170, r: 10, t: 10, b: 40}, yaxis: {categoryarray: labels}, hovermode: "closest",
       xaxis: {type: "log"}}, "No actions yet");
  }

  App.on("decisions", () => loadDecisions().catch(() => {}));
  App.on("actions", () => loadActions().catch(() => {}));
  document.addEventListener("DOMContentLoaded", () => {
    document.getElementById("filter-source").addEventListener("change", () => loadActions().catch(() => {}));
    Promise.allSettled([loadDecisions(), loadActions()]);
  });
})();
