/* Security monitoring: alert timeline, simulated attacks, isolated sensors, drift, alert table. */
"use strict";

(() => {
  const {api, escapeHtml, fmt} = App;

  function evidence(a) {
    const d = a.details || {};
    if (d.rules && d.rules.length) return d.rules.map((r) => `${escapeHtml(r.rule)}: ${escapeHtml(r.detail)}`).join("<br>");
    if (d.features) return d.features.filter((f) => (d.drifting_features || []).includes(f.feature))
      .map((f) => `${escapeHtml(f.feature)}: PSI ${fmt.num(f.psi, 2)}, KS p ${Number(f.ks_p_value).toExponential(1)}`).join("<br>");
    if (d.isolation_forest) return `Isolation Forest score ${fmt.num(d.isolation_forest.score, 3)} > ${fmt.num(d.isolation_forest.threshold, 3)}`;
    return "";
  }

  async function loadAlerts() {
    const type = document.getElementById("filter-type").value, zone = document.getElementById("filter-zone").value;
    const {rows} = await api(`/api/security/alerts?limit=200&type=${encodeURIComponent(type)}&zone_id=${encodeURIComponent(zone)}`);
    document.getElementById("alert-table").innerHTML = rows.length ? rows.map((a) => `<tr>
      <td>${fmt.time(a.detected_at)}</td><td>${escapeHtml(a.zone_id || "—")}</td><td>${App.severityBadge(a.severity)}</td>
      <td>${escapeHtml(a.alert_type)}</td><td>${escapeHtml(fmt.label(a.classification))}</td><td>${escapeHtml(a.detector)}</td>
      <td>${escapeHtml(a.description)}</td><td class="evidence">${evidence(a)}</td></tr>`).join("")
      : '<tr><td colspan="8" class="text-muted">No alerts</td></tr>';
  }

  async function loadTimeline() {
    const {rows} = await api("/api/security/timeline?hours=72");
    const groups = Charts.groupBy(rows, "classification");
    Charts.render("chart-timeline", Object.keys(groups).sort().map((c, i) => ({
      x: groups[c].map((r) => r.hour), y: groups[c].map((r) => r.alerts), name: fmt.label(c), type: "bar",
      marker: {color: Charts.PALETTE[i % Charts.PALETTE.length]}})), {barmode: "stack", yaxis: {title: "alerts"}});
  }

  async function loadAttacks() {
    const {rows} = await api("/api/security/attacks?limit=50");
    document.getElementById("attack-table").innerHTML = rows.length ? rows.map((a) => {
      const latency = a.first_alert_at ? Math.round((new Date(a.first_alert_at) - new Date(a.starts_at)) / 60000) : null;
      return `<tr><td>${fmt.time(a.requested_at)}</td><td>${escapeHtml(fmt.label(a.attack_type))}</td><td>${escapeHtml(a.zone_id)}</td>
        <td>${fmt.time(a.starts_at)} → ${fmt.time(a.ends_at).slice(11)}</td><td><code class="params">${escapeHtml(JSON.stringify(a.params))}</code></td>
        <td>${escapeHtml(a.readings_manipulated)}</td><td>${escapeHtml(a.alerts_during_attack)}</td>
        <td>${a.first_alert_at ? `${fmt.time(a.first_alert_at).slice(11)} (${latency} min)` : '<span class="text-muted">not detected (yet)</span>'}</td></tr>`;
    }).join("") : '<tr><td colspan="8" class="text-muted">No simulated attacks — trigger one with the controls</td></tr>';
  }

  async function loadIsolated() {
    const {rows} = await api("/api/security/isolated");
    document.getElementById("isolated-list").innerHTML = rows.length ? rows.map((i) =>
      `<li class="list-group-item"><i class="bi bi-slash-circle text-danger"></i> <strong>${escapeHtml(i.zone_id)} / ${escapeHtml(i.sensor)}</strong>
       <span class="text-muted">since ${fmt.time(i.since)}</span><div class="text-muted">${escapeHtml(i.reason || "")}</div></li>`).join("")
      : '<li class="list-group-item text-muted">No sensors isolated</li>';
  }

  async function loadDrift() {
    const {rows} = await api("/api/security/drift?limit=100");
    const points = {};
    // Only the features that met the drift criteria in that window.
    rows.forEach((a) => (a.features || []).filter((f) => ((a.details || {}).drifting_features || []).includes(f.feature)).forEach((f) => {
      const key = `${a.zone_id} ${f.feature}${a.classification === "data_drift" ? "" : " (security)"}`;
      const p = points[key] = points[key] || {x: [], y: [], security: a.classification !== "data_drift"};
      p.x.push(a.detected_at); p.y.push(f.psi);
    }));
    const traces = Object.entries(points).map(([name, p], i) => ({
      x: p.x, y: p.y, name, type: "scatter", mode: "markers",
      marker: {size: 8, symbol: p.security ? "diamond" : "circle", color: Charts.PALETTE[i % Charts.PALETTE.length]}}));
    Charts.render("chart-drift", traces, {yaxis: {title: "PSI (≥ 0.25 = significant)", type: "log"}, hovermode: "closest",
      shapes: [{type: "line", xref: "paper", x0: 0, x1: 1, y0: 0.25, y1: 0.25, line: {dash: "dash", color: "#dc3545"}}]},
      "No drift alerts");
  }

  const loadAll = () => Promise.allSettled([loadAlerts(), loadTimeline(), loadAttacks(), loadIsolated(), loadDrift()]);
  App.on("alerts", () => Promise.allSettled([loadAlerts(), loadTimeline(), loadAttacks(), loadDrift()]));
  App.on("actions", (rows) => { if (rows.some((r) => r.action_type.endsWith("_sensor"))) loadIsolated().catch(() => {}); });
  document.addEventListener("DOMContentLoaded", () => {
    ["filter-type", "filter-zone"].forEach((id) => document.getElementById(id).addEventListener("change", () => loadAlerts().catch(() => {})));
    loadAll();
    setInterval(() => loadAttacks().catch(() => {}), 15000);   // manipulated-reading counters
  });
})();
