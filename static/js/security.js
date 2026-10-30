/* Security monitoring: alert timeline, alert-type legend, simulated attacks, quarantined sensors, drift, alerts. */
"use strict";

(() => {
  const {api, escapeHtml, fmt} = App;
  const FEATURE_COLORS = {temperature_c: "#ef4444", humidity_pct: "#3b82f6", co2_ppm: "#8b5cf6", total_power_kw: "#f59e0b",
                          control_error_active: "#dc2626", co2_residual: "#b91c1c"};

  function pValue(p) {
    if (p == null) return "—";
    return p < 0.001 ? Number(p).toExponential(0) : Number(p).toFixed(3);
  }

  function evidence(a) {
    const d = a.details || {};
    if (d.rules && d.rules.length) return d.rules.map((r) => `${escapeHtml(fmt.label(r.rule))}: ${escapeHtml(r.detail)}`).join("<br>");
    if (d.features) {
      return d.features.filter((f) => (d.drifting_features || []).includes(f.feature)).map((f) => {
        const feat = Glossary.feature(f.feature);
        return `<strong>${escapeHtml(feat.label)}</strong>: ${escapeHtml(Glossary.psiStrength(f.psi))}
          <span class="text-muted">(PSI ${fmt.num(f.psi, 2)}, KS p ${pValue(f.ks_p_value)})</span>`;
      }).join("<br>");
    }
    if (d.isolation_forest) {
      return `Reading looks unlike normal data: anomaly score ${fmt.num(d.isolation_forest.score, 3)}, ` +
        `limit ${fmt.num(d.isolation_forest.threshold, 3)}` + (d.most_unusual_feature ? `; most unusual: ${escapeHtml(Glossary.feature(d.most_unusual_feature.feature).label)}` : "");
    }
    return "";
  }

  function classChip(name) {
    const c = Glossary.classification(name);
    return `<span class="chip" style="background:${c.color}1f;color:${c.color}"><i class="bi bi-${c.icon}"></i> ${escapeHtml(c.label)}</span>`;
  }

  async function loadAlerts() {
    const type = document.getElementById("filter-type").value, zone = document.getElementById("filter-zone").value;
    const {rows} = await api(`/api/security/alerts?limit=200&type=${encodeURIComponent(type)}&zone_id=${encodeURIComponent(zone)}`);
    document.getElementById("alert-table").innerHTML = rows.length ? rows.map((a) => `<tr>
      <td class="text-nowrap">${fmt.time(a.detected_at)}</td><td>${escapeHtml(a.zone_id || "—")}</td>
      <td>${classChip(a.classification || a.alert_type)}<div class="text-muted mt-1">${escapeHtml(Glossary.humanize(a.description))}</div></td>
      <td>${App.severityBadge(a.severity)}</td><td>${escapeHtml(Glossary.detector(a.detector))}</td>
      <td class="evidence">${evidence(a)}</td></tr>`).join("")
      : '<tr><td colspan="6" class="text-muted">No alerts</td></tr>';
  }

  async function loadTimeline() {
    const {rows} = await api("/api/security/timeline?hours=72");
    const groups = Charts.groupBy(rows, "classification");
    Charts.render("chart-timeline", Object.keys(groups).sort().map((c) => {
      const info = Glossary.classification(c);
      return {x: groups[c].map((r) => r.hour), y: groups[c].map((r) => r.alerts), name: info.label, type: "bar",
              marker: {color: info.color}, hovertemplate: `%{y} × ${info.label}<extra></extra>`};
    }), {barmode: "stack", yaxis: {title: "alerts per hour"}}, "No alerts yet");
  }

  function renderLegend() {
    document.getElementById("class-legend").innerHTML = Object.entries(Glossary.CLASSIFICATIONS)
      .filter(([k]) => k !== "invalid_message").map(([, c]) =>
        `<div class="legend-item"><span class="swatch" style="background:${c.color}"></span><div>
          <strong><i class="bi bi-${c.icon}"></i> ${escapeHtml(c.label)}</strong><span class="text-muted">${escapeHtml(c.plain)}</span></div></div>`).join("");
  }

  async function loadAttacks() {
    const {rows} = await api("/api/security/attacks?limit=50");
    document.getElementById("attack-table").innerHTML = rows.length ? rows.map((a) => {
      const latency = a.first_alert_at ? Math.round((new Date(a.first_alert_at) - new Date(a.starts_at)) / 60000) : null;
      const detected = a.first_alert_at
        ? `<span class="chip" style="background:#dcfce7;color:#166534"><i class="bi bi-check-circle-fill"></i> yes, after ${latency} min</span>`
        : `<span class="chip"><i class="bi bi-hourglass"></i> not yet</span>`;
      return `<tr><td>${classChip(a.attack_type)}</td><td>${escapeHtml(a.zone_id)}</td>
        <td class="text-nowrap">${fmt.time(a.starts_at)} → ${fmt.time(a.ends_at).slice(11)}</td>
        <td>${escapeHtml(a.readings_manipulated)}</td><td>${escapeHtml(a.alerts_during_attack)}</td><td>${detected}</td></tr>`;
    }).join("") : '<tr><td colspan="6" class="text-muted">No simulated attacks yet. Launch one with the panel above.</td></tr>';
  }

  async function loadIsolated() {
    const {rows} = await api("/api/security/isolated");
    document.getElementById("isolated-list").innerHTML = rows.length ? rows.map((i) =>
      `<li class="list-group-item"><i class="bi bi-slash-circle text-danger"></i> <strong>${escapeHtml(i.zone_id)}: ${escapeHtml(Glossary.sensor(i.sensor))} sensor</strong>
       <span class="text-muted">since ${fmt.time(i.since)}</span><div class="text-muted">${escapeHtml(i.reason || "")}</div></li>`).join("")
      : '<li class="list-group-item text-muted">No sensors quarantined</li>';
  }

  async function loadDrift() {
    const {rows} = await api("/api/security/drift?limit=100");
    const traces = {};
    // Only the measures that met the drift criteria in that window; one legend entry per (kind, measure).
    rows.forEach((a) => (a.features || []).filter((f) => ((a.details || {}).drifting_features || []).includes(f.feature)).forEach((f) => {
      const security = a.classification !== "data_drift";
      const feat = Glossary.feature(f.feature);
      const key = `${security ? "s" : "d"}:${f.feature}`;
      const t = traces[key] = traces[key] || {x: [], y: [], text: [], security, label: feat.label, color: FEATURE_COLORS[f.feature]};
      t.x.push(a.detected_at); t.y.push(Math.max(f.psi, 0.01));
      t.text.push(`<b>${escapeHtml(a.zone_id)} · ${escapeHtml(feat.label)}</b><br>${security ? "security drift (possible attack)" : "data drift (not an attack)"}` +
                  `<br>PSI ${fmt.num(f.psi, 2)}: ${Glossary.psiStrength(f.psi)}<br>KS p-value ${pValue(f.ks_p_value)}` +
                  (f.ks_p_value != null && f.ks_p_value < 0.001 ? " (certainly not chance)" : ""));
    }));
    const list = Object.values(traces).sort((a, b) => Number(b.security) - Number(a.security) || a.label.localeCompare(b.label));
    const data = list.map((t, i) => ({
      x: t.x, y: t.y, text: t.text, hovertemplate: "%{text}<extra></extra>", type: "scatter", mode: "markers",
      name: t.label, legendgroup: t.security ? "security" : "data",
      legendgrouptitle: {text: t.security ? "◆ Security drift" : "● Data drift"},
      marker: {size: t.security ? 12 : 9, symbol: t.security ? "diamond" : "circle", opacity: 0.85,
               color: t.color || Charts.PALETTE[i % Charts.PALETTE.length], line: {width: 1, color: "#fff"}}}));
    const band = (y0, y1, color) => ({type: "rect", xref: "paper", x0: 0, x1: 1, y0, y1, fillcolor: color, line: {width: 0}, layer: "below"});
    Charts.render("chart-drift", data, {
      yaxis: {title: "PSI: how much values changed (log scale)", type: "log", range: [-2, 1.2]},
      hovermode: "closest", legend: {orientation: "v", x: 1.02, y: 1, groupclick: "toggleitem"}, margin: {l: 60, r: 10, t: 10, b: 40},
      shapes: [band(0.01, 0.1, "rgba(34,197,94,.08)"), band(0.1, 0.25, "rgba(245,158,11,.10)"), band(0.25, 20, "rgba(220,38,38,.05)"),
               {type: "line", xref: "paper", x0: 0, x1: 1, y0: 0.25, y1: 0.25, line: {dash: "dash", color: "#dc2626"}}],
      annotations: [{xref: "paper", x: 0, y: Math.log10(0.25), yref: "y", text: "significant", showarrow: false,
                     xanchor: "left", yanchor: "bottom", font: {color: "#dc2626", size: 10}}]},
      "No drift detected yet");
  }

  const loadAll = () => Promise.allSettled([loadAlerts(), loadTimeline(), loadAttacks(), loadIsolated(), loadDrift()]);
  App.on("alerts", () => Promise.allSettled([loadAlerts(), loadTimeline(), loadAttacks(), loadDrift()]));
  App.on("actions", (rows) => { if (rows.some((r) => r.action_type.endsWith("_sensor"))) loadIsolated().catch(() => {}); });
  document.addEventListener("DOMContentLoaded", () => {
    renderLegend();
    ["filter-type", "filter-zone"].forEach((id) => document.getElementById(id).addEventListener("change", () => loadAlerts().catch(() => {})));
    loadAll();
    setInterval(() => loadAttacks().catch(() => {}), 15000);   // manipulated-reading counters
  });
})();
