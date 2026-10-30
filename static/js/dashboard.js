/* Main dashboard: KPIs, live zone cards, live power chart, alert feed, latest agent decision. */
"use strict";

(() => {
  const {api, escapeHtml, fmt} = App;
  const MAX_POINTS = 6 * 12 + 1;            // 6 h of 5-minute readings per zone
  let series = {};                          // zone -> {t: [], power: []}
  let alertedZones = new Set();

  function renderPower() {
    const traces = Object.keys(series).sort().map((zone, i) => ({
      x: series[zone].t, y: series[zone].power, name: zone, type: "scatter", mode: "lines",
      line: {color: Charts.zoneColor(zone, i), width: 2}}));
    Charts.render("chart-power", traces, {yaxis: {title: "kW"}}, "No readings yet — start the simulation");
  }

  function zoneCard(r) {
    const isolated = (r.isolated_sensors || []).length
      ? `<div class="isolated text-danger"><i class="bi bi-slash-circle"></i> isolated: ${escapeHtml(r.isolated_sensors.join(", "))}</div>` : "";
    return `<div class="col-12 col-sm-6 col-lg-4 col-xxl"><div class="zone-card ${alertedZones.has(r.zone_id) ? "alerted" : ""}" id="zone-${escapeHtml(r.zone_id)}">
      <div class="d-flex justify-content-between"><span class="zone-title">${escapeHtml(r.zone_id)}</span>
        <span class="small text-muted">${escapeHtml(r.zone_type || "")}</span></div>
      <div class="metric"><strong>${fmt.num(r.temperature_c)} °C</strong> (set ${fmt.num(r.hvac_setpoint_c)} °C, ${escapeHtml(r.hvac_mode)}/${escapeHtml(r.hvac_status)})</div>
      <div class="metric">CO₂ <strong>${fmt.num(r.co2_ppm, 0)}</strong> ppm · RH ${fmt.num(r.humidity_pct, 0)} %</div>
      <div class="metric">Occupancy <strong>${escapeHtml(r.occupancy_count)}</strong> · light ${fmt.num(r.lighting_level_pct, 0)} % (${fmt.num(r.lighting_lux, 0)} lx)</div>
      <div class="metric">Power <strong>${fmt.num(r.total_power_kw, 2)} kW</strong>
        <span class="text-muted">(HVAC ${fmt.num(r.hvac_power_kw, 2)}, light ${fmt.num(r.lighting_power_kw, 2)}, appl. ${fmt.num(r.appliance_power_kw, 2)})</span></div>
      <div class="metric text-muted">${fmt.time(r.event_time)}</div>${isolated}</div></div>`;
  }

  async function loadLive() {
    const {readings} = await api("/api/live");
    document.getElementById("zone-cards").innerHTML = readings.length ? readings.map(zoneCard).join("")
      : '<p class="text-muted mb-0">No readings yet. Start <code>workers.simulation_manager</code> and press Start.</p>';
    const total = readings.reduce((s, r) => s + (r.total_power_kw || 0), 0);
    document.getElementById("kpi-power").textContent = readings.length ? `${fmt.num(total, 1)} kW` : "—";
    document.getElementById("kpi-power-sub").textContent = readings.length ? `${readings.length} zones` : "no data";
  }

  async function loadPower() {
    const {zones} = await api("/api/energy/power?hours=6");
    series = {};
    Object.entries(zones).forEach(([zone, s]) => { series[zone] = {t: s.t, power: s.power_kw}; });
    renderPower();
  }

  async function loadSummary() {
    const s = await api("/api/energy/summary?hours=24");
    document.getElementById("kpi-energy").textContent = s.energy_kwh != null ? `${fmt.num(s.energy_kwh, 0)} kWh` : "—";
    document.getElementById("kpi-energy-sub").textContent = s.energy_kwh != null
      ? `HVAC ${fmt.num(s.hvac_kwh, 0)} · light ${fmt.num(s.lighting_kwh, 0)} · appl. ${fmt.num(s.appliance_kwh, 0)}` : "no energy records";
    document.getElementById("kpi-carbon").textContent = s.carbon_kg_co2e != null ? `${fmt.num(s.carbon_kg_co2e, 1)} kg` : "—";
    document.getElementById("kpi-carbon-sub").textContent = s.readings_without_carbon
      ? `${s.readings_without_carbon} readings without carbon data` : "CO₂e from grid intensity";
  }

  function alertItem(a) {
    return `<li class="list-group-item"><div class="d-flex justify-content-between">
      <span>${App.severityBadge(a.severity)} ${escapeHtml(a.zone_id || "building")} · ${escapeHtml(fmt.label(a.classification || a.alert_type))}</span>
      <span class="text-muted">${fmt.time(a.detected_at)}</span></div>
      <div class="text-muted">${escapeHtml(a.description)}</div></li>`;
  }

  async function loadAlerts() {
    const {rows} = await api("/api/security/alerts?limit=8");
    document.getElementById("alert-feed").innerHTML = rows.length ? rows.map(alertItem).join("")
      : '<li class="list-group-item text-muted">No alerts yet</li>';
  }

  function decisionSummary(d) {
    return `<div class="mb-1"><strong>${escapeHtml(d.zone_id || "building")}</strong> ·
      ${escapeHtml(fmt.label(d.classification || ""))} · <span class="text-muted">${escapeHtml(d.llm_provider)}</span></div>
      <div><strong>Cause:</strong> ${escapeHtml(d.likely_cause)} (confidence ${escapeHtml(d.confidence)})</div>
      <div class="text-muted">${escapeHtml(d.diagnosis).slice(0, 300)}</div>
      <div class="mt-1"><strong>Outcome:</strong> ${escapeHtml(d.outcome)}</div>`;
  }

  async function loadDecision() {
    const {rows} = await api("/api/agent/decisions?limit=1");
    if (rows.length) document.getElementById("latest-decision").innerHTML = decisionSummary(rows[0]);
  }

  App.on("readings", (rows) => {
    rows.forEach((r) => {
      const s = series[r.zone_id] = series[r.zone_id] || {t: [], power: []};
      s.t.push(r.event_time); s.power.push(r.total_power_kw);
      if (s.t.length > MAX_POINTS) { s.t.shift(); s.power.shift(); }
    });
    renderPower();
    loadLive().catch(() => {});
  });
  App.on("alerts", (rows) => {
    rows.forEach((a) => a.zone_id && alertedZones.add(a.zone_id));
    const feed = document.getElementById("alert-feed");
    if (feed.querySelector(".text-muted:only-child")) feed.innerHTML = "";
    rows.slice().reverse().forEach((a) => feed.insertAdjacentHTML("afterbegin", alertItem(a).replace("list-group-item", "list-group-item flash")));
    while (feed.children.length > 8) feed.lastElementChild.remove();
  });
  App.on("decisions", (rows) => { if (rows.length) document.getElementById("latest-decision").innerHTML = decisionSummary(rows[rows.length - 1]); });
  App.on("status", (s) => {
    const counts = s.counts || {};
    document.getElementById("kpi-alerts").textContent = counts.open_alerts ?? "—";
    document.getElementById("kpi-alerts-sub").textContent =
      `${counts.agent_decisions ?? 0} agent decisions · ${counts.simulated_attacks ?? 0} simulated attacks`;
  });

  document.addEventListener("DOMContentLoaded", () => {
    Promise.allSettled([loadLive(), loadPower(), loadSummary(), loadAlerts(), loadDecision()]);
    setInterval(() => loadSummary().catch(() => {}), 30000);
  });
})();
