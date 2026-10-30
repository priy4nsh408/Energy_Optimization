/* Main dashboard: KPIs, live zone cards, live power chart, alert feed, latest agent decision. */
"use strict";

(() => {
  const {api, escapeHtml, fmt} = App;
  const WINDOW_MS = 6 * 3600 * 1000;        // 6 h of simulated time
  const ZONE_NAMES = Object.fromEntries((window.DASHBOARD.zones || []).map(([id, name]) => [id, name]));
  let series = {};                          // zone -> {t: [], power: []}
  let alertedZones = new Set();
  let lastLiveLoad = 0;
  let previousTimes = {};

  function trimWindow() {
    const latest = Math.max(...Object.values(series).map((s) => s.t.length ? Date.parse(s.t[s.t.length - 1]) : 0));
    Object.values(series).forEach((s) => {
      while (s.t.length && Date.parse(s.t[0]) < latest - WINDOW_MS) { s.t.shift(); s.power.shift(); }
    });
  }

  function renderPower() {
    trimWindow();
    const zones = Object.keys(series).sort();
    // Plotly.react only redraws arrays it has not seen before, so every render passes fresh copies.
    const traces = zones.map((zone, i) => ({
      x: [...series[zone].t], y: [...series[zone].power], name: `${zone} ${ZONE_NAMES[zone] || ""}`.trim(),
      type: "scatter", mode: "lines", line: {color: Charts.zoneColor(zone, i), width: 2},
      hovertemplate: "%{y:.2f} kW<extra>" + escapeHtml(zone) + "</extra>"}));
    const totals = {};
    zones.forEach((zone) => series[zone].t.forEach((t, i) => { totals[t] = (totals[t] || 0) + (series[zone].power[i] || 0); }));
    const times = Object.keys(totals).sort();
    if (times.length) {
      traces.push({x: times, y: times.map((t) => totals[t]), name: "whole building", type: "scatter", mode: "lines",
                   line: {color: "#334155", width: 2, dash: "dot"}, yaxis: "y2",
                   hovertemplate: "%{y:.1f} kW<extra>building</extra>"});
    }
    Charts.render("chart-power", traces, {
      yaxis: {title: "kW per zone", rangemode: "tozero"},
      yaxis2: {title: "kW building", overlaying: "y", side: "right", rangemode: "tozero", showgrid: false},
      margin: {l: 50, r: 55, t: 10, b: 40}}, "No readings yet. Press Start to run the simulation.");
    const latest = times[times.length - 1];
    document.getElementById("power-updated").textContent = latest ? `simulated time ${fmt.time(latest)}` : "—";
  }

  function level(value, good, ok) {
    if (value == null) return "";
    return value <= good ? "good" : value <= ok ? "ok" : "bad";
  }

  function zoneCard(r, maxPower, index) {
    const occupied = Number(r.occupancy_count) > 0;
    const gap = Math.abs(Number(r.temperature_c) - Number(r.hvac_setpoint_c));
    const tempLevel = occupied ? level(gap, 1, 2) : "";
    const co2Level = level(Number(r.co2_ppm), 800, 1200);
    const humid = Number(r.humidity_pct);
    const humidLevel = humid >= 30 && humid <= 60 ? "good" : humid >= 20 && humid <= 70 ? "ok" : "bad";
    const hvac = r.hvac_status && r.hvac_status !== "off" ? `${escapeHtml(r.hvac_status)}` : "off";
    const pct = maxPower ? Math.min(100, 100 * (r.total_power_kw || 0) / maxPower) : 0;
    const color = Charts.zoneColor(r.zone_id, index);
    const changed = previousTimes[r.zone_id] && previousTimes[r.zone_id] !== r.event_time;
    const isolated = (r.isolated_sensors || []).length
      ? `<div class="isolated text-danger"><i class="bi bi-slash-circle"></i> not trusted: ${escapeHtml(r.isolated_sensors.map(Glossary.sensor).join(", "))} sensor</div>` : "";
    return `<div class="col-12 col-sm-6 col-lg-4"><div class="zone-card ${alertedZones.has(r.zone_id) ? "alerted" : ""} ${changed ? "updated" : ""}"
        style="--zone-color:${color}" id="zone-${escapeHtml(r.zone_id)}">
      <div class="d-flex justify-content-between align-items-start">
        <div><div class="zone-title">${escapeHtml(r.zone_id)} · ${escapeHtml(ZONE_NAMES[r.zone_id] || "")}</div>
          <div class="zone-sub">${escapeHtml(fmt.label(r.zone_type || ""))}</div></div>
        <span class="chip" title="Heating / cooling"><i class="bi bi-fan"></i> ${hvac}</span></div>
      <div class="temp mt-1">${fmt.num(r.temperature_c)} °C <small>target ${fmt.num(r.hvac_setpoint_c)} °C</small></div>
      <div class="chips">
        <span class="chip ${tempLevel}" title="Distance from the target temperature (judged only when people are present)">
          <i class="bi bi-thermometer-half"></i> ${occupied ? (tempLevel === "good" ? "comfortable" : `${fmt.num(gap)} °C off`) : "empty"}</span>
        <span class="chip ${co2Level}" title="CO₂: below 800 ppm good, above 1200 ppm stuffy"><i class="bi bi-wind"></i> CO₂ ${fmt.num(r.co2_ppm, 0)} ppm</span>
        <span class="chip ${humidLevel}" title="Relative humidity: 30–60 % comfortable"><i class="bi bi-droplet"></i> ${fmt.num(r.humidity_pct, 0)} %</span>
        <span class="chip" title="People in the zone"><i class="bi bi-people"></i> ${escapeHtml(r.occupancy_count)}</span>
        <span class="chip" title="Lights"><i class="bi bi-lightbulb"></i> ${fmt.num(r.lighting_level_pct, 0)} %</span>
      </div>
      <div class="power-bar"><span style="width:${pct.toFixed(0)}%"></span></div>
      <div class="power-line"><span><i class="bi bi-lightning-charge"></i> <strong>${fmt.num(r.total_power_kw, 2)} kW</strong></span>
        <span class="text-muted">HVAC ${fmt.num(r.hvac_power_kw, 1)} · light ${fmt.num(r.lighting_power_kw, 1)} · devices ${fmt.num(r.appliance_power_kw, 1)}</span></div>
      ${isolated}</div></div>`;
  }

  async function loadLive() {
    lastLiveLoad = Date.now();
    const {readings} = await api("/api/live");
    const maxPower = Math.max(1, ...readings.map((r) => r.total_power_kw || 0),
                              ...Object.values(series).flatMap((s) => s.power));
    document.getElementById("zone-cards").innerHTML = readings.length
      ? readings.map((r, i) => zoneCard(r, maxPower, i)).join("")
      : '<div class="empty-state"><i class="bi bi-hourglass-split"></i>No readings yet. Press <strong>Start</strong> in the simulation panel.</div>';
    previousTimes = Object.fromEntries(readings.map((r) => [r.zone_id, r.event_time]));
    const latest = readings.map((r) => r.event_time).sort().pop();
    document.getElementById("zones-updated").textContent = latest ? `simulated time ${fmt.time(latest)}` : "—";
    const total = readings.reduce((s, r) => s + (r.total_power_kw || 0), 0);
    document.getElementById("kpi-power").textContent = readings.length ? `${fmt.num(total, 1)} kW` : "—";
    const people = readings.reduce((s, r) => s + (Number(r.occupancy_count) || 0), 0);
    document.getElementById("kpi-power-sub").textContent = readings.length ? `${readings.length} zones · ${people} people inside` : "no data";
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
      ? `heating/cooling ${fmt.num(s.hvac_kwh, 0)} · lights ${fmt.num(s.lighting_kwh, 0)} · devices ${fmt.num(s.appliance_kwh, 0)} kWh` : "no energy records yet";
    document.getElementById("kpi-carbon").textContent = s.carbon_kg_co2e != null ? `${fmt.num(s.carbon_kg_co2e, 1)} kg` : "—";
    document.getElementById("kpi-carbon-sub").textContent = s.readings_without_carbon
      ? `${s.readings_without_carbon} readings still without grid data` : "from the grid's carbon intensity";
  }

  function alertItem(a) {
    const c = Glossary.classification(a.classification || a.alert_type);
    return `<li class="list-group-item"><div class="d-flex justify-content-between gap-2">
      <span><span class="chip" style="background:${c.color}22;color:${c.color}"><i class="bi bi-${c.icon}"></i> ${escapeHtml(c.label)}</span>
        <strong class="ms-1">${escapeHtml(a.zone_id || "building")}</strong></span>
      <span class="text-muted text-nowrap">${fmt.time(a.detected_at).slice(5)}</span></div>
      <div class="text-muted mt-1">${escapeHtml(Glossary.humanize(a.description))} · ${App.severityBadge(a.severity)}</div></li>`;
  }

  async function loadAlerts() {
    const {rows} = await api("/api/security/alerts?limit=8");
    document.getElementById("alert-feed").innerHTML = rows.length ? rows.map(alertItem).join("")
      : '<li class="list-group-item text-muted">No alerts yet</li>';
  }

  function decisionSummary(d) {
    const v = Verdict.of(d);
    return `<div class="d-flex justify-content-between align-items-center mb-2">
        <strong>${escapeHtml(d.zone_id || "building")} · ${escapeHtml(Glossary.classification(d.classification).label)}</strong>
        <span class="chip" style="background:${v.bg};color:${v.fg}"><i class="bi bi-${v.icon}"></i> ${escapeHtml(v.label)}</span></div>
      <div>${escapeHtml(Verdict.story(d))}</div>`;
  }

  async function loadDecision() {
    const {rows} = await api("/api/agent/decisions?limit=1");
    if (rows.length) document.getElementById("latest-decision").innerHTML = decisionSummary(rows[0]);
  }

  App.on("readings", (rows) => {
    rows.forEach((r) => {
      const s = series[r.zone_id] = series[r.zone_id] || {t: [], power: []};
      if (s.t.length && r.event_time <= s.t[s.t.length - 1]) return;     // already shown
      s.t.push(r.event_time); s.power.push(r.total_power_kw);
    });
    renderPower();
    if (Date.now() - lastLiveLoad > 1500) loadLive().catch(() => {});
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
      `${counts.agent_decisions ?? 0} checked by the AI agent · ${counts.simulated_attacks ?? 0} simulated attacks`;
  });

  document.addEventListener("DOMContentLoaded", () => {
    Promise.allSettled([loadPower(), loadSummary(), loadAlerts(), loadDecision()]).then(() => loadLive().catch(() => {}));
    setInterval(() => loadSummary().catch(() => {}), 30000);
  });
})();
