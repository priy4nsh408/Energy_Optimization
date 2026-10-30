/* Energy analytics: consumption, carbon, predictions vs. actual, baseline vs. optimized. */
"use strict";

(() => {
  const {api, escapeHtml, fmt} = App;
  const hours = () => document.getElementById("hours").value;
  const STATUS_TEXT = {approved: "recommended", executed: "applied to the building", rejected: "blocked by a safety limit",
                       failed: "failed", proposed: "proposed"};

  function kpi(icon, color, label, value, sub) {
    return `<div class="col-6 col-lg-3"><div class="card kpi h-100"><div class="card-body">
      <div class="kpi-icon ${color}"><i class="bi bi-${icon}"></i></div><div>
      <div class="kpi-label">${escapeHtml(label)}</div><div class="kpi-value">${escapeHtml(value)}</div>
      <div class="kpi-sub">${escapeHtml(sub)}</div></div></div></div></div>`;
  }

  function headline(value, label, good = true) {
    return `<div class="col"><div class="border rounded-3 p-2 text-center h-100">
      <div class="fs-5 fw-bold ${good ? "text-success" : "text-danger"}">${escapeHtml(value)}</div>
      <div class="small text-muted">${escapeHtml(label)}</div></div></div>`;
  }

  async function loadSummary() {
    const s = await api(`/api/energy/summary?hours=${hours()}`);
    const total = s.energy_kwh || 0;
    const share = (v) => total ? `${fmt.num(100 * v / total, 0)} % of the total` : "—";
    document.getElementById("energy-kpis").innerHTML = [
      kpi("battery-charging", "bg-grad-green", "Energy used", s.energy_kwh != null ? `${fmt.num(total, 1)} kWh` : "—",
          s.since ? `${fmt.time(s.since)} → ${fmt.time(s.until)}` : "no data yet"),
      kpi("fan", "bg-grad-blue", "Heating & cooling", `${fmt.num(s.hvac_kwh, 1)} kWh`, share(s.hvac_kwh || 0)),
      kpi("lightbulb", "bg-grad-orange", "Lights & devices", `${fmt.num((s.lighting_kwh || 0) + (s.appliance_kwh || 0), 1)} kWh`,
          share((s.lighting_kwh || 0) + (s.appliance_kwh || 0))),
      kpi("cloud-haze2-fill", "bg-grad-teal", "CO₂ emitted", s.carbon_kg_co2e != null ? `${fmt.num(s.carbon_kg_co2e, 1)} kg` : "—",
          s.readings_without_carbon ? `${s.readings_without_carbon} readings still without grid data` : "energy × grid carbon intensity"),
    ].join("");
  }

  async function loadHourly() {
    const {rows} = await api(`/api/energy/hourly?hours=${hours()}`);
    const groups = Charts.groupBy(rows, "zone_id");
    const zones = Object.keys(groups).sort();
    const names = Object.fromEntries((window.DASHBOARD.zones || []).map(([id, name]) => [id, `${id} ${name}`]));
    Charts.render("chart-hourly", zones.map((z, i) => ({
      x: groups[z].map((r) => r.hour), y: groups[z].map((r) => r.energy_kwh), name: names[z] || z, type: "bar",
      marker: {color: Charts.zoneColor(z, i)}, hovertemplate: "%{y:.2f} kWh<extra>" + escapeHtml(z) + "</extra>"})),
      {barmode: "stack", yaxis: {title: "kWh per hour"}}, "No energy data yet. Press Start on the dashboard.");
    Charts.render("chart-carbon", zones.map((z, i) => ({
      x: groups[z].map((r) => r.hour), y: groups[z].map((r) => r.carbon_kg_co2e), name: names[z] || z, type: "scatter",
      mode: "lines", stackgroup: "carbon", line: {color: Charts.zoneColor(z, i)},
      hovertemplate: "%{y:.2f} kg<extra>" + escapeHtml(z) + "</extra>"})), {yaxis: {title: "kg CO₂e per hour"}},
      "No carbon data yet");
    const missing = rows.filter((r) => r.carbon_kg_co2e === null).length;
    document.getElementById("carbon-note").textContent = missing
      ? `${missing} zone-hours have no carbon intensity yet (the external data service has not delivered it).`
      : "Stacked by zone: the top edge is the whole building.";
  }

  async function loadPredictions() {
    const zone = document.getElementById("pred-zone").value;
    const {rows, accuracy} = await api(`/api/energy/predictions?hours=${Math.min(hours(), 48)}&zone_id=${encodeURIComponent(zone)}`);
    Charts.render("chart-predictions", [
      {x: rows.map((r) => r.predicted_for), y: rows.map((r) => r.actual_power_kw), name: "measured", type: "scatter",
       mode: "lines", line: {color: "#1f2a37", width: 2}},
      {x: rows.map((r) => r.predicted_for), y: rows.map((r) => r.predicted_power_kw), name: "predicted by ML", type: "scatter",
       mode: "lines", line: {color: "#2563eb", dash: "dot", width: 2}},
    ], {yaxis: {title: "kW"}}, "No predictions yet");
    document.getElementById("accuracy-table").innerHTML = accuracy.length ? accuracy.map((a) => {
      const pct = a.mean_actual_kw ? 100 * a.mae_kw / a.mean_actual_kw : null;
      return `<tr><td>${escapeHtml(a.zone_id)}</td><td>${escapeHtml(a.n)}</td><td>${fmt.num(a.mae_kw, 3)} kW</td>
       <td>${fmt.num(a.mean_actual_kw, 2)} kW</td><td>${pct == null ? "—" : fmt.pct(pct)}</td></tr>`;
    }).join("") : '<tr><td colspan="5" class="text-muted">No predictions stored</td></tr>';
  }

  function renderExperiment(experiment) {
    const table = document.getElementById("comparison-table");
    if (!experiment) {
      table.innerHTML = '<tr><td colspan="7" class="text-muted">Run python -m experiments.comparison --json docs/results/optimization_comparison.json</td></tr>';
      Charts.render("chart-comparison", [], {}, "No experiment results file");
      return;
    }
    const rows = [], heads = [], bars = {baseline: {x: [], y: []}, optimized: {x: [], y: []}};
    Object.entries(experiment.cases).forEach(([week, results]) => {
      const base = results[0];
      // The live system optimizes with ML forecasts; fall back to the measured-basis run if there is none.
      const opt = results.find((r) => r.name.includes("ML")) || results[results.length - 1];
      results.forEach((r) => {
        const saving = base.energy_kwh ? 100 * (1 - r.energy_kwh / base.energy_kwh) : null;
        const name = r.name === "baseline" ? "baseline (normal schedule)" : r.name.replace(/, \d{8}T\d{6}Z/, "");
        rows.push(`<tr><td>${escapeHtml(week)}</td><td>${escapeHtml(name)}</td><td>${fmt.num(r.energy_kwh, 0)} kWh</td>
          <td>${r === base ? "—" : `<strong class="text-success">${fmt.pct(saving)}</strong>`}</td><td>${fmt.num(r.carbon_kg, 0)} kg</td>
          <td>${fmt.pct(r.comfort_compliance_pct)}</td><td>${fmt.num(r.peak_power_kw, 1)} kW</td></tr>`);
      });
      bars.baseline.x.push(week); bars.baseline.y.push(base.energy_kwh);
      bars.optimized.x.push(week); bars.optimized.y.push(opt.energy_kwh);
      heads.push(headline(`−${fmt.num(100 * (1 - opt.energy_kwh / base.energy_kwh), 1)} %`, `energy, ${week}`));
      heads.push(headline(`−${fmt.num(base.carbon_kg - opt.carbon_kg, 0)} kg`, `CO₂e, ${week}`));
    });
    document.getElementById("comparison-headline").innerHTML = heads.join("");
    table.innerHTML = rows.join("");
    Charts.render("chart-comparison", [
      {...bars.baseline, name: "baseline (normal schedule)", type: "bar", marker: {color: "#94a3b8"},
       hovertemplate: "%{y:.0f} kWh<extra>baseline</extra>"},
      {...bars.optimized, name: "optimized", type: "bar", marker: {color: "#16a34a"},
       hovertemplate: "%{y:.0f} kWh<extra>optimized</extra>"},
    ], {barmode: "group", yaxis: {title: "kWh per week"}, hovermode: "closest"});
    document.getElementById("comparison-note").textContent =
      `“Comfortable” is the share of occupied time inside the comfort temperature band. Source: ${experiment.source}; ` +
      "regenerate with python -m experiments.comparison. The experiment's carbon intensity is synthetic.";
  }

  function renderLive(live) {
    const byHour = {};
    live.forEach((r) => {
      const h = byHour[r.hour] = byHour[r.hour] || {baseline: 0, optimized: 0};
      h.baseline += r.baseline_kw || 0; h.optimized += r.optimized_kw || 0;
    });
    const list = Object.keys(byHour).sort();
    Charts.render("chart-optimizer", [
      {x: list, y: list.map((h) => byHour[h].baseline), name: "normal schedule (predicted)", type: "scatter",
       mode: "lines+markers", line: {color: "#94a3b8", width: 2}},
      {x: list, y: list.map((h) => byHour[h].optimized), name: "optimizer's choice (predicted)", type: "scatter",
       mode: "lines+markers", line: {color: "#16a34a", width: 2}, fill: "tonexty", fillcolor: "rgba(22,163,74,.12)"},
    ], {yaxis: {title: "building kW (sum of zones)"}}, "No optimizer decisions yet. Press Start on the dashboard.");
    const base = list.reduce((s, h) => s + byHour[h].baseline, 0), opt = list.reduce((s, h) => s + byHour[h].optimized, 0);
    document.getElementById("live-headline").innerHTML = list.length ? [
      headline(`${base > opt ? "−" : "+"}${fmt.num(Math.abs(100 * (1 - opt / base)), 1)} %`, "predicted power", base >= opt),
      headline(`${fmt.num((base - opt) / list.length, 2)} kW`, "average predicted saving", base >= opt),
    ].join("") : "";
  }

  function renderMode() {
    const closed = window.DASHBOARD.controlMode === "closed_loop";
    document.getElementById("mode-badge").innerHTML = closed
      ? '<span class="chip" style="background:#dcfce7;color:#166534"><i class="bi bi-arrow-repeat"></i> closed loop</span>'
      : '<span class="chip" style="background:#e0f2fe;color:#075985"><i class="bi bi-eye"></i> advisory</span>';
    document.getElementById("mode-help").innerHTML = closed
      ? "<strong>Closed-loop mode:</strong> the simulator applies every approved recommendation, so the building really " +
        "runs on the optimizer's settings. The measured result is in the energy charts above."
      : "<strong>Advisory mode:</strong> the optimizer only <em>recommends</em>. The building keeps its normal schedule, so " +
        "these savings are predicted, not realised. To let the optimizer control the building, run " +
        "<code>scripts\\windows\\setup.bat -ClosedLoop</code> once (it trains matching security detectors), then start " +
        "with <code>scripts\\windows\\start.ps1 -Mode closed_loop</code>.";
  }

  async function loadOptimization() {
    const [{live, experiment}, {rows}] = await Promise.all([api(`/api/energy/optimization?hours=${hours()}`),
                                                            api("/api/agent/actions?source=optimizer&limit=15")]);
    renderExperiment(experiment);
    renderLive(live);
    document.getElementById("optimizer-table").innerHTML = rows.length ? rows.map((a) => {
      const p = a.parameters || {};
      const power = p.predicted_power_baseline_kw != null
        ? `${fmt.num(p.predicted_power_baseline_kw, 2)} → <strong>${fmt.num(p.predicted_power_optimized_kw, 2)} kW</strong>` : "—";
      return `<tr><td class="text-nowrap">${fmt.time(a.created_at)}</td><td>${escapeHtml(a.zone_id)}</td>
        <td>${escapeHtml(Glossary.action(a.action_type, p))}</td><td>${power}</td>
        <td><span class="status-${escapeHtml(a.status)}">${escapeHtml(STATUS_TEXT[a.status] || a.status)}</span></td></tr>`;
    }).join("") : '<tr><td colspan="5" class="text-muted">No recommendations yet</td></tr>';
  }

  const loadAll = () => Promise.allSettled([loadSummary(), loadHourly(), loadPredictions(), loadOptimization()]);
  document.addEventListener("DOMContentLoaded", () => {
    renderMode();
    document.getElementById("hours").addEventListener("change", loadAll);
    document.getElementById("pred-zone").addEventListener("change", () => loadPredictions().catch(() => {}));
    loadAll();
    setInterval(loadAll, 20000);           // aggregates change slowly; live readings are on the main page
  });
})();
