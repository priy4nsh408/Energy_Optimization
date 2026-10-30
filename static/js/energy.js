/* Energy analytics: hourly consumption, carbon, predictions vs. actual, baseline vs. optimized. */
"use strict";

(() => {
  const {api, escapeHtml, fmt} = App;
  const hours = () => document.getElementById("hours").value;

  function kpi(label, value, sub) {
    return `<div class="col-6 col-lg-3"><div class="card"><div class="card-body">
      <div class="small text-muted text-uppercase">${escapeHtml(label)}</div>
      <div class="fs-4 fw-bold">${escapeHtml(value)}</div><div class="small text-muted">${escapeHtml(sub)}</div></div></div></div>`;
  }

  async function loadSummary() {
    const s = await api(`/api/energy/summary?hours=${hours()}`);
    const total = s.energy_kwh || 0;
    const share = (v) => total ? `${fmt.num(100 * v / total, 0)} % of total` : "—";
    document.getElementById("energy-kpis").innerHTML = [
      kpi("Energy", s.energy_kwh != null ? `${fmt.num(total, 1)} kWh` : "—", `${fmt.time(s.since)} → ${fmt.time(s.until)}`),
      kpi("HVAC", `${fmt.num(s.hvac_kwh, 1)} kWh`, share(s.hvac_kwh || 0)),
      kpi("Lighting + appliances", `${fmt.num((s.lighting_kwh || 0) + (s.appliance_kwh || 0), 1)} kWh`,
          share((s.lighting_kwh || 0) + (s.appliance_kwh || 0))),
      kpi("Carbon", s.carbon_kg_co2e != null ? `${fmt.num(s.carbon_kg_co2e, 1)} kg CO₂e` : "—",
          s.readings_without_carbon ? `${s.readings_without_carbon} readings without intensity` : "all readings priced"),
    ].join("");
  }

  async function loadHourly() {
    const {rows} = await api(`/api/energy/hourly?hours=${hours()}`);
    const groups = Charts.groupBy(rows, "zone_id");
    const zones = Object.keys(groups).sort();
    Charts.render("chart-hourly", zones.map((z, i) => ({
      x: groups[z].map((r) => r.hour), y: groups[z].map((r) => r.energy_kwh), name: z, type: "bar",
      marker: {color: Charts.zoneColor(z, i)}})), {barmode: "stack", yaxis: {title: "kWh"}});
    Charts.render("chart-carbon", zones.map((z, i) => ({
      x: groups[z].map((r) => r.hour), y: groups[z].map((r) => r.carbon_kg_co2e), name: z, type: "scatter",
      mode: "lines", stackgroup: "carbon", line: {color: Charts.zoneColor(z, i)}})), {yaxis: {title: "kg CO₂e"}});
    const missing = rows.filter((r) => r.carbon_kg_co2e === null).length;
    document.getElementById("carbon-note").textContent = missing
      ? `${missing} zone-hours have no carbon intensity yet (start workers.external_data_publisher).` : "";
  }

  async function loadPredictions() {
    const zone = document.getElementById("pred-zone").value;
    const {rows, accuracy} = await api(`/api/energy/predictions?hours=${Math.min(hours(), 48)}&zone_id=${encodeURIComponent(zone)}`);
    Charts.render("chart-predictions", [
      {x: rows.map((r) => r.predicted_for), y: rows.map((r) => r.actual_power_kw), name: "measured", type: "scatter",
       mode: "lines", line: {color: "#212529"}},
      {x: rows.map((r) => r.predicted_for), y: rows.map((r) => r.predicted_power_kw), name: "predicted", type: "scatter",
       mode: "lines", line: {color: "#0d6efd", dash: "dot"}},
    ], {yaxis: {title: "kW"}}, "No predictions yet (start workers.prediction_service)");
    document.getElementById("accuracy-table").innerHTML = accuracy.length ? accuracy.map((a) =>
      `<tr><td>${escapeHtml(a.zone_id)}</td><td>${escapeHtml(a.n)}</td><td>${fmt.num(a.mae_kw, 3)}</td>
       <td>${fmt.num(a.rmse_kw, 3)}</td><td>${fmt.num(a.mean_actual_kw, 2)}</td></tr>`).join("")
      : '<tr><td colspan="5" class="text-muted">No predictions stored</td></tr>';
  }

  async function loadOptimization() {
    const {live, experiment} = await api(`/api/energy/optimization?hours=${hours()}`);
    const byHour = {};
    live.forEach((r) => {
      const h = byHour[r.hour] = byHour[r.hour] || {baseline: 0, optimized: 0};
      h.baseline += r.baseline_kw || 0; h.optimized += r.optimized_kw || 0;
    });
    const hoursList = Object.keys(byHour).sort();
    Charts.render("chart-optimizer", [
      {x: hoursList, y: hoursList.map((h) => byHour[h].baseline), name: "baseline (predicted)", type: "scatter", mode: "lines+markers", line: {color: "#6c757d"}},
      {x: hoursList, y: hoursList.map((h) => byHour[h].optimized), name: "optimized (predicted)", type: "scatter", mode: "lines+markers", line: {color: "#198754"}},
    ], {yaxis: {title: "kW (sum of zones)"}}, "No optimizer decisions yet (start workers.optimization_service)");

    const table = document.getElementById("comparison-table");
    if (!experiment) {
      table.innerHTML = '<tr><td colspan="7" class="text-muted">Run python -m experiments.comparison --json docs/results/optimization_comparison.json</td></tr>';
      Charts.render("chart-comparison", [], {}, "No experiment results file");
      return;
    }
    const rows = [], traces = {};
    Object.entries(experiment.cases).forEach(([caseName, results]) => {
      const baseline = results[0];
      results.forEach((r) => {
        const saving = baseline.energy_kwh ? 100 * (1 - r.energy_kwh / baseline.energy_kwh) : null;
        rows.push(`<tr><td>${escapeHtml(caseName)}</td><td>${escapeHtml(r.name)}</td><td>${fmt.num(r.energy_kwh, 0)}</td>
          <td>${fmt.pct(saving)}</td><td>${fmt.num(r.carbon_kg, 0)}</td><td>${fmt.pct(r.comfort_compliance_pct)}</td>
          <td>${fmt.num(r.peak_power_kw, 1)}</td></tr>`);
        const key = r.name.startsWith("baseline") ? "baseline" : r.name;
        (traces[key] = traces[key] || {x: [], y: []}).x.push(caseName);
        traces[key].y.push(r.energy_kwh);
      });
    });
    table.innerHTML = rows.join("");
    Charts.render("chart-comparison", Object.entries(traces).map(([name, t], i) => ({
      x: t.x, y: t.y, name, type: "bar", marker: {color: Charts.PALETTE[(i * 2) % Charts.PALETTE.length]}})),
      {barmode: "group", yaxis: {title: "kWh per week"}, hovermode: "closest"});
    document.getElementById("comparison-note").textContent =
      `Measured in closed-loop simulations with identical weather and occupancy (${experiment.source}). Carbon intensity is synthetic.`;
  }

  const loadAll = () => Promise.allSettled([loadSummary(), loadHourly(), loadPredictions(), loadOptimization()]);
  document.addEventListener("DOMContentLoaded", () => {
    document.getElementById("hours").addEventListener("change", loadAll);
    document.getElementById("pred-zone").addEventListener("change", () => loadPredictions().catch(() => {}));
    loadAll();
    setInterval(loadAll, 30000);           // aggregates change slowly; live readings are on the main page
  });
})();
