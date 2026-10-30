/* Plotly.js helpers shared by the pages. */
"use strict";

const Charts = (() => {
  const ZONE_COLORS = {Z1: "#0d6efd", Z2: "#20c997", Z3: "#fd7e14", Z4: "#6f42c1", Z5: "#dc3545"};
  const PALETTE = ["#0d6efd", "#20c997", "#fd7e14", "#6f42c1", "#dc3545", "#6c757d", "#198754", "#d63384", "#0dcaf0"];
  const CONFIG = {responsive: true, displaylogo: false,
                  modeBarButtonsToRemove: ["lasso2d", "select2d", "autoScale2d"]};

  function base(layout = {}) {
    return {margin: {l: 50, r: 15, t: 10, b: 40}, legend: {orientation: "h", y: -0.18},
            hovermode: "x unified", paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)",
            font: {size: 11}, ...layout};
  }

  function render(id, traces, layout = {}, emptyText = "No data yet") {
    const el = document.getElementById(id);
    if (!el || typeof Plotly === "undefined") return;
    const empty = !traces.length || traces.every((t) => !(t.x || t.labels || []).length);
    const finalLayout = base(layout);
    if (empty) {
      finalLayout.annotations = [{text: emptyText, showarrow: false, xref: "paper", yref: "paper", x: 0.5, y: 0.5,
                                  font: {color: "#6c757d", size: 13}}];
      finalLayout.xaxis = {...(finalLayout.xaxis || {}), visible: false};
      finalLayout.yaxis = {...(finalLayout.yaxis || {}), visible: false};
    }
    Plotly.react(el, empty ? [] : traces, finalLayout, CONFIG);
  }

  function zoneColor(zone, index = 0) {
    return ZONE_COLORS[zone] || PALETTE[index % PALETTE.length];
  }

  function groupBy(rows, key) {
    const groups = {};
    rows.forEach((r) => { (groups[r[key]] = groups[r[key]] || []).push(r); });
    return groups;
  }

  return {render, zoneColor, groupBy, PALETTE};
})();
