/* Floor plan of the simulated building, coloured as a heat map of one measure per room.
   Buttons switch the measure (temperature, comfort, CO2, humidity, people, lighting, power).
   The layout is a sample floor: room sizes roughly follow the simulated floor areas. */
"use strict";

const FloorPlan = (() => {
  const {escapeHtml, fmt} = App;
  const SVG_NS = "http://www.w3.org/2000/svg";
  const ZONES = Object.fromEntries((window.DASHBOARD.zones || []).map(([id, name, type, area, capacity]) =>
    [id, {name, type, area, capacity}]));

  // Room rectangles in a 860 x 540 drawing (building outline 50,50 -> 810,490).
  const ROOMS = {
    Z1: {x: 50, y: 50, w: 400, h: 300},
    Z2: {x: 450, y: 50, w: 360, h: 135},
    Z3: {x: 480, y: 225, w: 130, h: 150},
    Z5: {x: 610, y: 225, w: 200, h: 80},
    Z4: {x: 50, y: 375, w: 760, h: 115},
  };

  /* Each measure: how to read it from a reading, its colour scale (value -> colour stops), and wording. */
  const METRICS = {
    temperature: {
      label: "Temperature", icon: "thermometer-half", unit: "°C", digits: 1,
      value: (r) => num(r.temperature_c),
      stops: [[16, "#1e3a8a"], [18, "#2563eb"], [20, "#06b6d4"], [22, "#22c55e"], [24, "#eab308"], [26, "#f97316"], [28, "#dc2626"]],
      explain: "Room temperature as a heat map: blue is cold, green is around 22 °C (comfortable), orange and red are warm. " +
               "The small label under each value is the target temperature the heating/cooling is aiming for.",
    },
    comfort: {
      label: "Comfort", icon: "emoji-smile", unit: "°C off target", digits: 1,
      value: (r) => Number(r.occupancy_count) > 0 ? Math.abs(num(r.temperature_c) - num(r.hvac_setpoint_c)) : null,
      stops: [[0, "#16a34a"], [1, "#84cc16"], [2, "#f59e0b"], [3.5, "#dc2626"]],
      empty: "empty",
      explain: "How far each occupied room is from its target temperature. Green means people are comfortable; " +
               "red means they are not. Empty rooms are grey, because comfort only matters when someone is there.",
    },
    co2: {
      label: "CO₂ (air quality)", icon: "wind", unit: "ppm", digits: 0,
      value: (r) => num(r.co2_ppm),
      stops: [[400, "#22c55e"], [800, "#a3e635"], [1000, "#eab308"], [1200, "#f97316"], [1600, "#b91c1c"]],
      explain: "CO₂ shows how fresh the air is. Outdoor air is about 420 ppm; below 800 ppm is good, above 1200 ppm " +
               "feels stuffy. CO₂ rises when many people are in a room.",
    },
    humidity: {
      label: "Humidity", icon: "droplet", unit: "%", digits: 0,
      value: (r) => num(r.humidity_pct),
      stops: [[15, "#b45309"], [30, "#facc15"], [45, "#22c55e"], [60, "#38bdf8"], [75, "#1d4ed8"]],
      explain: "Relative humidity. 30–60 % (green) is comfortable; brown/yellow is too dry, blue is too humid.",
    },
    people: {
      label: "People", icon: "people", unit: "people", digits: 0,
      value: (r) => num(r.occupancy_count),
      scaled: (r, z) => z.capacity ? num(r.occupancy_count) / z.capacity * 100 : null,
      stops: [[0, "#f1f5f9"], [25, "#c4b5fd"], [50, "#8b5cf6"], [75, "#6d28d9"], [100, "#3b0764"]],
      legendUnit: "% of room capacity",
      explain: "Each dot is one person. The colour shows how full the room is compared with its capacity " +
               "(open office 30, private offices 8, meeting room 12, cafeteria and lobby 60, server room 2).",
    },
    lighting: {
      label: "Lighting", icon: "lightbulb", unit: "%", digits: 0,
      value: (r) => num(r.lighting_level_pct),
      stops: [[0, "#1e293b"], [20, "#475569"], [50, "#ca8a04"], [80, "#facc15"], [100, "#fef08a"]],
      explain: "How bright the lights are set (0 % off, 100 % full). Lights follow people and the schedule, " +
               "and the optimizer dims them when daylight is enough.",
    },
    power: {
      label: "Power", icon: "lightning-charge", unit: "kW", digits: 2,
      value: (r) => num(r.total_power_kw),
      scaled: (r, z) => z.area ? num(r.total_power_kw) * 1000 / z.area : null,
      stops: [[0, "#f8fafc"], [20, "#fde68a"], [50, "#fb923c"], [100, "#dc2626"], [150, "#7f1d1d"]],
      legendUnit: "W per m² of floor",
      explain: "Electricity use (heating/cooling + lights + devices). The colour is power per square metre, so " +
               "small rooms can be compared with big ones: the server room is always the most intense.",
    },
  };

  let metric = "temperature";
  let readings = [];
  let alerted = new Set();
  let selected = null;

  function num(v) { return v === null || v === undefined || Number.isNaN(Number(v)) ? null : Number(v); }

  function hexToRgb(hex) { const n = parseInt(hex.slice(1), 16); return [n >> 16, (n >> 8) & 255, n & 255]; }

  function colorAt(stops, v) {
    if (v == null) return "#e2e8f0";
    if (v <= stops[0][0]) return stops[0][1];
    for (let i = 1; i < stops.length; i++) {
      const [v1, c1] = stops[i];
      if (v <= v1) {
        const [v0, c0] = stops[i - 1];
        const t = (v - v0) / (v1 - v0), a = hexToRgb(c0), b = hexToRgb(c1);
        return `rgb(${a.map((x, k) => Math.round(x + (b[k] - x) * t)).join(",")})`;
      }
    }
    return stops[stops.length - 1][1];
  }

  function inkFor(color) {
    const rgb = color.startsWith("#") ? hexToRgb(color) : color.match(/\d+/g).map(Number);
    const lum = (0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]) / 255;
    return lum > 0.6 ? "#0f172a" : "#ffffff";
  }

  /* Deterministic pseudo-random positions so people dots don't jump on every refresh. */
  function seeded(seed) {
    let s = seed;
    return () => { s = (s * 1664525 + 1013904223) % 4294967296; return s / 4294967296; };
  }

  function el(tag, attrs = {}, text) {
    const node = document.createElementNS(SVG_NS, tag);
    Object.entries(attrs).forEach(([k, v]) => node.setAttribute(k, v));
    if (text !== undefined) node.textContent = text;
    return node;
  }

  /* Static drawing: corridors, furniture, windows, doors. Drawn once. */
  function drawStatic(svg) {
    const defs = el("defs");
    defs.innerHTML = `
      <pattern id="fp-hatch" width="8" height="8" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
        <line x1="0" y1="0" x2="0" y2="8" stroke="#cbd5e1" stroke-width="2"/></pattern>
      <radialGradient id="fp-shade"><stop offset="0" stop-color="#fff" stop-opacity=".22"/>
        <stop offset="1" stop-color="#000" stop-opacity=".10"/></radialGradient>
      <radialGradient id="fp-glow"><stop offset="0" stop-color="#fef9c3" stop-opacity=".9"/>
        <stop offset="1" stop-color="#fef9c3" stop-opacity="0"/></radialGradient>`;
    svg.appendChild(defs);
    // Outer slab and corridors
    svg.appendChild(el("rect", {x: 44, y: 44, width: 772, height: 452, rx: 6, class: "fp-slab"}));
    svg.appendChild(el("path", {class: "fp-corridor",
      d: "M450 185 H810 V225 H480 V375 H50 V350 H450 Z"}));
    svg.appendChild(el("text", {x: 640, y: 210, class: "fp-tag"}, "CORRIDOR"));
    svg.appendChild(el("text", {x: 250, y: 367, class: "fp-tag"}, "CORRIDOR"));
    // Utility core (not a simulated zone)
    svg.appendChild(el("rect", {x: 610, y: 305, width: 200, height: 70, fill: "url(#fp-hatch)", class: "fp-core"}));
    svg.appendChild(el("text", {x: 710, y: 345, class: "fp-tag"}, "STAIRS · WC · LIFT"));

    const rooms = el("g", {id: "fp-rooms"});
    svg.appendChild(rooms);
    Object.entries(ROOMS).forEach(([zone, r]) => {
      const g = el("g", {class: "fp-room", "data-zone": zone, tabindex: 0, role: "button"});
      g.appendChild(el("rect", {x: r.x, y: r.y, width: r.w, height: r.h, class: "fp-fill"}));
      g.appendChild(el("rect", {x: r.x, y: r.y, width: r.w, height: r.h, fill: "url(#fp-shade)", class: "fp-shade"}));
      g.appendChild(el("g", {class: "fp-glow"}));
      g.appendChild(furniture(zone, r));
      g.appendChild(el("g", {class: "fp-dots"}));
      g.appendChild(el("rect", {x: r.x + 2, y: r.y + 2, width: r.w - 4, height: r.h - 4, class: "fp-alert-ring"}));
      g.appendChild(el("rect", {x: r.x, y: r.y, width: r.w, height: r.h, class: "fp-wall"}));
      g.appendChild(el("g", {class: "fp-labels"}));
      rooms.appendChild(g);
    });

    // Interior walls between the private offices, doors (white gaps), windows (blue lines)
    const top = el("g", {class: "fp-top"});
    [570, 690].forEach((x) => top.appendChild(el("line", {x1: x, y1: 50, x2: x, y2: 185, class: "fp-inner-wall"})));
    [[300, 350, 340, 350], [500, 185, 530, 185], [620, 185, 650, 185], [740, 185, 770, 185],
     [480, 280, 480, 310], [700, 225, 730, 225], [140, 375, 190, 375]]
      .forEach(([x1, y1, x2, y2]) => top.appendChild(el("line", {x1, y1, x2, y2, class: "fp-door"})));
    [[70, 50, 430, 50], [50, 70, 50, 330], [470, 50, 790, 50], [810, 70, 810, 165], [50, 395, 50, 470],
     [70, 490, 560, 490], [700, 490, 790, 490]]
      .forEach(([x1, y1, x2, y2]) => top.appendChild(el("line", {x1, y1, x2, y2, class: "fp-window"})));
    top.appendChild(el("line", {x1: 590, y1: 490, x2: 680, y2: 490, class: "fp-entrance"}));
    top.appendChild(el("text", {x: 635, y: 512, class: "fp-tag strong"}, "▲ MAIN ENTRANCE"));
    // North arrow
    top.appendChild(el("path", {d: "M835 70 l8 22 h-16 z", class: "fp-north"}));
    top.appendChild(el("text", {x: 835, y: 64, class: "fp-tag strong"}, "N"));
    svg.appendChild(top);
  }

  function furniture(zone, r) {
    const g = el("g", {class: "fp-furniture"});
    const rect = (x, y, w, h, rx = 2) => g.appendChild(el("rect", {x, y, width: w, height: h, rx}));
    const circ = (cx, cy, rr) => g.appendChild(el("circle", {cx, cy, r: rr}));
    if (zone === "Z1") {            // desk clusters
      for (let c = 0; c < 4; c++) for (let row = 0; row < 3; row++) rect(r.x + 30 + c * 92, r.y + 40 + row * 85, 64, 34);
    } else if (zone === "Z2") {     // one desk + chair per office
      [0, 1, 2].forEach((i) => { rect(r.x + 25 + i * 120, r.y + 25, 60, 28); circ(r.x + 55 + i * 120, r.y + 66, 7); });
    } else if (zone === "Z3") {     // meeting table with chairs
      g.appendChild(el("ellipse", {cx: r.x + r.w / 2, cy: r.y + r.h / 2 + 18, rx: 34, ry: 46}));
      for (let i = 0; i < 6; i++) {
        circ(r.x + r.w / 2 - 46, r.y + r.h / 2 - 12 + i % 3 * 30, 6);
        circ(r.x + r.w / 2 + 46, r.y + r.h / 2 - 12 + i % 3 * 30, 6);
      }
    } else if (zone === "Z5") {     // server racks
      for (let i = 0; i < 5; i++) rect(r.x + 18 + i * 36, r.y + 40, 24, 32, 1);
    } else if (zone === "Z4") {     // cafeteria tables + reception counter
      for (let i = 0; i < 6; i++) for (let j = 0; j < 2; j++) circ(r.x + 40 + i * 70, r.y + 40 + j * 45, 14);
      g.appendChild(el("path", {d: `M${r.x + 600} ${r.y + 30} h110 v18 h-92 v30 h-18 z`}));
    }
    return g;
  }

  function labelsFor(zone, r, reading, def, color) {
    const ink = inkFor(color);
    const z = ZONES[zone] || {};
    const cx = r.x + r.w / 2;
    const compact = r.h < 100, narrow = r.w < 200;
    const cy = r.y + (zone === "Z4" ? r.h / 2 + 10 : compact ? r.h / 2 + 4 : narrow ? r.h / 2 + 6 : r.h / 2);
    const g = el("g", {fill: ink, style: `--halo:${color}`});
    const value = reading ? def.value(reading) : null;
    const text = value == null ? (reading && def.empty ? def.empty : "—")
      : `${fmt.num(value, def.digits)}${def.unit.startsWith("°") || def.unit === "%" ? "" : " "}${def.unit.split(" ")[0]}`;
    g.appendChild(el("text", {x: r.x + 10, y: r.y + 20, class: "fp-name"},
                      narrow ? `${zone} · ${(z.name || "").split(" ")[0]}` : `${zone} · ${z.name || ""}`));
    g.appendChild(el("text", {x: cx, y: cy, class: compact || narrow ? "fp-value small" : "fp-value"}, text));
    let sub = "";
    if (reading) {
      if (metric === "temperature") sub = `target ${fmt.num(reading.hvac_setpoint_c)} °C`;
      else if (metric === "comfort") sub = value == null ? "no one here" : narrow ? `${fmt.num(reading.temperature_c)} → ${fmt.num(reading.hvac_setpoint_c)} °C`
        : `${fmt.num(reading.temperature_c)} °C now, target ${fmt.num(reading.hvac_setpoint_c)} °C`;
      else if (metric === "people") sub = `of ${z.capacity} places`;
      else if (metric === "power") sub = `${fmt.num(def.scaled(reading, z), 0)} W/m²`;
      else if (metric === "co2") sub = value < 800 ? "fresh" : value < 1200 ? "getting stuffy" : "stuffy";
      else if (metric === "humidity") sub = value < 30 ? "dry" : value <= 60 ? "comfortable" : "humid";
      else if (metric === "lighting") sub = value < 1 ? "lights off" : `${fmt.num(reading.lighting_lux, 0)} lux`;
    }
    if (sub) g.appendChild(el("text", {x: cx, y: cy + (compact ? 18 : 22), class: "fp-sub"}, sub));
    // HVAC badge, alert and quarantined-sensor markers (top right of the room)
    if (reading) {
      const status = reading.hvac_status || "off";
      const symbol = status.includes("heat") ? "▲ heating" : status.includes("cool") ? "▼ cooling" : status === "off" ? "○ HVAC off" : `● ${status}`;
      // Narrow rooms put the HVAC state on its own line under the room name.
      g.appendChild(narrow ? el("text", {x: r.x + 10, y: r.y + 36, class: "fp-hvac start"}, symbol)
                           : el("text", {x: r.x + r.w - 10, y: r.y + 20, class: "fp-hvac"}, symbol));
      const notes = [];
      if (alerted.has(zone)) notes.push("⚠ security alert");
      if ((reading.isolated_sensors || []).length) {
        notes.push(`⊘ ${reading.isolated_sensors.map(Glossary.sensor).join(", ")} ${narrow ? "untrusted" : "sensor not trusted"}`);
      }
      notes.reverse().forEach((note, i) =>
        g.appendChild(el("text", {x: r.x + 10, y: r.y + r.h - 10 - i * 15, class: "fp-note"}, note)));
    }
    return g;
  }

  function scaledValue(reading, def, zone) {
    if (!reading) return null;
    return def.scaled ? def.scaled(reading, ZONES[zone] || {}) : def.value(reading);
  }

  function render() {
    const svg = document.getElementById("fp-svg");
    if (!svg) return;
    const def = METRICS[metric];
    const byZone = Object.fromEntries(readings.map((r) => [r.zone_id, r]));
    svg.querySelectorAll(".fp-room").forEach((g) => {
      const zone = g.dataset.zone, r = ROOMS[zone], reading = byZone[zone];
      const v = scaledValue(reading, def, zone);
      const color = v == null ? (reading && def.empty ? "#e2e8f0" : "#f1f5f9") : colorAt(def.stops, v);
      g.querySelector(".fp-fill").setAttribute("fill", color);
      g.classList.toggle("alerted", alerted.has(zone));
      g.classList.toggle("selected", selected === zone);
      g.classList.toggle("dark", metric === "lighting");
      // Lighting: warm glow pools; people: one dot per person
      const glow = g.querySelector(".fp-glow"); glow.innerHTML = "";
      if (metric === "lighting" && reading && num(reading.lighting_level_pct) > 0) {
        const level = num(reading.lighting_level_pct) / 100;
        const cols = Math.max(1, Math.round(r.w / 110)), rows = Math.max(1, Math.round(r.h / 90));
        for (let i = 0; i < cols; i++) for (let j = 0; j < rows; j++) {
          glow.appendChild(el("circle", {cx: r.x + (i + 0.5) * r.w / cols, cy: r.y + (j + 0.5) * r.h / rows,
                                         r: 26 + 30 * level, fill: "url(#fp-glow)", opacity: 0.25 + 0.75 * level}));
        }
      }
      const dots = g.querySelector(".fp-dots"); dots.innerHTML = "";
      if (metric === "people" && reading) {
        const rand = seeded(zone.charCodeAt(1) * 7919);
        const count = Math.min(80, Math.round(num(reading.occupancy_count) || 0));
        // Keep dots out of the band in the middle that holds the value label.
        const label = {x0: r.x + r.w / 2 - 85, x1: r.x + r.w / 2 + 85, y0: r.y + r.h / 2 - 32, y1: r.y + r.h / 2 + 38};
        for (let i = 0, tries = 0; i < count && tries < 2000; tries++) {
          const x = r.x + 14 + rand() * (r.w - 28), y = r.y + 42 + rand() * (r.h - 58);
          if (x > label.x0 && x < label.x1 && y > label.y0 && y < label.y1) continue;
          dots.appendChild(el("circle", {cx: x, cy: y, r: 4.2}));
          i++;
        }
      }
      const old = g.querySelector(".fp-labels");
      const labels = labelsFor(zone, r, reading, def, color);
      labels.setAttribute("class", "fp-labels");
      old.replaceWith(labels);
    });
    renderLegend(def);
    renderRanking(def, byZone);
    document.getElementById("fp-explain").textContent = def.explain;
    const latest = readings.map((r) => r.event_time).sort().pop();
    document.getElementById("fp-updated").textContent = latest ? `simulated time ${fmt.simTime(latest)}` : "waiting for data";
    // Explain an empty building: the simulated clock is often at night or on a weekend.
    const people = readings.reduce((sum, r) => sum + (Number(r.occupancy_count) || 0), 0);
    const note = document.getElementById("fp-empty");
    if (note) {
      note.hidden = !(latest && people === 0);
      if (latest && people === 0) {
        const hour = Number(String(latest).slice(11, 13));
        const weekend = fmt.simTime(latest).includes("weekend");
        note.textContent = `Nobody is in the building at this simulated time (${weekend ? "weekend" :
          hour < 7 || hour >= 19 ? "night" : "outside office hours"}). The simulation runs much faster than ` +
          "real time (a simulated day passes in a few minutes), so its clock rarely matches yours; people arrive " +
          "from 07:00 on weekdays.";
      }
    }
  }

  function renderLegend(def) {
    const lo = def.stops[0][0], hi = def.stops[def.stops.length - 1][0];
    const gradient = def.stops.map(([v, c]) => `${c} ${((v - lo) / (hi - lo) * 100).toFixed(1)}%`).join(", ");
    const ticks = def.stops.map(([v]) =>
      `<span style="left:${((v - lo) / (hi - lo) * 100).toFixed(1)}%">${v}</span>`).join("");
    document.getElementById("fp-legend").innerHTML =
      `<div class="fp-legend-title">${escapeHtml(def.legendUnit || def.unit)}</div>
       <div class="fp-legend-bar" style="background:linear-gradient(90deg, ${gradient})"></div>
       <div class="fp-legend-ticks">${ticks}</div>`;
  }

  function renderRanking(def, byZone) {
    const rows = Object.keys(ROOMS).map((zone) => {
      const reading = byZone[zone];
      return {zone, value: reading ? def.value(reading) : null, scaled: scaledValue(reading, def, zone)};
    });
    const max = Math.max(1e-9, ...rows.map((r) => r.scaled ?? 0), def.stops[def.stops.length - 1][0] * 0.5);
    rows.sort((a, b) => (b.scaled ?? -Infinity) - (a.scaled ?? -Infinity));
    document.getElementById("fp-ranking").innerHTML = rows.map((r) => {
      const color = r.scaled == null ? "#e2e8f0" : colorAt(def.stops, r.scaled);
      const width = r.scaled == null ? 0 : Math.max(3, Math.min(100, 100 * r.scaled / max));
      let shown = r.value == null ? (def.empty && byZone[r.zone] ? def.empty : "—") : `${fmt.num(r.value, def.digits)} ${def.unit.split(" ")[0]}`;
      if (def.scaled && r.scaled != null) shown += metric === "people" ? ` · ${fmt.num(r.scaled, 0)} %` : ` · ${fmt.num(r.scaled, 0)} W/m²`;
      return `<div class="fp-rank ${selected === r.zone ? "selected" : ""}" data-zone="${escapeHtml(r.zone)}">
        <div class="d-flex justify-content-between"><span><strong>${escapeHtml(r.zone)}</strong> ${escapeHtml(ZONES[r.zone]?.name || "")}</span>
          <span class="fp-rank-val">${escapeHtml(shown)}</span></div>
        <div class="fp-rank-bar"><span style="width:${width.toFixed(0)}%;background:${color}"></span></div></div>`;
    }).join("");
    document.querySelectorAll("#fp-ranking .fp-rank").forEach((node) =>
      node.addEventListener("click", () => select(node.dataset.zone)));
  }

  function tooltipHtml(zone) {
    const r = readings.find((x) => x.zone_id === zone);
    const z = ZONES[zone] || {};
    if (!r) return `<strong>${escapeHtml(zone)} · ${escapeHtml(z.name || "")}</strong><div>No reading yet</div>`;
    const line = (icon, label, value) => `<div><i class="bi bi-${icon}"></i> ${label}: <strong>${value}</strong></div>`;
    return `<strong>${escapeHtml(zone)} · ${escapeHtml(z.name || "")}</strong>
      <div class="text-muted small mb-1">${fmt.num(z.area, 0)} m² · up to ${escapeHtml(z.capacity)} people</div>
      ${line("thermometer-half", "Temperature", `${fmt.num(r.temperature_c)} °C (target ${fmt.num(r.hvac_setpoint_c)} °C)`)}
      ${line("fan", "Heating/cooling", escapeHtml(r.hvac_status || "off"))}
      ${line("wind", "CO₂", `${fmt.num(r.co2_ppm, 0)} ppm`)}
      ${line("droplet", "Humidity", `${fmt.num(r.humidity_pct, 0)} %`)}
      ${line("people", "People", escapeHtml(r.occupancy_count))}
      ${line("lightbulb", "Lights", `${fmt.num(r.lighting_level_pct, 0)} % · ${fmt.num(r.lighting_lux, 0)} lux`)}
      ${line("lightning-charge", "Power", `${fmt.num(r.total_power_kw, 2)} kW`)}
      ${alerted.has(zone) ? '<div class="text-danger mt-1"><i class="bi bi-shield-exclamation"></i> recent security alert</div>' : ""}
      ${(r.isolated_sensors || []).length ? `<div class="text-danger"><i class="bi bi-slash-circle"></i> not trusted: ${escapeHtml(r.isolated_sensors.map(Glossary.sensor).join(", "))}</div>` : ""}`;
  }

  function showTooltip(zone, event) {
    const tip = document.getElementById("fp-tooltip");
    const wrap = document.getElementById("fp-wrap").getBoundingClientRect();
    tip.innerHTML = tooltipHtml(zone);
    tip.hidden = false;
    const x = Math.min(event.clientX - wrap.left + 14, wrap.width - tip.offsetWidth - 6);
    const y = Math.min(event.clientY - wrap.top + 14, wrap.height - tip.offsetHeight - 6);
    tip.style.left = `${Math.max(6, x)}px`;
    tip.style.top = `${Math.max(6, y)}px`;
  }

  function select(zone) {
    selected = selected === zone ? null : zone;
    render();
  }

  function setMetric(name) {
    metric = name;
    document.querySelectorAll("#fp-metrics button").forEach((b) => {
      const on = b.dataset.metric === name;
      b.classList.toggle("active", on);
      b.setAttribute("aria-pressed", on);
    });
    try { localStorage.setItem("fp-metric", name); } catch (e) { /* storage unavailable */ }
    render();
  }

  function init() {
    const svg = document.getElementById("fp-svg");
    if (!svg) return;
    drawStatic(svg);
    document.getElementById("fp-metrics").innerHTML = Object.entries(METRICS).map(([key, m]) =>
      `<button type="button" class="btn btn-sm btn-outline-primary" data-metric="${key}" aria-pressed="false">
         <i class="bi bi-${m.icon}"></i> ${escapeHtml(m.label)}</button>`).join("");
    document.querySelectorAll("#fp-metrics button").forEach((b) => b.addEventListener("click", () => setMetric(b.dataset.metric)));
    svg.querySelectorAll(".fp-room").forEach((g) => {
      g.addEventListener("mousemove", (e) => showTooltip(g.dataset.zone, e));
      g.addEventListener("mouseleave", () => { document.getElementById("fp-tooltip").hidden = true; });
      g.addEventListener("click", (e) => { select(g.dataset.zone); showTooltip(g.dataset.zone, e); });
      g.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); select(g.dataset.zone); } });
    });
    let saved = null;
    try { saved = localStorage.getItem("fp-metric"); } catch (e) { /* storage unavailable */ }
    setMetric(METRICS[saved] ? saved : "temperature");
  }

  /** Called by the dashboard with the latest reading of every zone and the zones with recent alerts. */
  function update(latestReadings, alertedZones) {
    readings = latestReadings || [];
    alerted = new Set(alertedZones || []);
    render();
  }

  /** Called when new security alerts arrive, so alerted rooms get their red outline straight away. */
  function setAlerted(alertedZones) {
    alerted = new Set(alertedZones || []);
    render();
  }

  document.addEventListener("DOMContentLoaded", init);
  return {update, alerted: setAlerted, METRICS};
})();
