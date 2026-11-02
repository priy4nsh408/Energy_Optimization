/* Data pipeline page: live diagram of how data flows from the simulator through Kafka, the services and
   PostgreSQL to the dashboard (and back to the building). Numbers come from GET /api/pipeline. */
"use strict";

(() => {
  const {api, escapeHtml, fmt} = App;
  const W = 1200, H = 760, REFRESH_MS = 3000, SLOW_WINDOW_MS = 20 * 60 * 1000;
  const TOPICS = {
    building_sensor_data: "sensor readings", weather_data: "weather", carbon_intensity: "carbon intensity",
    energy_predictions: "ML predictions", optimization_commands: "commands", security_alerts: "security alerts",
    agent_actions: "agent actions", operator_decisions: "your decisions", simulation_control: "start/stop/attack",
  };
  const NODES = [
    {id: "sim", x: 110, y: 230, w: 184, h: 104, icon: "building", color: "#2563eb", title: "Building simulator",
     service: "workers.simulation_manager", tech: "Python building physics model (5 zones)",
     reads: "commands that passed the safety check (and, in closed-loop mode, the optimizer's)",
     writes: "Kafka topic building_sensor_data: one reading per zone every simulated 5 minutes",
     text: "Simulates the office building: temperature, humidity, CO₂, people, lights and power in each zone. Simulated attacks manipulate the readings it publishes."},
    {id: "ext", x: 110, y: 480, w: 184, h: 104, icon: "cloud-sun", color: "#0ea5e9", title: "Weather & carbon APIs",
     service: "workers.external_data_publisher", tech: "Open-Meteo and Electricity Maps (or synthetic data)",
     reads: "public web APIs, every 15 minutes", writes: "Kafka topics weather_data and carbon_intensity",
     text: "Brings in real outdoor weather and the electricity grid's carbon intensity, used to compute CO₂ emissions."},
    {id: "kafka", x: 340, y: 385, w: 172, h: 630, icon: "arrow-left-right", color: "#7c3aed", title: "Kafka message bus", big: true,
     service: "Apache Kafka (Docker container)", tech: "Durable, partitioned topics; each service has its own consumer group",
     reads: "messages from every producer", writes: "a copy of each message to every subscribed service",
     text: "The backbone. Services never call each other directly: they publish messages to topics and subscribe to the topics they need, so one slow or restarting service does not stop the others."},
    {id: "persist", x: 590, y: 90, w: 200, h: 84, icon: "hdd-stack", color: "#64748b", title: "Sensor storage",
     service: "workers.sensor_persistence", tech: "Validates each reading against the data contract",
     reads: "building_sensor_data", writes: "PostgreSQL table sensor_readings (invalid messages to invalid_events)",
     text: "Checks every reading (types, ranges, physics) and stores it. Duplicates are ignored, so nothing is stored twice."},
    {id: "energy", x: 590, y: 190, w: 200, h: 84, icon: "calculator", color: "#16a34a", title: "Energy & CO₂ calculator",
     service: "workers.energy_processor", tech: "Feature engineering + carbon accounting",
     reads: "building_sensor_data, carbon_intensity", writes: "PostgreSQL table energy_records",
     text: "Turns power into energy (kWh), multiplies by the grid's carbon intensity to get CO₂, and adds features such as rolling averages."},
    {id: "predict", x: 590, y: 290, w: 200, h: 84, icon: "cpu", color: "#2563eb", title: "ML prediction (XGBoost)",
     service: "workers.prediction_service", tech: "XGBoost model trained on 28 days of history",
     reads: "building_sensor_data", writes: "Kafka energy_predictions and PostgreSQL energy_predictions",
     text: "Predicts each zone's power 5 minutes ahead. The optimizer uses these predictions to compare settings."},
    {id: "optimizer", x: 590, y: 390, w: 200, h: 84, icon: "sliders", color: "#0d9488", title: "Energy optimizer",
     service: "workers.optimization_service", tech: "Deterministic optimizer with comfort and safety constraints",
     reads: "sensor readings, predictions, carbon intensity, agent actions", writes: "Kafka optimization_commands (stored in PostgreSQL)",
     text: "Every 15 simulated minutes chooses a target temperature and light level per zone that keep people comfortable while using less energy. It pauses zones with a quarantined sensor."},
    {id: "security", x: 590, y: 490, w: 200, h: 84, icon: "shield-lock", color: "#dc2626", title: "Attack detectors",
     service: "workers.security_service", tech: "Physics rules, Isolation Forest, KS + PSI drift tests",
     reads: "building_sensor_data", writes: "Kafka security_alerts (stored in PostgreSQL)",
     text: "Checks every reading for signs of a cyberattack and every 6-hour window for slow drift. An alert is only a suspicion."},
    {id: "agent", x: 590, y: 590, w: 200, h: 84, icon: "robot", color: "#9333ea", title: "AI agent",
     service: "workers.agent_service", tech: "LangGraph agent (Claude or the offline playbook) with tools",
     reads: "security_alerts and the database", writes: "agent decisions and proposed actions",
     text: "Investigates each serious alert like an analyst: re-checks the data, diagnoses the cause and proposes the smallest safe response."},
    {id: "pg", x: 850, y: 290, w: 172, h: 470, icon: "database", color: "#0369a1", title: "PostgreSQL database", big: true,
     service: "PostgreSQL 16 (Docker container)", tech: "Relational tables; every write is idempotent",
     reads: "all processing services", writes: "everything the dashboard shows",
     text: "The system of record: readings, energy and CO₂, predictions, commands, alerts, agent decisions and the simulation state."},
    {id: "gate", x: 850, y: 655, w: 200, h: 96, icon: "person-check", color: "#d97706", title: "Safety check + your approval",
     service: "agent.safety + agent.approval", tech: "Fixed rules; then a person approves (manual mode)",
     reads: "the agent's proposed actions and your decisions from this dashboard", writes: "approved commands back to the building",
     text: "Fixed rules block anything unsafe. In manual mode the remaining actions wait for you, with a plain-language explanation; nothing runs without your OK."},
    {id: "dash", x: 1090, y: 290, w: 172, h: 120, icon: "display", color: "#1e3a5f", title: "Dashboard (this site)",
     service: "Flask + Socket.IO (app.py)", tech: "Reads PostgreSQL; pushes changes to the browser every 2 s",
     reads: "PostgreSQL only (it never consumes Kafka)", writes: "start/stop/attack commands and your approvals to Kafka",
     text: "What you are looking at. It reads the database, pushes new data to your browser, and sends your commands back through Kafka."},
  ];
  const BY_ID = Object.fromEntries(NODES.map((n) => [n.id, n]));
  const STAGES = [["1. Produce", 110], ["2. Carry", 340], ["3. Process", 590], ["4. Store & decide", 850], ["5. Show", 1090]];
  let data = null, selected = null;

  const stage = (name) => ((data || {}).stages || {})[name] || {};
  const rate = (name) => Number(stage(name).last_minute || 0);
  const ago = (iso) => iso ? Date.now() - Date.parse(iso) : Infinity;

  function flow(names) {
    const list = [].concat(names);
    const r = list.reduce((s, n) => s + rate(n), 0);
    if (r > 0) return {state: "flowing", rate: r};
    return {state: list.some((n) => ago(stage(n).latest) < SLOW_WINDOW_MS) ? "trickle" : "idle", rate: 0};
  }

  const right = (n) => [n.x + n.w / 2, n.y], left = (n) => [n.x - n.w / 2, n.y];
  const curve = ([x1, y1], [x2, y2]) => `M ${x1} ${y1} C ${(x1 + x2) / 2} ${y1}, ${(x1 + x2) / 2} ${y2}, ${x2} ${y2}`;
  const at = (node, y) => Math.max(node.y - node.h / 2 + 14, Math.min(node.y + node.h / 2 - 14, y));

  // Each edge: id, SVG path, label, what it carries (stage names), extra class.
  function edges() {
    const k = BY_ID.kafka, pg = BY_ID.pg, svc = ["persist", "energy", "predict", "optimizer", "security", "agent"];
    const list = [
      {id: "sim-k", d: curve(right(BY_ID.sim), [k.x - k.w / 2, BY_ID.sim.y]), label: "readings", carries: "sensor_readings"},
      {id: "ext-k", d: curve(right(BY_ID.ext), [k.x - k.w / 2, BY_ID.ext.y]), label: "weather, carbon",
       carries: ["weather_records", "carbon_records"]},
      {id: "k-persist", carries: "sensor_readings"}, {id: "k-energy", carries: "energy_records"},
      {id: "k-predict", carries: "energy_predictions"}, {id: "k-optimizer", carries: "optimizer_actions"},
      {id: "k-security", carries: "sensor_readings"}, {id: "k-agent", carries: "security_alerts", label: "alerts"},
      {id: "agent-gate", d: curve(right(BY_ID.agent), left(BY_ID.gate)), carries: "agent_actions", label: "proposed actions"},
      {id: "pg-dash", d: curve(right(pg), left(BY_ID.dash)), carries: null, label: "live data"},
      {id: "dash-gate", d: `M ${BY_ID.dash.x} ${BY_ID.dash.y + BY_ID.dash.h / 2} C ${BY_ID.dash.x} 600, 1000 ${BY_ID.gate.y}, ${BY_ID.gate.x + BY_ID.gate.w / 2} ${BY_ID.gate.y}`,
       carries: "operator", label: "your decision", cls: "operator"},
      {id: "gate-sim", d: `M ${BY_ID.gate.x} ${BY_ID.gate.y + BY_ID.gate.h / 2} V ${H - 10} H 14 V ${BY_ID.sim.y} H ${BY_ID.sim.x - BY_ID.sim.w / 2}`,
       carries: "agent_actions", label: "approved actions go back to the building", cls: "feedback"},
    ];
    svc.forEach((id) => {
      const e = list.find((x) => x.id === `k-${id}`);
      e.d = curve([k.x + k.w / 2, BY_ID[id].y], left(BY_ID[id]));
      const out = {persist: "sensor_readings", energy: "energy_records", predict: "energy_predictions",
                   optimizer: "optimizer_actions", security: "security_alerts", agent: "agent_decisions"}[id];
      list.push({id: `${id}-pg`, d: curve(right(BY_ID[id]), [pg.x - pg.w / 2, at(pg, BY_ID[id].y)]), carries: out});
    });
    return list;
  }

  function edgeState(e) {
    if (e.carries === null) return {state: "flowing", rate: 1};                        // the dashboard polls continuously
    if (e.carries === "operator") {
      const a = (data || {}).approvals || {};
      return {state: Number(a.waiting || 0) > 0 ? "trickle" : "idle", rate: 0};
    }
    return flow(e.carries);
  }

  function drawEdges() {
    const svg = document.getElementById("pipe-edges");
    svg.innerHTML = edges().map((e) => {
      const s = edgeState(e);
      const width = s.state === "flowing" ? 2 + Math.min(4, Math.log10(s.rate + 1) * 1.6) : 2;
      return `<path class="edge ${s.state} ${e.cls || ""}" d="${e.d}" stroke-width="${width.toFixed(1)}"><title>${escapeHtml(e.label || e.id)}: ${s.state}${s.rate ? ` (${s.rate}/min)` : ""}</title></path>`;
    }).join("") + edges().filter((e) => e.label).map((e) => {
      const m = /M ([\d.]+) ([\d.]+)/.exec(e.d);
      const [x, y] = e.id === "gate-sim" ? [420, H - 16] : e.id === "dash-gate" ? [1000, 560] : [Number(m[1]) + 8, Number(m[2]) - 6];
      return `<text class="edge-label" x="${x}" y="${y}">${escapeHtml(e.label)}</text>`;
    }).join("");
  }

  function count(n) { return Number(n || 0).toLocaleString(); }

  function metric(id) {
    const st = (data || {}).status || {}, sim = st.simulation, a = (data || {}).approvals || {};
    const simple = (name, unit) => `<strong>${rate(name)}</strong>/min · ${count(stage(name).total)} ${unit}`;
    switch (id) {
      case "sim": return sim ? `<strong>${escapeHtml(sim.state)}</strong> · ${escapeHtml(fmt.time(st.latest_reading))}<br><strong>${rate("sensor_readings")}</strong> readings/min`
        : "not running";
      case "ext": return `weather ${escapeHtml(((st.weather || {}).source) || "—")}, ${escapeHtml(agoText(stage("weather_records").latest))}<br>` +
        `carbon ${escapeHtml(((st.carbon || {}).source) || "—")}, ${escapeHtml(agoText(stage("carbon_records").latest))}`;
      case "kafka": {
        const k = (data || {}).kafka;
        if (!k) return "message counts unavailable";
        return `<ul class="topics">${Object.entries(TOPICS).map(([t, label]) =>
          `<li><span>${escapeHtml(label)}</span><span>${count((k[t] || {}).messages)}</span></li>`).join("")}</ul>`;
      }
      case "persist": return simple("sensor_readings", "stored");
      case "energy": return simple("energy_records", "records");
      case "predict": return simple("energy_predictions", "predictions");
      case "optimizer": return simple("optimizer_actions", "recommendations");
      case "security": return simple("security_alerts", "alerts");
      case "agent": return `${count(stage("agent_decisions").total)} investigations · ${count(stage("agent_actions").total)} actions`;
      case "pg": {
        const rows = Object.values((data || {}).stages || {}).reduce((s, x) => s + Number(x.total || 0), 0);
        return `<strong>${count(rows)}</strong> rows<ul class="topics">${[["sensor_readings", "readings"], ["energy_records", "energy"],
          ["energy_predictions", "predictions"], ["security_alerts", "alerts"], ["agent_decisions", "decisions"]]
          .map(([s, l]) => `<li><span>${l}</span><span>${count(stage(s).total)}</span></li>`).join("")}</ul>`;
      }
      case "gate": return (data || {}).approval_mode === "manual"
        ? `manual · <strong>${count(a.waiting)}</strong> waiting<br>${count(a.approved)} approved · ${count(a.declined)} declined · ${count(a.expired)} expired`
        : "automatic (no approval step)";
      case "dash": return "live push every 2 s<br><strong>you are here</strong>";
      default: return "";
    }
  }

  function agoText(iso) {
    const ms = ago(iso);
    if (!Number.isFinite(ms)) return "no data yet";
    if (ms < 90000) return "just now";
    return ms < 3600000 ? `${Math.round(ms / 60000)} min ago` : `${Math.round(ms / 3600000)} h ago`;
  }

  function active(id) {
    const map = {sim: "sensor_readings", ext: ["weather_records", "carbon_records"], persist: "sensor_readings",
                 energy: "energy_records", predict: "energy_predictions", optimizer: "optimizer_actions",
                 security: "sensor_readings", agent: "agent_decisions", kafka: "sensor_readings", pg: "sensor_readings"};
    return map[id] ? flow(map[id]).state === "flowing" : id === "dash";
  }

  function drawNodes() {
    const pipe = document.getElementById("pipe");
    pipe.querySelectorAll(".pnode, .stage-label").forEach((el) => el.remove());
    STAGES.forEach(([label, x]) => pipe.insertAdjacentHTML("beforeend",
      `<div class="stage-label" style="left:${(100 * x / W).toFixed(2)}%">${escapeHtml(label)}</div>`));
    NODES.forEach((n) => {
      pipe.insertAdjacentHTML("beforeend", `<button type="button" class="pnode ${n.big ? "big" : ""} ${active(n.id) ? "active" : ""} ${selected === n.id ? "selected" : ""}"
        data-id="${n.id}" style="--node:${n.color};left:${(100 * n.x / W).toFixed(2)}%;top:${(100 * n.y / H).toFixed(2)}%;
        width:${(100 * n.w / W).toFixed(2)}%;height:${(100 * n.h / H).toFixed(2)}%">
        <div class="ptitle"><i class="bi bi-${n.icon}"></i>${escapeHtml(n.title)}</div><div class="pmetric">${metric(n.id)}</div></button>`);
    });
  }

  function showDetail(id) {
    const n = BY_ID[id];
    if (!n) return;
    selected = id;
    document.getElementById("detail-title").textContent = n.title;
    document.getElementById("detail-body").innerHTML = `<p>${escapeHtml(n.text)}</p><dl class="mb-2">
      <dt>Program</dt><dd><code>${escapeHtml(n.service)}</code></dd>
      <dt>How</dt><dd>${escapeHtml(n.tech)}</dd>
      <dt>Reads</dt><dd>${escapeHtml(n.reads)}</dd>
      <dt>Writes</dt><dd>${escapeHtml(n.writes)}</dd>
      <dt>Live</dt><dd class="pmetric">${metric(id)}</dd></dl>`;
    document.querySelectorAll(".pnode").forEach((el) => el.classList.toggle("selected", el.dataset.id === id));
  }

  function renderKpis() {
    const k = (data || {}).kafka;
    const kafkaTotal = k ? Object.values(k).reduce((s, t) => s + Number(t.messages || 0), 0) : null;
    const rows = Object.values((data || {}).stages || {}).reduce((s, x) => s + Number(x.total || 0), 0);
    const a = (data || {}).approvals || {};
    const tile = (icon, color, label, value, sub) => `<div class="col-6 col-lg-3"><div class="card kpi h-100"><div class="card-body">
      <div class="kpi-icon ${color}"><i class="bi bi-${icon}"></i></div><div><div class="kpi-label">${escapeHtml(label)}</div>
      <div class="kpi-value">${escapeHtml(value)}</div><div class="kpi-sub">${escapeHtml(sub)}</div></div></div></div></div>`;
    document.getElementById("pipeline-kpis").innerHTML = [
      tile("speedometer", "bg-grad-blue", "Readings per minute", count(rate("sensor_readings")), "stored in the last minute (real time)"),
      tile("arrow-left-right", "bg-grad-purple", "Messages in Kafka", kafkaTotal == null ? "—" : count(kafkaTotal), "retained across all topics"),
      tile("database", "bg-grad-teal", "Rows in the database", count(rows), "written by the pipeline"),
      tile("person-check", "bg-grad-orange", "Waiting for approval", (data || {}).approval_mode === "manual" ? count(a.waiting) : "off",
           (data || {}).approval_mode === "manual" ? "agent actions waiting for you" : "automatic mode"),
    ].join("");
  }

  function renderTable() {
    const rows = [["Sensor storage", "sensor_readings", "sensor_readings"], ["Energy & CO₂ calculator", "energy_records", "energy_records"],
      ["ML prediction", "energy_predictions", "energy_predictions"], ["Energy optimizer", "optimizer_actions", "optimization_actions (optimizer)"],
      ["Attack detectors", "security_alerts", "security_alerts"], ["AI agent", "agent_decisions", "agent_decisions"],
      ["AI agent actions", "agent_actions", "optimization_actions (agent)"], ["Weather", "weather_records", "weather_records"],
      ["Carbon intensity", "carbon_records", "carbon_intensity_records"], ["Rejected messages", "invalid_events", "invalid_events"]];
    document.getElementById("stage-table").innerHTML = rows.map(([label, key, table]) => `<tr><td>${escapeHtml(label)}</td>
      <td><code>${escapeHtml(table)}</code></td><td>${count(rate(key))}</td><td>${count(stage(key).total)}</td>
      <td class="text-nowrap">${escapeHtml(agoText(stage(key).latest))}</td></tr>`).join("");
  }

  function renderJourney() {
    const manual = (data || {}).approval_mode === "manual";
    const steps = [
      ["The building produces a reading", "Every simulated 5 minutes (one real second) each zone's sensors are read and published to Kafka."],
      ["Kafka delivers copies", "Every service that subscribed to sensor readings gets its own copy, at its own pace."],
      ["It is checked and stored", "Sensor storage validates the reading and saves it in PostgreSQL."],
      ["Energy and CO₂ are computed", "Power becomes kWh; multiplied by the grid's carbon intensity it gives the CO₂ emitted."],
      ["The ML model predicts", "XGBoost predicts the zone's next power value; the optimizer uses it to pick efficient settings."],
      ["Attack detectors check it", "Physics rules, the Isolation Forest and drift tests look for manipulation; a suspicious reading raises an alert."],
      ["The AI agent investigates", manual ? "It diagnoses the alert and proposes a response, which passes the safety check and then waits for your approval."
                                          : "It diagnoses the alert and proposes a response, which runs if it passes the safety check."],
      ["The loop closes", "Approved actions change the building, which changes the next readings. The dashboard shows each step within about 2 seconds."],
    ];
    document.getElementById("journey").innerHTML = steps.map(([t, d]) => `<li><strong>${escapeHtml(t)}</strong>${escapeHtml(d)}</li>`).join("");
  }

  async function refresh() {
    try {
      data = await api("/api/pipeline");
    } catch (e) {
      return;
    }
    drawEdges();
    drawNodes();
    renderKpis();
    renderTable();
    renderJourney();
    if (selected) showDetail(selected);
    document.getElementById("pipeline-updated").textContent = `updated ${new Date().toLocaleTimeString()}`;
  }

  document.addEventListener("DOMContentLoaded", () => {
    document.getElementById("pipe").addEventListener("click", (event) => {
      const node = event.target.closest(".pnode");
      if (node) showDetail(node.dataset.id);
    });
    drawEdges();
    drawNodes();
    renderJourney();
    refresh();
    setInterval(refresh, REFRESH_MS);
  });
})();
