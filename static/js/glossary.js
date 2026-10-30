/* Plain-language names and explanations for the technical terms the dashboard shows
   (alert classifications, measured features, detectors, actions). Used by every page. */
"use strict";

const Glossary = (() => {
  const CLASSIFICATIONS = {
    temperature_spoofing: {label: "Temperature spoofing", color: "#dc3545", icon: "thermometer-high",
      plain: "Someone fakes the temperature readings, so the building heats or cools at the wrong time."},
    occupancy_manipulation: {label: "Occupancy manipulation", color: "#6f42c1", icon: "people",
      plain: "Fake people-count readings, e.g. an occupied room reported as empty so its lights and air turn off."},
    replay: {label: "Replay attack", color: "#d63384", icon: "arrow-repeat",
      plain: "Old sensor data is sent again, hiding what is really happening in the room."},
    abnormal_power: {label: "Abnormal power", color: "#0d6efd", icon: "lightning-charge",
      plain: "The power meter shows values that do not fit the equipment that is running."},
    gradual_manipulation: {label: "Gradual manipulation", color: "#fd7e14", icon: "graph-down-arrow",
      plain: "Readings are pushed away from reality very slowly, too slowly for single-reading checks to notice."},
    data_drift: {label: "Data drift (not an attack)", color: "#20c997", icon: "calendar-range",
      plain: "Normal conditions have changed since training (season, weather, usage). Not an attack, but the ML " +
             "model may become less accurate and should be retrained."},
    unknown_anomaly: {label: "Unusual reading", color: "#6c757d", icon: "question-circle",
      plain: "A reading that looks unusual but matches no known attack pattern."},
    invalid_message: {label: "Invalid message", color: "#adb5bd", icon: "file-earmark-x",
      plain: "A message that broke the data-format rules and was rejected."},
  };

  const FEATURES = {
    temperature_c: {label: "Temperature", unit: "°C"},
    humidity_pct: {label: "Humidity", unit: "%"},
    co2_ppm: {label: "CO₂ level", unit: "ppm"},
    total_power_kw: {label: "Power use", unit: "kW"},
    occupancy_count: {label: "People in the zone", unit: ""},
    lighting_lux: {label: "Light level", unit: "lux"},
    control_error_active: {label: "Temperature-vs-target gap", unit: "°C", security: true,
      plain: "How far the room is from its target temperature while heating or cooling is on. It normally " +
             "stays stable; a lasting shift suggests the temperature sensor is lying."},
    co2_residual: {label: "CO₂-vs-people mismatch", unit: "ppm", security: true,
      plain: "Measured CO₂ minus the CO₂ expected from the number of people. It normally stays stable; a " +
             "lasting shift suggests the CO₂ or people-count readings are being manipulated."},
  };

  const DETECTORS = {
    rules: "physics rules",
    isolation_forest: "ML anomaly detector (Isolation Forest)",
    "rules+isolation_forest": "physics rules + ML anomaly detector",
    "ks_test+psi": "statistical drift test (KS + PSI)",
    schema_validation: "data-format check",
  };

  const SENSORS = {temperature: "temperature", occupancy: "people-count", power_meter: "power-meter", co2: "CO₂"};

  function classification(name) {
    return CLASSIFICATIONS[name] || {label: String(name || "alert").replaceAll("_", " "), color: "#6c757d",
                                     icon: "exclamation-circle", plain: ""};
  }

  function feature(name) {
    const key = String(name || "");
    return FEATURES[key] || {label: key.replaceAll("_", " "), unit: ""};
  }

  function detector(name) {
    return DETECTORS[name] || String(name || "").replaceAll("_", " ").replaceAll("+", " + ");
  }

  function sensor(name) {
    return SENSORS[name] || String(name || "").replaceAll("_", " ");
  }

  /** One sentence describing an action, e.g. "Stop trusting the temperature sensor". */
  function action(type, p = {}) {
    switch (type) {
      case "isolate_sensor": return `Stop trusting the ${sensor(p.sensor)} sensor (quarantine)`;
      case "restore_sensor": return `Trust the ${sensor(p.sensor)} sensor again`;
      case "clear_overrides": return "Return the zone to its normal schedule";
      case "set_setpoint": return p.previous_setpoint_c != null
        ? `Change the target temperature from ${Number(p.previous_setpoint_c).toFixed(1)} to ${Number(p.setpoint_c).toFixed(1)} °C`
        : `Set the target temperature to ${Number(p.setpoint_c).toFixed(1)} °C`;
      case "set_hvac_mode": return `Switch heating/cooling to “${p.hvac_mode}”`;
      case "set_lighting": return `Set the lights to ${Math.round(100 * Number(p.lighting_level))} %`;
      default: return String(type || "").replaceAll("_", " ");
    }
  }

  /** How strong a PSI value is, in words. */
  function psiStrength(psi) {
    if (psi == null) return "";
    if (psi < 0.1) return "no real change";
    if (psi < 0.25) return "small change";
    if (psi < 1) return "clear change";
    return "very large change";
  }

  /** Replace raw measure names (e.g. humidity_pct) in detector descriptions with plain names. */
  function humanize(text) {
    return String(text ?? "").replace(/\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b/g, (word) => {
      const label = FEATURES[word] && FEATURES[word].label;
      return !label ? word : /^[A-Z]{2}/.test(label) ? label : label[0].toLowerCase() + label.slice(1);
    });
  }

  return {CLASSIFICATIONS, FEATURES, classification, feature, detector, sensor, action, psiStrength, humanize};
})();

/* Plain-language verdict and one-paragraph story for an agent decision. */
const Verdict = (() => {
  const STYLES = {
    acted: {label: "Attack confirmed · action taken", icon: "shield-fill-exclamation", bg: "#fee2e2", fg: "#991b1b"},
    confirmed: {label: "Attack confirmed", icon: "shield-exclamation", bg: "#ffedd5", fg: "#9a3412"},
    false_alarm: {label: "False alarm", icon: "check-circle-fill", bg: "#dcfce7", fg: "#166534"},
    not_attack: {label: "Not an attack", icon: "info-circle-fill", bg: "#e0f2fe", fg: "#075985"},
    too_early: {label: "Too early to tell", icon: "hourglass-split", bg: "#f1f5f9", fg: "#475569"},
    review: {label: "Needs a person to review", icon: "person-exclamation", bg: "#ede9fe", fg: "#5b21b6"},
  };

  function kind(d) {
    const cause = String(d.likely_cause || "").toLowerCase();
    const recs = (d.recommendations || []).join(" ").toLowerCase();
    if (recs.includes("readings are available") || recs.includes("cold start")) return "too_early";
    if (cause.includes("false alarm") || cause.includes("normal operation")) return "false_alarm";
    if (d.classification === "data_drift" || cause.includes("not an attack")) return "not_attack";
    if (cause.includes("undetermined") || cause.includes("not zone-specific")) return "review";
    if ((d.actions || []).some((a) => ["approved", "executed"].includes(a.status))) return "acted";
    return d.confidence === "low" ? "review" : "confirmed";
  }

  function of(d) { return {kind: kind(d), ...STYLES[kind(d)]}; }

  function story(d) {
    const c = Glossary.classification(d.classification || d.alert_type);
    const where = d.zone_id ? `zone ${d.zone_id}` : "the building";
    const checks = (d.tool_calls || []).length;
    const intro = `A detector raised a “${c.label}” alert for ${where}. The AI agent re-checked the latest data` +
                  (checks ? ` (${checks} checks)` : "") + ". ";
    const done = (d.actions || []).filter((a) => ["approved", "executed"].includes(a.status))
      .map((a) => Glossary.action(a.action_type, a.parameters).toLowerCase());
    const blocked = (d.actions || []).filter((a) => a.status === "rejected").length;
    switch (kind(d)) {
      case "too_early": return intro + "There is not enough history yet (it needs about one simulated day), so it only keeps watching the zone.";
      case "false_alarm": return intro + "The checks did not confirm the alert, so it is treated as a false alarm and nothing was changed.";
      case "not_attack": return intro + "Conditions have simply changed (e.g. the season), which is not an attack. No action is needed, but the ML model should be retrained.";
      case "acted": return intro + `The checks confirmed the attack, so it acted: ${done.join("; ")}.` +
        (blocked ? ` ${blocked} other proposal(s) were blocked by the safety check.` : "");
      case "confirmed": return intro + "The checks confirmed the problem." +
        (blocked ? ` Its proposed actions were blocked by the safety check (${blocked}).` : " No automatic action was needed.");
      default: return intro + "The result was not clear, so a person should review it.";
    }
  }

  return {of, story, STYLES};
})();
