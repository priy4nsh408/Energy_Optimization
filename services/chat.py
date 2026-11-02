"""Dashboard chat: answer basic questions about the system, its models and its data.

Every answer is built from facts read from PostgreSQL (``DashboardData``) or
from a fixed glossary - nothing is invented. A question is matched to a topic
by keywords; the topic's handler collects the facts and writes a plain answer.
If a small language model is configured (``SLM_PROVIDER=ollama``) it rewrites
the same facts as a conversational answer; if it is unavailable the built-in
answer is returned. Questions outside the known topics get a list of examples.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Callable

from agent.slm import OllamaSLM, SLMError
from simulator.building_model import default_zones

logger = logging.getLogger(__name__)
MAX_QUESTION_CHARS = 500
SLM_SYSTEM = (
    "You are the assistant of a smart-building energy and security dashboard. Answer the user's question in at most "
    "four short sentences, using only the JSON facts provided. If the facts do not answer the question, say so and "
    "suggest what the user can ask instead. Write plain language for non-experts and explain technical terms briefly. "
    "No markdown.")
EXAMPLES = ["How much power is the building using now?", "Are there any security alerts?",
            "What did the AI agent decide last?", "How accurate is the ML model?",
            "How much does the optimizer save?", "What is PSI?", "How is zone Z1?",
            "Which actions are waiting for my approval?", "How does the data pipeline work?"]

GLOSSARY = {
    "psi": ("PSI (Population Stability Index)", "measures how much the spread of a measurement changed compared with "
            "the training data: below 0.1 no real change, 0.1-0.25 small, above 0.25 significant."),
    "ks": ("KS test", "the Kolmogorov-Smirnov test compares recent readings with the training data. Its p-value is the "
           "chance that the difference is pure luck; a tiny p-value means the change is real."),
    "p-value": ("p-value", "the chance that a difference this large is pure luck. Very small values (e.g. 1e-10) mean "
                "the change is certainly real."),
    "security drift": ("Security drift", "relationships that should never change (e.g. CO2 vs. number of people, room "
                       "temperature vs. its target) have shifted - a sign of slow sensor manipulation."),
    "data drift": ("Data drift", "ordinary values differ from the training period, usually because of the season or "
                   "usage. Not an attack, but the ML model may get less accurate and should be retrained."),
    "drift": ("Drift", "a slow change in the data over hours. Security drift can mean manipulation; data drift means "
              "conditions changed and the model may need retraining."),
    "isolation forest": ("Isolation Forest", "an ML anomaly detector that learned what normal readings look like for "
                         "each zone and flags readings that are very unlike them."),
    "mae": ("MAE (mean absolute error)", "how far the ML prediction is off on average, in kW. Smaller is better."),
    "rmse": ("RMSE", "root mean squared error: like MAE, but large mistakes count more."),
    "xgboost": ("XGBoost", "the gradient-boosted tree model that predicts each zone's power 5 minutes ahead."),
    "baseline": ("Baseline", "the building's normal fixed schedule of target temperatures and light levels, used to "
                 "measure how much the optimizer saves."),
    "quarantine": ("Sensor quarantine", "the AI agent stops trusting a sensor it believes is manipulated; the zone "
                   "then runs on its normal schedule until the sensor is trusted again after a quiet hour."),
    "kafka": ("Kafka", "the message system that carries every reading, alert and command between the services."),
    "carbon intensity": ("Carbon intensity", "grams of CO2 emitted per kWh of grid electricity; energy x intensity "
                         "gives the CO2 emitted."),
    "slm": ("Small language model", "a compact AI model running locally (via Ollama) that phrases explanations and "
            "chat answers from the system's facts. It never decides anything."),
    "approval": ("Operator approval", "in manual mode, every action the AI agent wants to take - after it passes the "
                 "safety validator - waits for a person to approve or decline it on the AI Agent page."),
    "safety validator": ("Safety validator", "fixed rules that check every proposed action (limits, comfort, "
                         "equipment safety, rate limits). The AI cannot bypass it."),
}
_DEFINE = re.compile(r"^\s*(what('?s| is| are| does)|define|explain|meaning|mean)\b|\bmean(s|ing)?\b", re.I)
_ZONE = re.compile(r"\bz\s?([1-9])\b", re.I)


def _n(value: Any, digits: int = 1) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return "unknown"


class ChatAssistant:
    def __init__(self, data, slm: OllamaSLM | None = None) -> None:
        self.data, self.slm = data, slm
        self.zones = {z.zone_id: z.name for z in default_zones()}
        self.topics: list[tuple[str, Callable[[str], bool], Callable[[str], tuple[str, dict]]]] = [
            ("help", lambda q: bool(re.search(r"\b(help|hello|hi|hey|what can you)\b", q)), self._help),
            ("glossary", self._is_definition, self._glossary),
            ("zone", lambda q: bool(_ZONE.search(q)) or any(n.lower() in q for n in self.zones.values()), self._zone),
            ("approvals", lambda q: bool(re.search(r"approv|pending|waiting|permission", q)), self._approvals),
            ("agent", lambda q: bool(re.search(r"\bagent|decision|decide|diagnos|\bai\b", q)), self._agent),
            ("quarantine", lambda q: bool(re.search(r"isolat|quarantin|trust", q)), self._quarantine),
            ("security", lambda q: bool(re.search(r"alert|attack|threat|secur|safe|hack|spoof", q)), self._security),
            ("accuracy", lambda q: bool(re.search(r"accura|predict|\bml\b|model|error|mae", q)), self._accuracy),
            ("optimizer", lambda q: bool(re.search(r"optimi[sz]|saving|\bsave|baseline", q)), self._optimizer),
            ("energy", lambda q: bool(re.search(r"energy|kwh|carbon|co2|emission|today", q)), self._energy),
            ("power", lambda q: bool(re.search(r"power|consum|\bkw\b|using|usage|electric", q)), self._power),
            ("weather", lambda q: bool(re.search(r"weather|outside|outdoor", q)), self._weather),
            ("pipeline", lambda q: bool(re.search(r"pipeline|how does|how do|work|flow|kafka|architecture", q)),
             self._pipeline),
            ("simulation", lambda q: bool(re.search(r"simulat|running|time|clock", q)), self._simulation),
        ]

    # -- entry point --------------------------------------------------------------------
    def answer(self, question: str, page: str | None = None) -> dict[str, Any]:
        question = " ".join(str(question or "").split())[:MAX_QUESTION_CHARS]
        if not question:
            return {"answer": "Ask me something about the building, its data or its models.", "topic": "empty",
                    "source": "built-in answers", "suggestions": EXAMPLES[:4]}
        q = question.lower()
        topic, handler = next(((name, fn) for name, match, fn in self.topics if match(q)), ("unknown", self._unknown))
        text, facts = handler(q)
        source = "built-in answers"
        if self.slm is not None and topic not in ("help", "unknown"):
            try:
                text = self.slm.generate(SLM_SYSTEM, f"Question: {question}\nFacts (JSON): "
                                         f"{json.dumps(facts, default=str, ensure_ascii=False)[:6000]}", max_tokens=250)
                source = f"small language model {self.slm.name}"
            except SLMError as exc:
                logger.warning("SLM chat answer failed; using the built-in answer", extra={"error": str(exc)})
        return {"answer": text, "topic": topic, "source": source, "page": page,
                "suggestions": [] if topic != "unknown" else EXAMPLES[:5]}

    # -- topics -------------------------------------------------------------------------
    def _help(self, q: str) -> tuple[str, dict]:
        return ("I answer questions about this building's live data and models: power and energy use, CO2, security "
                "alerts, the AI agent's decisions, actions waiting for approval, ML accuracy, optimizer savings and "
                "technical terms such as PSI or drift. Try: " + "; ".join(EXAMPLES[:4]) + ".", {})

    @staticmethod
    def _terms(q: str) -> list[str]:
        return [t for t in GLOSSARY if re.search(rf"\b{re.escape(t)}\b", q)]

    def _is_definition(self, q: str) -> bool:
        return bool(_DEFINE.search(q)) and bool(self._terms(q))

    def _glossary(self, q: str) -> tuple[str, dict]:
        # Longest matching term first, so "data drift" wins over "drift".
        term = max(self._terms(q), key=len)
        name, text = GLOSSARY[term]
        return f"{name}: {text}", {"term": name, "definition": text}

    def _zone(self, q: str) -> tuple[str, dict]:
        match = _ZONE.search(q)
        zone = f"Z{match.group(1)}" if match else next(z for z, n in self.zones.items() if n.lower() in q)
        reading = next((r for r in self.data.live_readings() if r["zone_id"] == zone), None)
        if reading is None:
            return f"There is no reading for {zone} yet. Press Start on the dashboard to run the simulation.", {}
        facts = {k: reading.get(k) for k in ("zone_id", "event_time", "temperature_c", "hvac_setpoint_c", "co2_ppm",
                                              "humidity_pct", "occupancy_count", "total_power_kw", "hvac_status",
                                              "isolated_sensors")} | {"name": self.zones.get(zone)}
        text = (f"{zone} ({self.zones.get(zone, 'zone')}) at {str(reading['event_time'])[:16].replace('T', ' ')}: "
                f"{_n(reading['temperature_c'])} °C (target {_n(reading['hvac_setpoint_c'])} °C), "
                f"{reading['occupancy_count']} people, CO2 {_n(reading['co2_ppm'], 0)} ppm, "
                f"using {_n(reading['total_power_kw'], 2)} kW (heating/cooling {reading['hvac_status']}).")
        if reading.get("isolated_sensors"):
            text += f" Not trusted right now: {', '.join(reading['isolated_sensors'])} sensor."
        return text, facts

    def _approvals(self, q: str) -> tuple[str, dict]:
        rows = self.data.approvals(limit=10)
        if not rows:
            return ("No action is waiting for your approval. In manual approval mode, actions the AI agent wants to "
                    "take appear on the AI Agent page with Approve and Decline buttons.", {"waiting": 0})
        items = [f"{r['zone_id']}: {((r.get('parameters') or {}).get('approval') or {}).get('title') or r['action_type']}"
                 for r in rows]
        return (f"{len(rows)} action(s) are waiting for your approval: {'; '.join(items)}. Open the AI Agent page to "
                "approve or decline them.", {"waiting": items})

    def _agent(self, q: str) -> tuple[str, dict]:
        rows = self.data.decisions(limit=1)
        if not rows:
            return "The AI agent has not investigated anything yet. It starts when a detector raises an alert.", {}
        d = rows[0]
        done = [f"{a['action_type']} ({a['status']})" for a in d.get("actions") or []]
        facts = {k: d.get(k) for k in ("zone_id", "classification", "likely_cause", "confidence", "outcome",
                                        "llm_provider")} | {"actions": done}
        text = (f"Latest investigation: zone {d.get('zone_id') or 'building'}, alert "
                f"'{str(d.get('classification') or d.get('alert_type') or 'unknown').replace('_', ' ')}'. "
                f"The agent concluded: {d['likely_cause']} (confidence {d['confidence']}). "
                + (f"Actions: {', '.join(done)}." if done else "It took no action."))
        return text, facts

    def _quarantine(self, q: str) -> tuple[str, dict]:
        rows = self.data.isolated_sensors()
        if not rows:
            return "No sensor is quarantined: every sensor is trusted right now.", {"isolated": []}
        items = [f"{r['zone_id']} {r['sensor']}" for r in rows]
        return (f"Quarantined (not trusted): {', '.join(items)}. Each is trusted again automatically after its zone "
                "has had no security alert for one simulated hour.", {"isolated": items})

    def _security(self, q: str) -> tuple[str, dict]:
        status, alerts = self.data.status(), self.data.alerts(limit=5)
        counts = status.get("counts") or {}
        if not alerts:
            return "There are no security alerts so far.", {"open_alerts": 0}
        latest = [f"{a.get('zone_id') or 'building'} {str(a.get('classification') or a['alert_type']).replace('_', ' ')} "
                  f"({a['severity']})" for a in alerts[:3]]
        return (f"{counts.get('open_alerts', 0)} alerts are open. Latest: {'; '.join(latest)}. "
                f"{counts.get('simulated_attacks', 0)} simulated attacks have been launched; alerts are only "
                "suspicions until the AI agent re-checks them.", {"open": counts.get("open_alerts"), "latest": latest})

    def _accuracy(self, q: str) -> tuple[str, dict]:
        accuracy = self.data.predictions(hours=1)["accuracy"]
        if not accuracy:
            return "No ML predictions are stored yet. They start once the simulation runs.", {}
        n = sum(a["n"] for a in accuracy)
        mae = sum(a["mae_kw"] * a["n"] for a in accuracy) / n
        mean = sum(a["mean_actual_kw"] * a["n"] for a in accuracy) / n
        best = min(accuracy, key=lambda a: a["mae_kw"] / max(a["mean_actual_kw"], 1e-6))
        return (f"The XGBoost model predicts each zone's power 5 minutes ahead. Over {n:,} predictions its average "
                f"error is {_n(mae, 3)} kW, about {_n(100 * mae / mean, 1)} % of the average power. Most accurate zone: "
                f"{best['zone_id']}.", {"predictions": n, "mae_kw": mae, "mean_power_kw": mean})

    def _optimizer(self, q: str) -> tuple[str, dict]:
        live, experiment = self.data.optimizer_savings(24), self.data.comparison()
        parts, facts = [], {}
        if live:
            base = sum(r["baseline_kw"] or 0 for r in live)
            opt = sum(r["optimized_kw"] or 0 for r in live)
            if base:
                facts["live_predicted_saving_pct"] = 100 * (1 - opt / base)
                parts.append(f"Right now the optimizer's choices are predicted to use {_n(100 * (1 - opt / base))} % "
                             "less power than the normal schedule (a model prediction).")
        if experiment:
            for week, results in experiment["cases"].items():
                base, opt = results[0], results[-1]
                saving = 100 * (1 - opt["energy_kwh"] / base["energy_kwh"])
                facts[week] = saving
                parts.append(f"In the measured {week} experiment it saved {_n(saving)} % energy.")
        if not parts:
            return "There are no optimizer results yet. Start the simulation to see live recommendations.", {}
        return " ".join(parts) + " It chooses target temperatures and light levels that keep people comfortable.", facts

    def _energy(self, q: str) -> tuple[str, dict]:
        s = self.data.energy_summary(24)
        if s.get("energy_kwh") is None:
            return "No energy data yet. Press Start on the dashboard to run the simulation.", {}
        text = (f"In the last 24 simulated hours the building used {_n(s['energy_kwh'], 0)} kWh (heating/cooling "
                f"{_n(s['hvac_kwh'], 0)}, lights {_n(s['lighting_kwh'], 0)}, devices {_n(s['appliance_kwh'], 0)})")
        text += f" and emitted {_n(s['carbon_kg_co2e'])} kg of CO2." if s.get("carbon_kg_co2e") is not None else "."
        return text, s

    def _power(self, q: str) -> tuple[str, dict]:
        readings = self.data.live_readings()
        if not readings:
            return "No readings yet. Press Start on the dashboard to run the simulation.", {}
        total = sum(r["total_power_kw"] or 0 for r in readings)
        top = max(readings, key=lambda r: r["total_power_kw"] or 0)
        facts = {r["zone_id"]: r["total_power_kw"] for r in readings} | {"total_kw": total}
        return (f"The building uses {_n(total)} kW right now. The biggest user is {top['zone_id']} "
                f"({self.zones.get(top['zone_id'], '')}) with {_n(top['total_power_kw'], 2)} kW.", facts)

    def _weather(self, q: str) -> tuple[str, dict]:
        weather = self.data.status().get("weather")
        if not weather:
            return "No weather data has arrived yet.", {}
        return (f"Latest outdoor weather ({weather.get('source')}): {_n(weather.get('temperature_c'))} °C, humidity "
                f"{_n(weather.get('humidity_pct'), 0)} %, cloud cover {_n(weather.get('cloud_cover_pct'), 0)} %.", weather)

    def _pipeline(self, q: str) -> tuple[str, dict]:
        stages = self.data.pipeline()["stages"]
        per_min = (stages.get("sensor_readings") or {}).get("last_minute", 0)
        return ("The simulator publishes one reading per zone to Kafka. Separate services store the readings in "
                "PostgreSQL, compute energy and CO2, predict power with the ML model, optimize settings, and check for "
                f"attacks; the AI agent investigates alerts. In the last minute {per_min} readings were stored. The "
                "Data Pipeline page shows the whole flow live.", {"readings_last_minute": per_min})

    def _simulation(self, q: str) -> tuple[str, dict]:
        status = self.data.status()
        sim = status.get("simulation")
        if not sim:
            return "The simulation manager is not running. Start the system with start.ps1.", {}
        return (f"The simulation is {sim['state']} at simulated time "
                f"{str(status.get('latest_reading') or '')[:16].replace('T', ' ')}; one simulated hour passes every "
                "12 seconds.", {"state": sim["state"], "latest_reading": status.get("latest_reading")})

    def _unknown(self, q: str) -> tuple[str, dict]:
        return ("I can only answer questions about this building's data and models. Here are some examples.", {})
