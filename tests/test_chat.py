"""Dashboard chat assistant (services.chat): topics, answers from data, SLM phrasing and fallback."""

from __future__ import annotations

import pytest

from agent.slm import SLMError
from services.chat import EXAMPLES, ChatAssistant


class Data:
    def status(self):
        return {"simulation": {"state": "running"}, "latest_reading": "2026-02-10T12:00:00+00:00",
                "counts": {"open_alerts": 4, "simulated_attacks": 1},
                "weather": {"source": "open-meteo", "temperature_c": 31.2, "humidity_pct": 70, "cloud_cover_pct": 20}}

    def live_readings(self):
        return [{"zone_id": "Z1", "event_time": "2026-02-10T12:00:00+00:00", "temperature_c": 22.4, "hvac_setpoint_c": 22,
                 "co2_ppm": 812, "humidity_pct": 40, "occupancy_count": 18, "total_power_kw": 7.2, "hvac_status": "cooling",
                 "isolated_sensors": ["temperature"]},
                {"zone_id": "Z5", "event_time": "2026-02-10T12:00:00+00:00", "temperature_c": 20.9, "hvac_setpoint_c": 20,
                 "co2_ppm": 420, "humidity_pct": 31, "occupancy_count": 0, "total_power_kw": 6.0, "hvac_status": "cooling",
                 "isolated_sensors": []}]

    def energy_summary(self, hours=24):
        return {"energy_kwh": 462.0, "hvac_kwh": 119.0, "lighting_kwh": 82.0, "appliance_kwh": 261.0,
                "carbon_kg_co2e": 131.2}

    def approvals(self, limit=50):
        return [{"zone_id": "Z1", "action_type": "isolate_sensor",
                 "parameters": {"approval": {"title": "Quarantine the temperature sensor in Z1"}}}]

    def decisions(self, limit=30):
        return [{"zone_id": "Z1", "classification": "temperature_spoofing", "likely_cause": "spoofed temperature",
                 "confidence": "high", "actions": [{"action_type": "isolate_sensor", "status": "awaiting_approval"}],
                 "outcome": "1 awaiting_approval", "llm_provider": "offline"}]

    def isolated_sensors(self):
        return [{"zone_id": "Z1", "sensor": "temperature"}]

    def alerts(self, limit=100, alert_type=None, zone_id=None):
        return [{"zone_id": "Z1", "alert_type": "attack", "classification": "temperature_spoofing", "severity": "high"}]

    def predictions(self, hours=12, zone_id=None):
        return {"rows": [], "accuracy": [{"zone_id": "Z1", "n": 100, "mae_kw": 0.2, "mean_actual_kw": 5.0},
                                         {"zone_id": "Z3", "n": 100, "mae_kw": 0.05, "mean_actual_kw": 0.8}]}

    def optimizer_savings(self, hours=24):
        return [{"baseline_kw": 10.0, "optimized_kw": 9.0}]

    def comparison(self):
        return {"cases": {"winter week": [{"energy_kwh": 100.0}, {"energy_kwh": 92.6}]}}

    def pipeline(self):
        return {"stages": {"sensor_readings": {"last_minute": 300}}}


@pytest.fixture
def chat():
    return ChatAssistant(Data())


@pytest.mark.parametrize("question, topic, fragment", [
    ("hello", "help", "Try:"),
    ("What is PSI?", "glossary", "Population Stability Index"),
    ("what does security drift mean", "glossary", "Security drift"),
    ("How is zone Z1?", "zone", "22.4 °C"),
    ("how is the server room doing", "zone", "Z5"),
    ("Which actions are waiting for my approval?", "approvals", "Quarantine the temperature sensor in Z1"),
    ("What did the AI agent decide last?", "agent", "spoofed temperature"),
    ("Which sensors are quarantined?", "quarantine", "Z1 temperature"),
    ("Are there any security alerts?", "security", "4 alerts are open"),
    ("How accurate is the ML model?", "accuracy", "200 predictions"),
    ("How much does the optimizer save?", "optimizer", "10.0 % less power"),
    ("How much energy was used today?", "energy", "462 kWh"),
    ("How much power is the building using now?", "power", "13.2 kW"),
    ("What's the weather outside?", "weather", "31.2 °C"),
    ("How does the data pipeline work?", "pipeline", "300 readings"),
    ("Is the simulation running?", "simulation", "running"),
    ("who won the football", "unknown", "only answer questions"),
])
def test_topics_are_answered_from_the_data(chat, question, topic, fragment):
    answer = chat.answer(question, page="pages.index")
    assert answer["topic"] == topic and fragment in answer["answer"], answer
    assert answer["source"] == "built-in answers"


def test_glossary_terms_match_whole_words(chat):
    assert chat.answer("what does the pipeline workflow look like")["topic"] == "pipeline"     # not "ks"
    assert chat.answer("What is data drift?")["answer"].startswith("Data drift")               # longest term wins


def test_unknown_and_empty_questions_suggest_examples(chat):
    assert chat.answer("  ")["topic"] == "empty"
    assert chat.answer("tell me a joke")["suggestions"] == EXAMPLES[:5]
    assert len(chat.answer("x" * 2000)["answer"]) < 400


def test_slm_phrases_answers_and_falls_back():
    class SLM:
        name = "tiny (Ollama)"

        def __init__(self, fail):
            self.fail, self.prompts = fail, []

        def generate(self, system, prompt, max_tokens=300):
            self.prompts.append(prompt)
            if self.fail:
                raise SLMError("down")
            return "The building uses about 13 kW."

    slm = SLM(False)
    answer = ChatAssistant(Data(), slm).answer("How much power is the building using now?")
    assert answer["answer"] == "The building uses about 13 kW." and answer["source"] == "small language model tiny (Ollama)"
    assert '"total_kw": 13.2' in slm.prompts[0]                     # the SLM only sees facts from the database
    assert ChatAssistant(Data(), SLM(False)).answer("hello")["source"] == "built-in answers"   # help needs no SLM
    fallback = ChatAssistant(Data(), SLM(True)).answer("How much power is the building using now?")
    assert fallback["source"] == "built-in answers" and "13.2 kW" in fallback["answer"]
