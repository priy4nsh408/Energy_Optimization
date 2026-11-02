"""Small language model (SLM) client for plain-language text, served locally by Ollama.

The SLM only *phrases* text from facts it is given: explanations of actions
waiting for operator approval (``agent.explainer``) and chat answers on the
dashboard (``services.chat``). It never decides whether an action is safe (the
deterministic safety validator does) or whether it runs (the operator does).
Every caller has a template fallback, so the system works without it.

    ollama pull qwen2.5:1.5b          # once
    SLM_PROVIDER=ollama               # in .env
"""

from __future__ import annotations

import logging
import re

import requests

from config import SLMSettings

logger = logging.getLogger(__name__)
_THINK = re.compile(r"<think>.*?</think>", re.S)      # some small models print their reasoning first


class SLMError(RuntimeError):
    """The model did not produce a usable answer (not running, model missing, timeout, empty reply)."""


class OllamaSLM:
    def __init__(self, settings: SLMSettings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()

    @property
    def name(self) -> str:
        return f"{self.settings.model} (Ollama)"

    def generate(self, system: str, prompt: str, *, max_tokens: int = 300) -> str:
        """One answer to ``prompt``; raises :class:`SLMError`."""
        try:
            response = self.session.post(f"{self.settings.base_url}/api/chat", timeout=self.settings.timeout_s, json={
                "model": self.settings.model, "stream": False,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
                "options": {"temperature": 0.2, "num_predict": max_tokens}})
            response.raise_for_status()
            text = str(response.json()["message"]["content"])
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            raise SLMError(f"{self.name}: {exc}") from exc
        text = _THINK.sub("", text).strip()
        if not text:
            raise SLMError(f"{self.name}: empty answer")
        return text

    def available(self) -> tuple[bool, str]:
        """Whether Ollama answers and has the model (for preflight)."""
        try:
            response = self.session.get(f"{self.settings.base_url}/api/tags", timeout=5)
            response.raise_for_status()
            names = {m.get("name", "") for m in response.json().get("models", [])}
        except (requests.RequestException, ValueError) as exc:
            return False, f"Ollama not reachable at {self.settings.base_url} ({exc.__class__.__name__})"
        wanted = self.settings.model
        if wanted in names or f"{wanted}:latest" in names:
            return True, f"{wanted} served by Ollama at {self.settings.base_url}"
        return False, f"model {wanted} not pulled (Ollama has: {', '.join(sorted(names)) or 'none'})"


def create_slm(settings: SLMSettings) -> OllamaSLM | None:
    """The configured SLM, or ``None`` (templates only)."""
    return OllamaSLM(settings) if settings.provider == "ollama" else None
