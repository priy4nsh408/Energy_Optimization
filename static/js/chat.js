/* AI chat assistant: floating button on every page, answers from POST /api/chat. */
"use strict";

(() => {
  const SUGGESTIONS = {
    "pages.index": ["How much power is the building using now?", "How is zone Z1?", "Are there any security alerts?"],
    "pages.energy": ["How much energy was used today?", "How accurate is the ML model?", "How much does the optimizer save?"],
    "pages.security": ["Are there any security alerts?", "What is PSI?", "What is security drift?", "Which sensors are quarantined?"],
    "pages.agent": ["What did the AI agent decide last?", "Which actions are waiting for my approval?", "What is the safety validator?"],
    "pages.pipeline": ["How does the data pipeline work?", "What is Kafka?", "Is the simulation running?"],
  };
  const el = (id) => document.getElementById(id);

  function add(text, who, source) {
    const msg = document.createElement("div");
    msg.className = `chat-msg ${who}`;
    msg.textContent = text;
    if (source) {
      const src = document.createElement("span");
      src.className = "src";
      src.textContent = `answered with ${source}`;
      msg.appendChild(src);
    }
    el("chat-log").appendChild(msg);
    el("chat-log").scrollTop = el("chat-log").scrollHeight;
    return msg;
  }

  function suggest(list) {
    const box = el("chat-suggest");
    box.innerHTML = "";
    (list || []).slice(0, 4).forEach((q) => {
      const b = document.createElement("button");
      b.type = "button";
      b.textContent = q;
      b.addEventListener("click", () => ask(q));
      box.appendChild(b);
    });
  }

  async function ask(question) {
    question = String(question || "").trim();
    if (!question) return;
    add(question, "user");
    el("chat-input").value = "";
    const typing = add(window.DASHBOARD.slmName ? "thinking…" : "looking it up…", "bot typing");
    try {
      const r = await App.postJson("/api/chat", {question, page: window.DASHBOARD.page});
      typing.remove();
      add(r.answer, "bot", r.source);
      if (r.suggestions && r.suggestions.length) suggest(r.suggestions);
    } catch (e) {
      typing.remove();
      add(`Sorry, I could not answer: ${e.message}`, "bot");
    }
  }

  function toggle(open) {
    el("chat-panel").hidden = !open;
    el("chat-open").hidden = open;
    el("chat-open").setAttribute("aria-expanded", String(open));
    if (open) el("chat-input").focus();
  }

  document.addEventListener("DOMContentLoaded", () => {
    if (!el("chat-open")) return;
    el("chat-mode").textContent = window.DASHBOARD.slmName
      ? `answers from the live data, phrased by ${window.DASHBOARD.slmName}` : "answers from the live data";
    suggest(SUGGESTIONS[window.DASHBOARD.page] || SUGGESTIONS["pages.index"]);
    el("chat-open").addEventListener("click", () => toggle(true));
    el("chat-close").addEventListener("click", () => toggle(false));
    el("chat-form").addEventListener("submit", (event) => { event.preventDefault(); ask(el("chat-input").value); });
    document.addEventListener("keydown", (event) => { if (event.key === "Escape" && !el("chat-panel").hidden) toggle(false); });
  });
})();
