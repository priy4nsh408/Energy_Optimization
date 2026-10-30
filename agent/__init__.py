"""Agentic AI: a LangGraph incident agent for security alerts.

The agent investigates an alert with read-only tools, explains the cause and
the energy/operational impact, and *proposes* corrective actions. Every
proposal goes through the deterministic :mod:`agent.safety` validator; only
approved actions are executed, and only on the simulated building
(:mod:`agent.executor`). The agent never controls physical equipment.
"""
