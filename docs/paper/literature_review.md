# Literature review — reading list and gaps (Week 1, draft)

**Status:** first pass, 19 Nov 2026. Every entry below was found by web search; most details come from
abstracts or search snippets, **not** full texts. Before citing anything: open the paper, confirm the
authors, venue, year and DOI, and check that it says what the note claims. Mark each entry ✅ once checked.
Preprints (arXiv) should be replaced by their peer-reviewed version where one exists.

Legend: ✅ checked · ☐ not yet checked · *(preprint)* · *(year to confirm)*

---

## A. Reading list

### A1. Building energy forecasting
| ☐ | Paper | Why read it |
|---|---|---|
| ☐ | Candanedo, Feldheim, Deramaix, "Data driven prediction models of energy use of appliances in a low-energy house", *Energy and Buildings* 140 (2017) 81–97, doi:10.1016/j.enbuild.2017.01.083 | **Original paper of our KAG/UCI dataset — must cite.** Gradient boosting: test R² ≈ 0.57 (their split and features differ from ours; compare carefully). [link](https://archive.ics.uci.edu/dataset/374/appliances+energy+prediction) |
| ☐ | Miller et al., "The Building Data Genome Project 2, energy meter data from the ASHRAE Great Energy Predictor III competition", *Scientific Data* 7, 368 (2020), doi:10.1038/s41597-020-00712-x | Candidate **second dataset** (3,053 meters, 1,636 buildings, hourly, 2016–2017). [link](https://pmc.ncbi.nlm.nih.gov/articles/PMC7591488) |
| ☐ | "A Review of Deep Learning Techniques for Forecasting Energy Use in Buildings", *Energies* 14(3), 608 (2021) | Review of DL forecasters (LSTM etc.) for E3 baselines. [link](https://www.mdpi.com/1996-1073/14/3/608/htm) |
| ☐ | Review of ANN models for building energy prediction, *Sustainability* 16(17), 7805 (2024) | Recent survey, 2015–2023 studies. [link](https://www.mdpi.com/2071-1050/16/17/7805) |
| ☐ | Systematic review of building energy consumption prediction, *Applied Sciences* 15(6), 3086 (2025) | Says existing reviews under-cover deep learning. [link](https://mdpi.com/2076-3417/15/6/3086) |
| ☐ | Systematic review of deep learning and machine learning for building energy *(preprint, arXiv 2202.12269)* | Broad ML/DL overview. [link](https://arxiv.org/pdf/2202.12269) |
| ☐ | Short-term energy forecasting review, *Artificial Intelligence Review* (2023), doi:10.1007/s10462-023-10660-8 | Definitions of forecast horizons; RNNs for short term. [link](https://link.springer.com/article/10.1007/s10462-023-10660-8) |

### A2. Concept drift and model updating
| ☐ | Paper | Why read it |
|---|---|---|
| ☐ | Drift detection in whole-building electricity forecasting, *Sustainability* 14(10), 5857 (2022) | Active vs passive retraining with change detectors. [link](https://mdpi.com/2071-1050/14/10/5857) |
| ☐ | Li et al., large-scale comparison of model-update / continual-learning methods for building energy prediction (PolyU) *(venue/year to confirm)* | Concept drift + catastrophic forgetting. [link](https://ira.lib.polyu.edu.hk/bitstream/10397/108193/1/Li_Large-scale_Comparison_Demonstration.pdf) |
| ☐ | Drift-triggered retraining for 35 HVAC devices (RWTH Aachen) *(year to confirm)* | Drift-detection timing matters more than retraining frequency. [link](https://publications.rwth-aachen.de/record/1022620/files/1022620.pdf) |
| ☐ | Accuracy vs energy trade-offs of concept-drift detectors *(preprint, arXiv 2404.19452)* | KSWIN vs ADWIN vs HDDM_W; general ML. [link](https://arxiv.org/pdf/2404.19452) |

### A3. HVAC and lighting optimisation (MPC, RL)
| ☐ | Paper | Why read it |
|---|---|---|
| ☐ | Comparison of optimal control techniques for building energy management, *Frontiers in Built Environment* (2022), doi:10.3389/fbuil.2022.849754 | MPC beat RL with an equivalent problem formulation — supports our optimisation-based (SLSQP) choice. [link](https://www.frontiersin.org/articles/10.3389/fbuil.2022.849754/pdf) |
| ☐ | Data-driven MPC and RL for building energy management: a survey *(preprint, arXiv 2106.14450)* | Background on MPC/RL. [link](https://arxiv.org/pdf/2106.14450.pdf) |
| ☐ | Review of MPC and RL field demonstrations for HVAC (24 residential, 80 commercial) *(preprint, arXiv 2503.05022)* | Simulation-heavy field; little real deployment. [link](https://arxiv.org/html/2503.05022v6) |
| ☐ | Comparative field deployment of RL and MPC for residential HVAC *(preprint, arXiv 2510.01475)* | Safety and interpretability problems of RL in practice. [link](https://arxiv.org/html/2510.01475v1) |

### A4. Cyberattacks and anomaly detection in building systems
| ☐ | Paper | Why read it |
|---|---|---|
| ☐ | Cash et al., "On False Data Injection Attack against Building Automation Systems" *(preprint, arXiv 2208.02733)* | KNX temperature FDI; small false inputs → large energy cost, hard to detect from temperature alone. [link](https://arxiv.org/abs/2208.02733) |
| ☐ | Haque et al., ensemble (autoencoder + one-class SVM) intrusion detection for smart-building HVAC, COMPSAC 2021 | **Candidate published baseline for E3** (AE + OCSVM). [link](https://acyd.fiu.edu/wp-content/uploads/COMPSAC-2021_Ensemble_Haque.pdf) |
| ☐ | BIoTA: control-aware attack analytics for building IoT *(preprint, arXiv 2107.14136)* | Occupancy-driven attacks; capacity / consistency checks like our rules. [link](https://arxiv.org/pdf/2107.14136) |
| ☐ | Sensor attacks and resilient defense on HVAC for energy-market signal tracking *(preprint, arXiv 2310.15413)* | Robust control under sensor distortion. [link](https://arxiv.org/pdf/2310.15413) |
| ☐ | A cyber-resilient control framework with adaptive MPC (Drexel, 2025) | **Closest to our agent**: detection-triggered control reconfiguration; −76.3 % unmet degree-hours, +11.4 % power. [link](https://researchdiscovery.drexel.edu/esploro/outputs/journalArticle/A-cyber-resilient-control-framework-with-adaptive/991022118072104721) |
| ☐ | Attack detection in BACnet/IP networks with ML *(preprint, arXiv 2607.20686; year to confirm)* | Stealthy FDI/tampering detected only ~77 %. [link](https://arxiv.org/pdf/2607.20686) |
| ☐ | Survey of attacks and resilient control in building automation (NSF PAR) *(details to confirm)* | Taxonomy: corruption, delay, blocking; passive vs active resilience. [link](https://par.nsf.gov/servlets/purl/10482644) |
| ☐ | ML-based temperature control and security protection for smart buildings (VT showcase, 2024) | Short paper combining FDI, detection and control. [link](https://cyberinitiative.org/content/dam/cci_research_vt_edu/calls-for-proposals/research-showcase-call-for-papers-2024/A%20Machine%20Learning-Based%20Temperature%20Control%20and%20Security%20Protection%20for%20Smart%20Buildings.pdf) |

### A5. LLM agents for building energy and HVAC
| ☐ | Paper | Why read it |
|---|---|---|
| ☐ | Sawada et al., "Office-in-the-Loop: an investigation into agentic AI for advanced building HVAC control systems" (Cambridge University Press journal) | **Real-world** agentic HVAC control — key comparison. [link](https://www.cambridge.org/core/product/B6CE619AC4EBC5EB1E917F55835B4692) |
| ☐ | He and Jazizadeh, "Context-aware LLM-based AI agents for human-centered energy management systems in smart buildings" *(preprint, arXiv 2512.25055)* | LLM BEMS agent with natural-language interaction. [link](https://arxiv.org/html/2512.25055v1) |
| ☐ | Large language models for HVAC operations: a critical review *(preprint, arXiv 2609.05314)* | **Mine its reference list** for 2024–2025 journal papers. [link](https://arxiv.org/pdf/2609.05314) |
| ☐ | AutoControl: LLM agents generate PI controllers from Brick schema | LLMs for control design. [link](https://huziqi.github.io/files/AutoControl.pdf) |
| ☐ | ThermoLLM: thermodynamics-aware HVAC control with a knowledge graph *(preprint, arXiv 2606.22911)* | Multi-zone LLM HVAC control. [link](https://arxiv.org/pdf/2606.22911) |
| ☐ | Hierarchical LLM + RL for decarbonised HVAC *(preprint, arXiv 2603.26050)* | Argues LLMs alone are unreliable for closed-loop numeric optimisation — supports keeping SLSQP for optimisation and the LLM for incidents. [link](https://arxiv.org/html/2603.26050v1) |

### A6. Safety guardrails for AI agents acting in the physical world
| ☐ | Paper | Why read it |
|---|---|---|
| ☐ | RoboGuard: safety guardrails for LLM-enabled robots *(preprint, arXiv 2503.07885)* | Rule grounding + action filtering before execution. [link](https://arxiv.org/pdf/2503.07885) |
| ☐ | "From Refusal to Recovery: a control-theoretic approach to generative AI guardrails" *(preprint, arXiv 2510.13727)* | Replace unsafe actions instead of just blocking. [link](https://arxiv.org/html/2510.13727v1) |
| ☐ | Runtime action authorization for autonomous systems: a literature review *(preprint, arXiv 2606.00090)* | Final authority should be inspectable constraints outside the model — matches our deterministic validator. [link](https://arxiv.org/pdf/2606.00090) |
| ☐ | LLMs in the loop: networked control, cyber-physical and multi-agent systems survey *(preprint, arXiv 2609.16599)* | Digital-twin validation before commands reach the plant. [link](https://arxiv.org/pdf/2609.16599) |
| ☐ | Trust in LLM-controlled robotics: security threats and defenses survey *(preprint, arXiv 2601.02377)* | GuardAgent, VeriGuard, reachability filters. [link](https://arxiv.org/pdf/2601.02377) |

### A7. Streaming architectures (Kafka) for building IoT
| ☐ | Paper | Why read it |
|---|---|---|
| ☐ | End-to-end architecture for real-time IoT analytics and predictive maintenance, *Sensors* 25(9), 2945 (2025), doi:10.3390/s25092945 | Kafka + Spark + InfluxDB; maintenance, not building energy. [link](https://pmc.ncbi.nlm.nih.gov/articles/PMC12074242/) |
| ☐ | Overview of sensing and data processing technologies for smart building services, *Sustainability* 17(9), 4029 (2025) | Kafka/Storm/SAMOA smart-building architecture. [link](https://ideas.repec.org/a/gam/jsusta/v17y2025i9p4029-d1646078.html) |

**Count: 30 entries** (target 40+). Still missing: TFT/Transformer forecasters, LSTM-autoencoder attack detection on building data,
occupancy-based lighting control, carbon-aware (grid-intensity) building control, human-in-the-loop approval for AI actions,
and explainability (SLM/LLM explanations) for operators.

---

## B. Gap table (draft — fill after reading)

✓ = covered, ✗ = not covered, ? = check in the full text. Our row is filled from the repo.

| Work | Real-time streaming | Forecasting | Optimisation | Attack / drift detection | AI agent responds | Deterministic safety layer | Human approval |
|---|---|---|---|---|---|---|---|
| Candanedo 2017 | ✗ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |
| Frontiers 2022 (MPC vs RL) | ✗ | ? | ✓ | ✗ | ✗ | ✗ | ✗ |
| Cash et al. (KNX FDI) | ? | ✗ | ✗ | ✓ | ✗ | ✗ | ✗ |
| Haque et al. 2021 (AE+OCSVM) | ? | ✗ | ✗ | ✓ | ✗ | ✗ | ✗ |
| Drexel 2025 (adaptive MPC) | ? | ? | ✓ | ✓ | ✗ (rule-based reconfiguration) | ? | ✗ |
| Office-in-the-Loop | ? | ? | ✓ | ✗ | ✓ | ? | ? |
| He & Jazizadeh (LLM BEMS) | ? | ? | ✗ | ✗ | ✓ | ? | ✓ (user dialogue) |
| RoboGuard (robots) | n/a | ✗ | ✗ | ✗ | ✓ | ✓ | ? |
| Sensors 2025 (Kafka IoT) | ✓ | ✓ | ✗ | ? | ✗ | ✗ | ✗ |
| **This work** | ✓ Kafka | ✓ XGBoost | ✓ SLSQP | ✓ rules + IF + KS/PSI | ✓ LLM or playbook (LangGraph) | ✓ validator, rate limit | ✓ approval gate |

---

## C. Gaps

### C1. Gaps in the literature (candidate contributions)
1. **No end-to-end system.** The papers above each cover one or two columns of the gap table; none found combines
   streaming, forecasting, optimisation, attack/drift detection, an AI agent and a deterministic safety layer.
2. **LLM building agents trust their inputs.** The LLM HVAC/BEMS agents are evaluated on comfort and energy, not on
   manipulated sensor data. What an agent does when the readings themselves are attacked is not studied.
3. **Guardrails are studied for robots, not buildings.** Action validation (RoboGuard, runtime authorization) has not
   been applied to building control with *measured* counts of blocked, corrected and allowed actions.
4. **Attack response is rule-based reconfiguration.** Cyber-resilient building control (e.g. adaptive MPC) switches
   control modes; it does not diagnose incidents, explain them, or ask an operator before acting.
5. **Detection is evaluated offline.** Building attack-detection papers mostly use static datasets; streaming
   evaluation with detection latency and end-to-end delay is rare.
6. **Season shift breaks security detectors.** Drift work targets forecasting accuracy. We measured that detectors
   trained on one season raise thousands of false spoofing alarms in other seasons, and that multi-season training
   with month-matched drift references removes them (`README.md`, *Fix: multi-season detectors*). Check whether
   anyone has reported this for building security detectors.
7. **Energy cost of attacks and of the response.** Few papers quantify both what an attack costs in energy/comfort
   and what the defensive response costs (Drexel: +11.4 % power is one of the few numbers).

### C2. Gaps in our own project that reviewers will find (fix or state openly)
1. **The agent has never run with an LLM** — only the offline playbook (`docs/results/agent_demo.md`). → E1.
2. **The optimiser ignores the ML forecast in practice**: the forecast shifts the objective by a constant, so
   decisions are identical with or without it (`docs/PROGRESS.md`, open issue 2; `optimization_comparison.md` shows
   identical measured- and ML-basis results in winter). The "forecasting → optimisation" claim needs a fix or rewording.
3. **One real dataset, one house** (UCI/KAG); the building and the carbon profile are synthetic. → E2.
4. **No published baselines** for forecasting (LSTM/TFT, seasonal naive) or detection (e.g. Haque et al. AE + OCSVM). → E3.
5. **Single seed per case** in the optimisation comparison. → E4.
6. **Lighting trade-off**: under-lit readings rise (winter 18 → 148, summer 1 → 85). → E4.
7. **Self-designed attacks only**; no attacker who knows the detectors (stealthy attacks within the learned ranges).
8. **Spoofing recall trade-off**: 0.78 of spoofed readings in held-out test week 1 after the multi-season fix
   (all attacks still detected at their first reading).
9. **No real-building or hardware-in-the-loop validation** (Office-in-the-Loop has it). State as a limitation.
10. **KAG forecasting**: Linear Regression beats XGBoost on the test split (0.559 vs 0.512 R²) — report honestly;
    Candanedo 2017 reports ≈ 0.57 with gradient boosting under a different protocol.

### C3. Your additions
- 
