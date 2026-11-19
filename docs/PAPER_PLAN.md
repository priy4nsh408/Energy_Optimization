# Research Paper Plan — Energy_Optimization

> **How to use this file:** paste it into the Claude chat for the `Energy_Optimization` repo. Ask that chat to save it as `docs/PAPER_PLAN.md`, then work through the timeline one week at a time ("Start Week 1 of PAPER_PLAN.md").

---

## 0. Instructions for the assistant working on this repo

You are helping me turn this repo into a research paper for a **Q1/Q2 journal**. The target is to **submit by 31 December 2026**.

Rules that apply to every task in this plan:
1. **Never make up a number.** Every figure in the paper must come from a report produced by a script in this repo, such as those under `docs/results/`. If a result doesn't exist yet, write `TODO(result)` instead.
2. **Keep everything reproducible.** Each experiment needs one command, a fixed seed, and saved output (markdown + JSON) under `docs/results/paper/`.
3. **Report negative results honestly.** For example, on the KAG data Linear Regression beats XGBoost on the test split, and the optimiser increases under-lit readings. Discuss these openly; don't hide them.
4. **State what is synthetic.** The building and carbon profile are simulated (`MockCarbonProvider`). `test_energy_data.csv` is an exact linear formula, so **leave it out of the paper**.
5. At the end of each week, update `docs/PAPER_PLAN.md`: tick off completed items, link the result files, and note anything that is blocked.
6. Work on a branch per week (e.g. `paper/week-1`) and keep commits small.

---

## 1. Working title and claim

**Working title:** *Safety-Validated Agentic AI for Cyber-Resilient Smart-Building Energy Management on a Real-Time Streaming Architecture*

**Main claim (one sentence):** An LLM-based agent, whose every action must pass a deterministic safety validator, can respond to sensor attacks and data drift in a live building-energy system. It does this without losing the energy and comfort gains of the optimiser, and without any unsafe action reaching the building.

### Research questions
- **RQ1 — Forecasting:** How well do ML models forecast short-term building energy use on real and simulated data, compared with naive baselines?
- **RQ2 — Optimisation:** How much energy, CO₂e and peak demand does closed-loop HVAC and lighting optimisation save, and what does it cost in comfort and lighting?
- **RQ3 — Security:** How accurately are sensor attacks (spoofing, replay, abnormal power, occupancy manipulation) and drift detected while the system is streaming?
- **RQ4 — Agent and safety:** Compared with a rule-based playbook, how does an LLM agent perform on incident handling? And how many of its proposed actions does the safety layer block or correct?

### Contributions (draft; revise once results are in)
1. An end-to-end, reproducible architecture: Kafka streaming → ML forecasting → optimisation → security detection → agentic response.
2. A safety layer that every AI action must pass, with measured counts of blocked and allowed actions.
3. Attack-injection experiments on real data (the public UCI Appliances energy dataset, called "KAG" in this repo) and on a simulated multi-zone building.
4. An ablation study showing what each module adds.
5. Open-source code, plus scripts that regenerate every table in the paper.

---

## 2. Results already in the repo (starting point)

| Area | Result | Source |
|---|---|---|
| Forecasting (simulated building) | XGBoost test MAE 0.116 kW vs persistence 0.158 kW | `docs/results/energy_prediction_building.md` |
| Forecasting (KAG, real data) | Linear Regression R² 0.559 > XGBoost 0.512 > persistence 0.461 on the test split | `docs/results/energy_prediction_kag.md` |
| Forecasting (customer cost) | XGBoost R² 0.609 ± 0.016 (5-fold CV repeated 10 times) | `docs/results/energy_prediction_customer_energy_cost.md` |
| Optimisation (winter week) | −7.4% energy, −6.5% CO₂e, −3.8% peak; comfort 88.7% → 93.1%; but under-lit readings 18 → 148 | `docs/results/optimization_comparison.md` |
| Attack detection (KAG, real data) | Precision 0.674, Recall 0.833, F1 0.745, false-positive rate 1.09% | `docs/results/kag_anomaly_detection.md` |
| Agent | **Offline rule-based playbook only; no LLM has been run yet** | `docs/results/agent_demo*.md` |

**Main gaps:** the agent has never been run with an LLM; there is only one real dataset; there is no comparison with published methods; the simulations use a single seed; there is no ablation study.

---

## 3. Experiment plan

### E1 — LLM agent vs rule-based playbook (RQ4) — *most important*
- Run `agent.demo` with an LLM provider, using the same seed and the same injected attacks as the offline run.
- Compare these setups: offline playbook, LLM, LLM without the safety layer (in a sandbox only, logging what *would* have run), and no agent.
- **Metrics:** incidents correctly diagnosed, time to contain, actions proposed / blocked / changed by the safety layer, unsafe actions that would have run without the safety layer, energy and comfort impact, number of tool calls, and LLM latency and cost.
- Use at least 5 seeds. Report the mean ± std.

### E2 — A second real dataset (RQ1, RQ3)
- Add one public building dataset. Candidates: ASHRAE Great Energy Predictor III, the Building Data Genome 2, or a public IoT/ICS attack dataset with building sensors.
- Rerun the forecasting and attack-injection experiments on it with the same protocol as KAG (time-ordered split, tune on validation only, test once).

### E3 — Comparison with published methods (RQ1, RQ3)
- **Forecasting:** add an LSTM or Temporal Fusion Transformer model and a seasonal-naive baseline.
- **Detection:** add at least 2 published methods (e.g. LSTM autoencoder, One-Class SVM, or a recent paper's method) and run them on the same splits.

### E4 — Statistical strength (RQ2)
- Run the optimisation comparison for 10 seeds × 4 seasons (winter, spring, summer, autumn).
- Report 95% confidence intervals and a paired test (Wilcoxon) between baseline and optimised runs.
- Fix or justify the rise in under-lit readings (e.g. a lighting comfort constraint, or a sensitivity analysis on the objective weights).

### E5 — Ablation study (all RQs)
Remove one piece at a time: safety layer, attack detector, drift detector, optimiser, and ML forecast (use measured data instead). Report how energy, comfort, security and safety metrics change.

### E6 — Streaming performance
Use `experiments/streaming_latency.py` to report end-to-end latency (p50 and p95) and throughput as the number of zones grows (5 → 20 → 50).

---

## 4. Paper structure (journal article, about 8,000–10,000 words)

1. **Abstract** (≈250 words): problem, method, 3–4 key numbers, and why it matters.
2. **Introduction**: what drives building energy use; cyber risk in building management systems; why autonomous AI agents need safety guarantees; the contributions list; how the paper is organised.
3. **Related work** (35–50 references, mostly from 2021–2026):
   - 3.1 Building energy forecasting
   - 3.2 HVAC and lighting optimisation (MPC, reinforcement learning, SLSQP)
   - 3.3 Cyberattacks and anomaly detection in building systems
   - 3.4 LLM agents for energy and control, and AI safety guardrails
   - 3.5 Gap table: rows are prior works, columns are streaming / forecasting / optimisation / security / agent / safety layer.
4. **System architecture**: the Kafka topology diagram, how workers divide responsibilities, the control modes (advisory vs closed loop), and the data flow.
5. **Methodology**
   - 5.1 Building simulator (thermal model, occupancy, weather)
   - 5.2 Forecasting models and features
   - 5.3 Optimisation formulation (objective, constraints, weights)
   - 5.4 Attack models and detectors (Isolation Forest, CUSUM, drift monitors)
   - 5.5 Agent design: LangGraph flow, tools, prompts
   - 5.6 Safety layer: the formal rules and how actions are isolated or recovered
6. **Experimental setup**: datasets table, splits, metrics, hardware, seeds, how to reproduce.
7. **Results**: one subsection per research question (E1–E6), each with tables and figures.
8. **Discussion**: findings, trade-offs (energy vs lighting), negative results, implications for deployment.
9. **Threats to validity / limitations**: simulation realism, synthetic carbon data, a single real house, LLM non-determinism.
10. **Conclusion and future work**
11. **Code and data availability statement**
12. **References**

### Figures and tables checklist
- [ ] Fig. 1: architecture diagram
- [ ] Fig. 2: agent and safety-layer decision flow
- [ ] Fig. 3: forecast vs actual (one real and one simulated dataset)
- [ ] Fig. 4: energy and comfort over a week, baseline vs optimised
- [ ] Fig. 5: attack timeline with detections marked
- [ ] Fig. 6: ablation bar chart
- [ ] Table 1: gap analysis vs related work
- [ ] Table 2: datasets
- [ ] Table 3: forecasting results
- [ ] Table 4: optimisation results with confidence intervals
- [ ] Table 5: detection results per attack type
- [ ] Table 6: agent comparison (offline vs LLM, with and without the safety layer)
- [ ] Table 7: latency and scalability

---

## 5. Timeline (today is 19 Nov 2026; submission target is 31 Dec 2026)

| Week | Dates | Tasks | Done when |
|---|---|---|---|
| **1** | 20–26 Nov | Literature review (collect 40+ papers, build the gap table); choose the target journal and download its template; choose the second dataset; set up the `paper/` folder (LaTeX) and `docs/results/paper/` | Gap table drafted; journal chosen; dataset downloaded; LaTeX skeleton compiles |
| **2** | 27 Nov – 3 Dec | **E1** LLM agent experiments (all setups, 5 seeds) | Table 6 produced from script output |
| **3** | 4–10 Dec | **E2** second dataset + **E3** published-method baselines | Tables 3 and 5 updated with the new dataset and baselines |
| **4** | 11–17 Dec | **E4** multi-seed, multi-season runs + statistics; **E5** ablation; **E6** latency | Tables 4 and 7 and Fig. 6 produced; lighting trade-off fixed or explained |
| **5** | 18–24 Dec | Write the full draft: methods and results first, then introduction and related work, then abstract | Complete draft with every `TODO(result)` resolved |
| **6** | 25–31 Dec | Edit language and figures; check plagiarism and similarity; format for the journal; cover letter; highlights; graphical abstract; submit | Submission confirmation received |

**Buffer:** if a week slips, cut scope in this order: E6 → the second published-method baseline → seasons in E4 (keep winter and summer). **Never cut E1 or E5.**

---

## 6. Target journals (check current quartiles on scimagojr.com before choosing)

| Journal | Fit | Notes |
|---|---|---|
| Energy and Buildings (Elsevier) | Very high | Core venue for building energy |
| Journal of Building Engineering (Elsevier) | High | Welcomes systems-oriented papers |
| Energy and AI (Elsevier) | High | AI focus; open access |
| Sustainable Cities and Society (Elsevier) | Medium–high | Emphasise the smart-city and carbon angle |
| Energies (MDPI) | Medium | Faster review; check the APC (publication fee) |
| IEEE Access | Medium | Fast; multidisciplinary |

---

## 7. Submission checklist
- [ ] Every number traced to a result file (keep a table mapping each paper number to its source file)
- [ ] Code tagged with a release (e.g. `v1.0-paper`) and archived on Zenodo for a DOI
- [ ] README section on how to reproduce the paper
- [ ] Use of LLMs/AI tools disclosed according to the journal's policy
- [ ] Author contributions, funding, conflict-of-interest and data-availability statements
- [ ] Cover letter, highlights (3–5 bullets of 85 characters or fewer), graphical abstract
- [ ] 3–5 suggested reviewers (not co-authors, no conflict of interest)

---

## 8. Progress log

### 19 Nov 2026 — plan added; section 2 checked against the repo
- Every number in section 2 matches its source file (`energy_prediction_building.md`, `energy_prediction_kag.md`,
  `energy_prediction_customer_energy_cost.md`, `optimization_comparison.md` winter week, `kag_anomaly_detection.md`,
  `agent_demo.md` = offline playbook only).
- Results that exist but are not yet in section 2:
  - `optimization_comparison.md` also has a **summer week** (seed 102): −12.5 % energy, −12.1 % CO₂e, −11.9 % peak,
    comfort 85.7 % → 95.7 %, under-lit readings 1 → 85.
  - `security_evaluation.md` (simulated building, held-out test week 2): regenerated with the multi-season detectors
    (precision 0.883, recall 0.988, F1 0.933, false-positive rate 0.23 %, gradual attacks 2 of 2). Earlier
    January-only detectors: precision 0.310 at the same recall. Both are documented in the README.
- Note for 5.4: CUSUM is used only in the KAG experiment (`experiments/kag_anomaly_experiment.py`), not in the
  live security service.
- Branches: this session can push only to `claude/nifty-brown-uckrlo`; per-week `paper/week-N` branches need
  the owner's go-ahead (or the work stays on that branch with one commit per task).
