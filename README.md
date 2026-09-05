# Factory Guardian

**An AI incident investigator for a factory production line.**

When a Grafana alert fires, Factory Guardian does what a good SRE does: reads the
alert, queries **metrics, logs and traces**, works out which signal moved
*first*, names the root cause, proposes a fix, **waits for a human to approve
it**, and then re-queries the data to prove the line actually recovered.

Built on [Google ADK](https://google.github.io/adk-docs/) + Gemini, with the
[Grafana MCP server](https://github.com/grafana/mcp-grafana) as the agent's tools.

---

## The problem

In a real incident the alert fires on a **symptom**. The symptom is never the
cause. An engineer spends the first ten minutes clicking between three Grafana
tabs trying to work out which signal moved first — because that one is upstream
of everything else.

This project automates that correlation step, with evidence.

## The scenario

A production line where equipment degradation is the hidden root cause and
everything visible is downstream of it:

```
equipment degrades  (motor 65°C → 97°C, vibration 0.2 → 1.8 mm/s)
  → unit processing time rises        (120 ms → 1,080 ms)
  → requests retry
  → DB connection pool fills          (40% → 100%)
  → API latency spikes, non-linearly  (200 ms → 6,800 ms)   ← the alert fires HERE
  → error rate climbs                 (0.2% → 14.7%)
  → production output falls           (980 → 577 units/hr)
```

The alert fires on API latency. The cause is a motor, four layers upstream.
That gap is the whole demo.

The simulator models **one** hidden variable and *derives* every signal from it,
so the chain the agent discovers is a real relationship rather than a script.

---

## Architecture

```
        ┌────────────────────────────────┐
  you → │ operator console (app.py :8080)│
        │  live panels + agent chat      │
        │  + human approval              │
        └───────────────┬────────────────┘
                        │ InMemoryRunner (in-process)
                        ▼
        ┌────────────────────────────────┐
        │ factory_guardian  (Google ADK)  │
        │ Gemini + investigation procedure│
        └───┬────────────────────────┬───┘
  remediate │ (human-gated)          │ McpToolset over stdio
            ▼                        ▼
      simulator.py  ── OTLP ──►  mcp-grafana ──► Grafana Cloud
     (the fake factory)          (local binary)  Mimir · Loki · Tempo
```

| File | Role |
|---|---|
| `simulator.py` | FastAPI fake factory; `POST /incident`, `POST /remediate`, `GET /state`, `GET /logs` |
| `telemetry.py` | OpenTelemetry wiring — metrics→Mimir, logs→Loki, traces→Tempo |
| `guardian/agent.py` | The ADK agent: mode routing, investigation procedure, filtered toolset, token/retry safeguards |
| `guardian/tools.py` | `remediate()` (approval-gated), `submit_findings()`, `submit_report()`, `get_line_state()` |
| `app.py` | Operator console: process flow, metric panels, trace waterfall, log stream, agent chat, approval + report cards |
| `scripts/build_dashboard.py` | Build & push the Grafana dashboard (`dashboard.json`) |
| `scripts/build_alert.py` | Create the "API latency > 5s" alert rule |
| `scripts/ask.py` | Run one investigation from the CLI |

---

## How the agent investigates

It routes first, then executes.

- **Mode A — direct question** ("list the data sources", "is the alert firing?"):
  one tool, short answer, stop.
- **Mode B — incident investigation**:

```
1. Read the alert            → threshold, state, time window
2. Confirm the symptom       → latency baseline vs current
3. Walk the stack            → ONE combined query for the other six metrics;
                               determine which moved FIRST = upstream
4. Logs                      → Loki, warn/error lines, line up timestamps
5. Traces                    → Tempo; compare span durations
6. Conclude                  → submit_findings(root_cause, confidence, evidence)
   ── stops here unless you ask it to fix ──
7. Propose remediation       → remediate(...)  ⟵ PAUSES FOR HUMAN APPROVAL
8. Verify recovery           → re-query, before/after comparison
9. File the report           → submit_report(...)
```

### The approval gate

`remediate` is registered as `FunctionTool(remediate, require_confirmation=True)`.
The check lives inside ADK's `FunctionTool.run_async` **before** the function is
invoked, so it is structural — no prompt can reach past it.

Verified empirically: calling `remediate` without confirmation during a live
incident returns `{'error': 'This tool call requires confirmation'}` and the
simulator's `incident` flag stays `True`. Nothing executes.

---

## Setup

Requires Python 3.12, [`mcp-grafana`](https://github.com/grafana/mcp-grafana)
(`brew install mcp-grafana`), a Grafana Cloud stack, and a Gemini API key.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt          # google-adk[mcp], otel, fastapi, ...

cp .env.example .env                      # then fill it in
```

`.env` notes — each of these cost real debugging time:

- `GRAFANA_SERVICE_ACCOUNT_TOKEN` — a `glsa_` token. **Admin** role is needed to
  provision the alert rule; querying only needs Viewer.
- `OTEL_EXPORTER_OTLP_HEADERS` must be
  `Authorization=Basic%20<base64(instanceID:token)>`. A literal space after
  `Basic` breaks it, and so does omitting the `Authorization=` key — metrics and
  logs are then silently dropped.
- **Quote any value containing `&`**, or `source .env` breaks and the app starts
  without `GOOGLE_API_KEY`.
- The agent uses the **local** `mcp-grafana` over stdio, not the hosted
  `mcp.grafana.com` gateway — that gateway rejected both token types with
  `could not determine backend for token`.

One-time provisioning:

```bash
set -a && source .env && set +a
PATH=/opt/homebrew/bin:$PATH PYTHONPATH=$PWD python scripts/build_dashboard.py
PATH=/opt/homebrew/bin:$PATH PYTHONPATH=$PWD python scripts/build_alert.py
```

## Run

```bash
source .venv/bin/activate && set -a && source .env && set +a

# 1) the fake factory  — no --reload: it watches the repo and would reset the incident
uvicorn simulator:app --port 8000

# 2) the operator console
PATH=/opt/homebrew/bin:$PATH uvicorn app:app --port 8080
#    → open http://localhost:8080
```

**Demo path:** **Trigger incident** → watch the process flow go red left-to-right
→ the Grafana alert fires (~85–90 s: 25 s ramp + 60 s pending) → **Investigate**
→ read the Findings panel → **Propose a fix** → **Approve** → Recovery Verified.

Prefer a terminal? `PYTHONPATH=$PWD python scripts/ask.py`
Prefer ADK's own UI? `adk web --port 8001` (agent + native approval, no panels).

---

## Results

**It finds the root cause unaided.** Given only "the latency alert is firing":

> Drive motor overheating on LINE-01 degraded unit processing, saturating the
> database connection pool and breaching the API latency SLO.

with an ordered causal chain, each link citing a queried number and timestamp,
and root cause explicitly separated from symptoms.

**It proves recovery instead of claiming it:**

| Metric | Incident peak | After remediation |
|---|---|---|
| API latency | 6,834 ms | 199 ms |
| API error rate | 14.8 % | 0.20 % |
| DB pool in use | 100 % | 39.1 % |
| Motor temperature | 97.8 °C | 66.7 °C |
| Throughput | 575 /hr | 966 /hr |

**It uses all three signals, visibly** — PromQL panels, a live Loki log stream,
and a trace waterfall where `db.query production_orders` consumes 5,528 ms of a
6,588 ms request.

**It runs on a free tier** — a ~10-call agentic investigation completes inside
Gemini's 250k tokens/minute cap.

---

## Notes

**Gemini free tier.** Two separate walls, two separate fixes:
- *Token cap* (250k input tokens/min) — an agent re-sends its transcript every
  turn and raw trace JSON is 10–30k tokens per result. Handled by an
  `after_tool_callback` that truncates results to `MAX_TOOL_CHARS`, a 9-tool
  filter, and `LLM_MIN_GAP_SECONDS` pacing.
- *Transient `503 UNAVAILABLE`* — capacity, not quota. Handled by
  `retry_options` on the `Gemini` model object.

Enabling billing removes both; the project runs without it.

**Model.** `GEMINI_MODEL` env var, default `gemini-flash-lite-latest`.

**Why the console draws its own panels.** Grafana Cloud sends
`Content-Security-Policy: frame-ancestors 'none'` — its dashboards cannot be
embedded in any external page, public dashboards included. The console's panels
are fed by the simulator for display; the **agent** always queries the real
Grafana. The "Grafana ↗" link opens the real dashboard.

## Known limitations

- The alert metrics currently sit below the fold on a laptop viewport; needs a
  layout compaction pass.
- Trace bar colours are overloaded — the root bar uses a continuous severity
  scale while child bars use a categorical one.
- The simulator models a single failure mode. The agent is instructed to verify
  rather than assume the causal order, but has not been tested against a
  scenario that contradicts it.
- The trace waterfall is derived from live metrics using the same formula
  `telemetry.py` uses to build the real spans — accurate, but not read back
  from Tempo.

## License

MIT — see [LICENSE](LICENSE).
