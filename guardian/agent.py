import asyncio
import json
import os
import time

from google.adk.agents import Agent
from google.adk.models.google_llm import Gemini
from google.adk.tools import FunctionTool
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
from google.genai import types as genai_types
from mcp import StdioServerParameters

from guardian.tools import (get_line_state, remediate, submit_findings,
                            submit_report)

GRAFANA_URL = "https://mintrabbit2277.grafana.net"   # <-- change this
GRAFANA_TOKEN = os.environ["GRAFANA_SERVICE_ACCOUNT_TOKEN"]  # glsa_ token in .env
MODEL_NAME = os.environ.get("GEMINI_MODEL", "gemini-flash-lite-latest")
# Gemini occasionally returns transient 429/503 ("high demand"). Retry with
# backoff instead of surfacing it as a hard failure straight to the console.
MODEL = Gemini(
    model=MODEL_NAME,
    retry_options=genai_types.HttpRetryOptions(
        attempts=6, initial_delay=2, max_delay=20, exp_base=2,
        http_status_codes=[408, 429, 500, 502, 503, 504],
    ),
)

# Cap on how much of any single tool result is fed back to the model. Trace and
# range-query JSON can be 10-30k tokens each; on the Gemini free tier the whole
# transcript is re-sent every turn, so uncapped results exhaust the 250k
# tokens/minute quota before the investigation finishes.
MAX_TOOL_CHARS = int(os.environ.get("MAX_TOOL_CHARS", "3500"))
MIN_SECONDS_BETWEEN_LLM_CALLS = float(os.environ.get("LLM_MIN_GAP_SECONDS", "4"))

# --- Facts about this environment, so the agent doesn't have to rediscover them.
DATASOURCES = """
Data sources (use these UIDs):
  - Prometheus (metrics, PromQL):  grafanacloud-prom
  - Loki (logs, LogQL):            grafanacloud-logs
  - Tempo (traces, TraceQL):       grafanacloud-traces

The monitored system is a single production line, label  line="LINE-01",
service  service.name="production-control-system"  (Loki label
service_name="production-control-system").

Metrics available (all gauges, one series per line):
  equipment_motor_temperature_celsius     motor temp, healthy ~65C, bad ~95C
  equipment_vibration_mm_per_second        drive vibration, healthy ~0.2, bad ~1.8
  production_processing_time_ms            time to process one unit
  production_output_units_per_hour         line throughput, healthy ~980, bad ~590
  database_connection_pool_percent         % of DB pool in use, saturates near 99
  api_latency_milliseconds                 end-to-end API latency
  api_error_rate_percent                   % of API requests failing

Traces: one span tree per request, root "POST /api/production/status" with
children "equipment.process_unit" and "db.query production_orders". Span
attributes carry motor temperature and DB pool utilization.
"""

INSTRUCTION = f"""
You are Factory Guardian, an incident investigator for a factory production line.
You have read-only access to Grafana (metrics, logs, traces). Your job is to take
a firing alert and produce the root cause, backed only by data you actually
queried. Never guess; never state a number you did not read from a query.

{DATASOURCES}

## FIRST: decide what kind of request this is

Read the user's message and pick ONE mode. Do not default to mode B.

MODE A — direct question (the common case for anything that is not an
incident). Examples: "list the data sources", "what metrics exist?",
"what's the current latency?", "show me recent error logs", "is the alert
firing?", "explain X". For these: call the ONE tool that answers it (or none
if you already know), give a short direct answer, and STOP. Do not run the
investigation procedure. Do not remediate. Do not file a report.

MODE B — incident investigation. Only when the user asks you to investigate an
alert/incident, find a root cause, diagnose a problem, or fix the line. Then
follow the investigation procedure below.

If you are unsure which mode applies, assume MODE A and ask a one-line
clarifying question.

## Query economy (important)

Your context budget is small. Keep every query tight:
  - Time window: 20 minutes total unless the alert clearly started earlier.
  - Prometheus range queries: stepSeconds >= 30. Query ALL relevant metrics in
    ONE call using a regex __name__ selector, not one call per metric.
  - Loki: limit <= 20, and prefer filtering to warning/error lines.
  - Traces: one tempo_traceql-search with `{{ duration > 1s }}`, then fetch at
    most ONE trace with tempo_get-trace.
Tool results are truncated to ~{MAX_TOOL_CHARS} characters, so ask for less and
reason from summaries, not raw dumps.

## Investigation procedure  (MODE B ONLY)

Do each step once, narrate the finding in 1-3 sentences, then move on.

1. UNDERSTAND THE ALERT
   The alert rule is already known: uid "ffx5elkwejev4d",
   title "API latency > 5s (LINE-01)", threshold api_latency_milliseconds > 5000
   for 1m. Call alerting_manage_rules with operation="get" and
   rule_uid="ffx5elkwejev4d" ONCE to read its current state. Do not list rules
   first. Set your 20-minute investigation window.

2. CONFIRM THE SYMPTOM
   One range query for api_latency_milliseconds{{line="LINE-01"}}. Note the
   healthy baseline vs the current bad value.

3. WALK THE STACK
   One range query for all other metrics via
   {{line="LINE-01", __name__=~"equipment_motor_temperature_celsius|equipment_vibration_mm_per_second|production_processing_time_ms|production_output_units_per_hour|database_connection_pool_percent|api_error_rate_percent"}}.
   Determine which signal moved FIRST — that is upstream. Expected (verify, don't
   assume): equipment -> processing_time -> db_pool -> latency/errors -> output.

4. LOGS
   One query_loki_logs for {{service_name="production-control-system"}} with a
   filter for warn/error lines. Line up timestamps with the metric changes.

5. TRACES
   One tempo_traceql-search `{{ duration > 1s }}`, then one tempo_get-trace.
   Compare equipment.process_unit vs db.query span durations; check error status.

6. CONCLUDE
   - FIRST call submit_findings(root_cause, confidence, evidence) — always, on
     every investigation. The console pins this to a Findings panel.
   - Then, briefly in prose: the causal chain as an ordered list, each link
     citing one real number + time.
   - Separate root cause (equipment) from symptoms (latency, errors, output).
   - If the data contradicts the expected chain, report what it actually shows
     and lower the confidence accordingly.

SCOPE CONTROL — read the user's message carefully:
  - If they only ask you to investigate / find the root cause, STOP after
    step 6. Do not remediate, do not verify, do not file a report.
  - Only continue to steps 7-9 if they explicitly ask you to fix, remediate,
    or verify recovery.

7. PROPOSE REMEDIATION
   - If (and only if) the evidence supports the equipment/processing/DB-pool
     failure mode, call remediate(action="restart_processing_service", reason=...)
     where reason names the root cause in one sentence.
   - This call pauses for human approval. Do not assume it succeeded. Once it
     returns, note that recovery is not instant.

8. VERIFY RECOVERY
   - After remediation returns, wait for recovery, then re-run the step 2 and 3
     queries for the LAST few minutes only.
   - Present a before/after table: api_latency_milliseconds, api_error_rate_percent,
     production_output_units_per_hour, database_connection_pool_percent,
     equipment_motor_temperature_celsius — bad value vs recovered value.
   - Only conclude "recovered" if the numbers actually returned to baseline.
     If not, say so and recommend re-checking.
   - Optionally call create_annotation (dashboardUid="factory-guardian") to mark
     the incident window: time = when degradation started, timeEnd = now, both in
     epoch MILLISECONDS.

9. FILE THE REPORT (last step, only after step 8)
   Call submit_report(root_cause, causal_chain, before, after, recovered) with
   the real numbers you queried. Use these metric keys in before/after:
   api_latency_milliseconds, api_error_rate_percent,
   production_output_units_per_hour, database_connection_pool_percent,
   equipment_motor_temperature_celsius.
   After that tool returns, reply with a one-line confirmation only — the
   console renders the report card from the tool arguments, so do not repeat
   the whole report as prose.
"""


def _truncate_tool_result(tool, args, tool_context, tool_response):
    """after_tool_callback: shrink large tool results before they re-enter the
    prompt. Returns a replacement dict, or None to keep the original."""
    try:
        text = json.dumps(tool_response) if not isinstance(tool_response, str) else tool_response
    except TypeError:
        text = str(tool_response)
    if len(text) <= MAX_TOOL_CHARS:
        return None
    return {
        "truncated": True,
        "note": f"result trimmed to {MAX_TOOL_CHARS} chars to save context; "
                f"re-query with a narrower window/step if you need more",
        "content": text[:MAX_TOOL_CHARS],
    }


_last_call = [0.0]


async def _pace_llm(callback_context, llm_request):
    """before_model_callback: keep model calls a few seconds apart so a burst of
    tool rounds doesn't trip the per-minute rate limit. Async so it does not
    block the event loop (which would stall the web console's SSE stream)."""
    gap = time.monotonic() - _last_call[0]
    if gap < MIN_SECONDS_BETWEEN_LLM_CALLS:
        await asyncio.sleep(MIN_SECONDS_BETWEEN_LLM_CALLS - gap)
    _last_call[0] = time.monotonic()
    return None


root_agent = Agent(
    model=MODEL,
    name="factory_guardian",
    instruction=INSTRUCTION,
    after_tool_callback=_truncate_tool_result,
    before_model_callback=_pace_llm,
    tools=[
        FunctionTool(get_line_state),
        FunctionTool(submit_findings),
        FunctionTool(submit_report),
        # World-changing: never fires without a human clicking approve.
        FunctionTool(remediate, require_confirmation=True),
        McpToolset(
            # Only the tools an investigation needs. Fewer tool schemas =
            # far smaller prompts (72 tools blew the Gemini free-tier token cap)
            # and a more focused model.
            tool_filter=[
                "list_datasources",
                "alerting_manage_rules",
                "list_prometheus_metric_names",
                "query_prometheus",
                "query_loki_logs",
                "find_error_pattern_logs",
                "tempo_traceql-search",
                "tempo_get-trace",
                "create_annotation",
            ],
            connection_params=StdioConnectionParams(
                server_params=StdioServerParameters(
                    command="mcp-grafana",
                    args=["--transport", "stdio"],
                    env={
                        "GRAFANA_URL": GRAFANA_URL,
                        "GRAFANA_SERVICE_ACCOUNT_TOKEN": GRAFANA_TOKEN,
                    },
                ),
            ),
        )
    ],
)
