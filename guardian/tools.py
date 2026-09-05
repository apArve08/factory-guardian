"""Action tools for Factory Guardian (Stage 6).

Only `remediate` changes the world, and it is wired into the agent with
`require_confirmation=True` so it never fires without a human approving.
"""

import os

import httpx

SIMULATOR_URL = os.environ.get("SIMULATOR_URL", "http://localhost:8000")


def remediate(action: str, reason: str) -> dict:
    """Execute a remediation action on the production line.

    Call this ONLY after you have identified a root cause and can justify the
    action. The call pauses for human approval before anything happens.

    Args:
        action: The remediation to perform. Supported:
            "restart_processing_service" — restarts the unit-processing service,
            which clears the degradation cascade. Use this for the
            equipment/processing/DB-pool failure mode.
        reason: One sentence: the root cause and why this action addresses it.

    Returns:
        The simulator's response, including a note that recovery is not
        instant and telemetry must be re-checked before concluding.
    """
    r = httpx.post(
        f"{SIMULATOR_URL}/remediate",
        params={"action": action},
        timeout=10,
    )
    r.raise_for_status()
    return {"requested_action": action, "reason": reason, "result": r.json()}


def submit_findings(
    root_cause: str,
    confidence: str,
    evidence: list[str],
) -> dict:
    """File the investigation conclusion. Call this at the END of step 6, on
    EVERY investigation, before you write your prose summary. The console pins
    it to a Findings panel so the conclusion has a fixed place to land.

    Args:
        root_cause: One sentence naming the root cause (not a symptom).
        confidence: "high", "medium" or "low", based on how well the queried
            data supports the chain.
        evidence: 2-5 short bullets, each citing one real queried number, e.g.
            ["motor temp 65C -> 97.4C at 14:31", "DB pool 40% -> 100% at 14:32"].

    Returns:
        Acknowledgement that the findings were filed.
    """
    return {
        "filed": True,
        "root_cause": root_cause,
        "confidence": confidence,
        "evidence": evidence,
    }


def submit_report(
    root_cause: str,
    causal_chain: list[str],
    before: dict,
    after: dict,
    recovered: bool,
) -> dict:
    """File the final structured incident report. Call this LAST, after you have
    verified recovery, so the operator console can render a report card.

    Args:
        root_cause: One sentence naming the root cause (not a symptom).
        causal_chain: Ordered steps, each citing a real queried number, e.g.
            ["motor temp rose 65C -> 97C", "cycle time 120ms -> 1080ms", ...].
        before: Metric name -> value at the worst point of the incident, e.g.
            {"api_latency_milliseconds": 6849, "api_error_rate_percent": 14.7}.
        after: The same metric names -> value after recovery.
        recovered: True if the LINE has returned to baseline (per
            get_line_state, which is instant). Do not set this False merely
            because Grafana still shows stale values — that is ingest lag.

    Returns:
        Acknowledgement that the report was filed.
    """
    return {
        "filed": True,
        "root_cause": root_cause,
        "causal_chain": causal_chain,
        "before": before,
        "after": after,
        "recovered": recovered,
    }


def get_line_state() -> dict:
    """Return the production line's current raw state (incident flag,
    degradation level, and all sensor readings) straight from the simulator.
    Useful as a quick sanity check alongside the Grafana queries."""
    r = httpx.get(f"{SIMULATOR_URL}/state", timeout=10)
    r.raise_for_status()
    return r.json()
