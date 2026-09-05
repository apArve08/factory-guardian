"""
Create/update the Stage 4 alert rule: API latency > 5s on LINE-01.

Run:  PATH=/opt/homebrew/bin:$PATH PYTHONPATH=$PWD python scripts/build_alert.py
"""

import asyncio

from dotenv import load_dotenv

load_dotenv()

PROM = "grafanacloud-prom"
FOLDER_UID = "dfx5e0lefmcjka"  # "Factory Guardian" folder

DATA = [
    {
        "refId": "A",
        "datasourceUid": PROM,
        "relativeTimeRange": {"from": 600, "to": 0},
        "model": {
            "refId": "A",
            "expr": 'api_latency_milliseconds{line="LINE-01"}',
            "instant": True,
            "range": False,
        },
    },
    {
        "refId": "B",
        "datasourceUid": "__expr__",
        "relativeTimeRange": {"from": 600, "to": 0},
        "model": {
            "refId": "B",
            "type": "reduce",
            "expression": "A",
            "reducer": "last",
        },
    },
    {
        "refId": "C",
        "datasourceUid": "__expr__",
        "relativeTimeRange": {"from": 600, "to": 0},
        "model": {
            "refId": "C",
            "type": "threshold",
            "expression": "B",
            "conditions": [
                {"evaluator": {"type": "gt", "params": [5000]}}
            ],
        },
    },
]


async def main():
    import guardian.agent as a
    tools = {t.name: t for t in await a.root_agent.tools[0].get_tools()}
    args = {
        "operation": "create",
        "org_id": 1,
        "title": "API latency > 5s (LINE-01)",
        "folder_uid": FOLDER_UID,
        "rule_group": "factory-guardian",
        "condition": "C",
        "data": DATA,
        "for": "1m",
        "no_data_state": "OK",
        "exec_err_state": "Error",
        "labels": {"severity": "critical", "line": "LINE-01"},
        "annotations": {
            "summary": "API latency on LINE-01 is above 5s",
            "description": (
                "api_latency_milliseconds for line LINE-01 has exceeded 5000ms "
                "for 1m. Likely upstream: equipment degradation -> processing "
                "time -> DB pool saturation. Investigate with Factory Guardian."
            ),
        },
    }
    r = await tools["alerting_manage_rules"].run_async(args=args, tool_context=None)
    print(r["content"][0]["text"])


if __name__ == "__main__":
    asyncio.run(main())
