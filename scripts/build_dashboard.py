"""
Build/push the Factory Guardian dashboard to Grafana Cloud.

Run:  python scripts/build_dashboard.py
Reads GRAFANA_URL / GRAFANA_SERVICE_ACCOUNT_TOKEN from .env.
Also writes dashboard.json to the repo root for version control / manual import.
"""

import asyncio
import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROM = "grafanacloud-prom"
LOKI = "grafanacloud-logs"
LINE = 'line="LINE-01"'

RED = [
    {"color": "green", "value": None},
    {"color": "orange", "value": 0},
    {"color": "red", "value": 0},
]


def ts(title, expr, unit, x, y, w=12, h=8, thresholds=None, legend="{{line}}"):
    field = {"unit": unit, "custom": {"drawStyle": "line", "fillOpacity": 10,
                                      "lineWidth": 2, "spanNulls": True}}
    if thresholds:
        steps = [{"color": "green", "value": None}]
        steps += [{"color": c, "value": v} for v, c in thresholds]
        field["thresholds"] = {"mode": "absolute", "steps": steps}
        field["custom"]["thresholdsStyle"] = {"mode": "line"}
    return {
        "type": "timeseries",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "prometheus", "uid": PROM},
        "targets": [{"refId": "A", "expr": expr,
                     "datasource": {"type": "prometheus", "uid": PROM},
                     "legendFormat": legend}],
        "fieldConfig": {"defaults": field, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi"}},
    }


def stat(title, expr, unit, x, y, w=6, h=4, thresholds=None):
    steps = [{"color": "green", "value": None}]
    if thresholds:
        steps += [{"color": c, "value": v} for v, c in thresholds]
    return {
        "type": "stat",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "prometheus", "uid": PROM},
        "targets": [{"refId": "A", "expr": expr,
                     "datasource": {"type": "prometheus", "uid": PROM}}],
        "fieldConfig": {"defaults": {
            "unit": unit,
            "thresholds": {"mode": "absolute", "steps": steps},
            "color": {"mode": "thresholds"}}, "overrides": []},
        "options": {"colorMode": "background", "graphMode": "area",
                    "reduceOptions": {"calcs": ["lastNotNull"]}},
    }


def row(title, y):
    return {"type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def logs(title, expr, x, y, w=24, h=9):
    return {
        "type": "logs",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "loki", "uid": LOKI},
        "targets": [{"refId": "A", "expr": expr,
                     "datasource": {"type": "loki", "uid": LOKI}}],
        "options": {"showTime": True, "wrapLogMessage": True,
                    "sortOrder": "Descending", "enableLogDetails": True},
    }


def build():
    panels = []
    y = 0

    # --- summary stats ------------------------------------------------
    panels.append(row("Now", y)); y += 1
    panels += [
        stat("Production output", f"production_output_units_per_hour{{{LINE}}}",
             "short", 0, y, thresholds=[(600, "orange"), (400, "red")]),
        stat("API latency", f"api_latency_milliseconds{{{LINE}}}",
             "ms", 6, y, thresholds=[(1000, "orange"), (5000, "red")]),
        stat("API error rate", f"api_error_rate_percent{{{LINE}}}",
             "percent", 12, y, thresholds=[(2, "orange"), (5, "red")]),
        stat("DB pool in use", f"database_connection_pool_percent{{{LINE}}}",
             "percent", 18, y, thresholds=[(80, "orange"), (95, "red")]),
    ]
    y += 4

    # --- equipment (root cause) ------------------------------------
    panels.append(row("Equipment  —  root cause", y)); y += 1
    panels += [
        ts("Motor temperature", f"equipment_motor_temperature_celsius{{{LINE}}}",
           "celsius", 0, y, thresholds=[(85, "red")]),
        ts("Vibration", f"equipment_vibration_mm_per_second{{{LINE}}}",
           "none", 12, y, thresholds=[(1.2, "red")]),
    ]
    y += 8

    # --- production ----------------------------------------------------
    panels.append(row("Production line", y)); y += 1
    panels += [
        ts("Output (units/hour)", f"production_output_units_per_hour{{{LINE}}}",
           "short", 0, y),
        ts("Processing time per unit", f"production_processing_time_ms{{{LINE}}}",
           "ms", 12, y),
    ]
    y += 8

    # --- application / database -------------------------------------
    panels.append(row("Application & database", y)); y += 1
    panels += [
        ts("DB connection pool", f"database_connection_pool_percent{{{LINE}}}",
           "percent", 0, y, thresholds=[(80, "orange"), (95, "red")]),
        ts("API latency", f"api_latency_milliseconds{{{LINE}}}",
           "ms", 12, y, thresholds=[(5000, "red")]),
        ts("API error rate", f"api_error_rate_percent{{{LINE}}}",
           "percent", 0, y + 8, thresholds=[(5, "red")]),
    ]
    y += 16

    # --- logs --------------------------------------------------------
    panels.append(row("Logs (Loki)", y)); y += 1
    panels.append(logs("production-control-system",
                       '{service_name="production-control-system"}', 0, y))
    y += 9

    return {
        "uid": "factory-guardian",
        "title": "Factory Guardian — Production Line LINE-01",
        "tags": ["factory-guardian", "demo"],
        "timezone": "browser",
        "schemaVersion": 39,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "panels": panels,
        "annotations": {"list": [{
            "name": "Annotations & Alerts",
            "datasource": {"type": "grafana", "uid": "-- Grafana --"},
            "enable": True, "iconColor": "red",
            "target": {"limit": 100, "matchAny": False, "tags": [], "type": "dashboard"},
        }]},
    }


async def push(dash):
    import guardian.agent as a
    tools = {t.name: t for t in await a.root_agent.tools[0].get_tools()}
    r = await tools["update_dashboard"].run_async(
        args={"dashboard": dash, "overwrite": True,
              "message": "build_dashboard.py"},
        tool_context=None,
    )
    print(r["content"][0]["text"])


if __name__ == "__main__":
    dash = build()
    out = Path(__file__).resolve().parents[1] / "dashboard.json"
    out.write_text(json.dumps(dash, indent=2))
    print(f"wrote {out}")
    try:
        asyncio.run(push(dash))
    except Exception as e:
        print("push failed (import dashboard.json manually):", repr(e)[:300])
