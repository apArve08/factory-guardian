"""
Factory Guardian - production line simulator.

Models a single variable (equipment degradation, 0.0 to 1.0) and derives
every other signal from it, so the causal chain the agent discovers is real:

    equipment degrades
      -> processing time rises
      -> requests retry
      -> DB connection pool fills
      -> API latency spikes (non-linearly, as the pool saturates)
      -> error rate climbs, production output falls

Run:  uvicorn simulator:app --reload --port 8000
"""

import asyncio
import collections
import logging
import time
import telemetry
import random
from contextlib import asynccontextmanager

from fastapi import FastAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("production-control-system")


class RingBufferHandler(logging.Handler):
    """Keeps the last N log records so the operator console can show a live
    log stream (GET /logs) without touching Loki."""

    def __init__(self, capacity: int = 200) -> None:
        super().__init__()
        self.records: collections.deque = collections.deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append({
            "t": record.created,
            "level": record.levelname,
            "msg": record.getMessage(),
        })


_log_buffer = RingBufferHandler()
log.addHandler(_log_buffer)

TICK_SECONDS = 2.0
RAMP_UP_PER_TICK = 0.08      # ~25s from healthy to fully degraded
RAMP_DOWN_PER_TICK = 0.15    # recovery is faster, but not instant


def jitter(value: float, pct: float = 0.03) -> float:
    """Add small random noise so the graphs don't look synthetic."""
    return value * (1 + random.uniform(-pct, pct))


class Factory:
    def __init__(self) -> None:
        self.incident = False
        self.degradation = 0.0   # 0.0 healthy .. 1.0 fully degraded
        self.readings: dict[str, float] = {}
        self._log_state: dict[str, bool] = {}

    def step(self) -> None:
        """Advance degradation one tick toward its target."""
        target = 1.0 if self.incident else 0.0
        if self.degradation < target:
            self.degradation = min(target, self.degradation + RAMP_UP_PER_TICK)
        elif self.degradation > target:
            self.degradation = max(target, self.degradation - RAMP_DOWN_PER_TICK)
        self.readings = self._derive()

    def _derive(self) -> dict[str, float]:
        d = self.degradation

        # --- Equipment layer: the root cause ---------------------------
        motor_temp = 65 + 30 * d                    # 65 C -> 95 C
        vibration = 0.2 + 1.6 * d                   # 0.2 mm/s -> 1.8 mm/s

        # --- Production layer: equipment slows the line ----------------
        processing_ms = 120 + 950 * d               # per-unit processing time
        production_output = 980 - 390 * d           # units/hour

        # --- Application layer: slow processing causes retries ---------
        # Retries only start once processing is meaningfully slow.
        retry_rate = max(0.0, (processing_ms - 300) / 1000)   # 0 .. ~0.77

        # --- Database layer: retries consume connections ---------------
        db_connections = min(99.0, 40 + 75 * retry_rate)      # percent of pool

        # --- Latency: non-linear once the pool nears saturation --------
        # Below ~80% the pool absorbs load; above it, queueing explodes.
        saturation = max(0.0, (db_connections - 80) / 20)     # 0 .. 1
        api_latency_ms = 200 + 400 * retry_rate + 7800 * saturation**2

        # --- Errors: connection acquisition timeouts -------------------
        error_rate = 0.2 + 18 * saturation**2                 # percent

        return {
            "equipment_motor_temperature_celsius": round(jitter(motor_temp), 2),
            "equipment_vibration_mm_per_second": round(jitter(vibration), 3),
            "production_output_units_per_hour": round(jitter(production_output), 1),
            "production_processing_time_ms": round(jitter(processing_ms), 1),
            "database_connection_pool_percent": round(jitter(db_connections), 1),
            "api_latency_milliseconds": round(jitter(api_latency_ms), 1),
            "api_error_rate_percent": round(max(0.0, jitter(error_rate)), 2),
        }

    def emit_logs(self) -> None:
        """Operational log lines (also shipped to Loki via telemetry).

        Edge-triggered: a line is emitted when a signal crosses a threshold, not
        every tick, so the stream reads like a real incident timeline.
        """
        r = self.readings
        pool = r["database_connection_pool_percent"]
        temp = r["equipment_motor_temperature_celsius"]
        vib = r["equipment_vibration_mm_per_second"]
        proc = r["production_processing_time_ms"]
        lat = r["api_latency_milliseconds"]
        err = r["api_error_rate_percent"]
        out = r["production_output_units_per_hour"]
        prev = self._log_state

        def crossed(key, value, threshold, up=True):
            was = prev.get(key, False)
            now = value > threshold if up else value < threshold
            prev[key] = now
            return now and not was

        if crossed("temp_hi", temp, 85):
            log.warning("equipment: LINE-01 motor temperature %.1f°C exceeds 85°C threshold", temp)
        if crossed("vib_hi", vib, 1.2):
            log.warning("equipment: LINE-01 drive vibration %.2f mm/s above nominal (0.2 mm/s)", vib)
        if crossed("proc_hi", proc, 500):
            log.warning("processing: unit cycle time degraded to %.0f ms (nominal 120 ms)", proc)
        if crossed("pool_hi", pool, 85):
            log.warning("database: connection pool at %.0f%% utilization, approaching capacity", pool)
        if crossed("pool_crit", pool, 95):
            log.error("database: connection acquisition timeout after 5000ms; pool exhausted at %.0f%%", pool)
        if crossed("lat_hi", lat, 5000):
            log.error("api: p50 latency %.0f ms breached 5000 ms SLO on POST /api/production/status", lat)
        if crossed("err_hi", err, 5):
            log.error("api: error rate %.1f%% - upstream 'production_orders' query failing", err)
        if crossed("out_lo", out, 700, up=False):
            log.warning("production: LINE-01 throughput dropped to %.0f units/hr (target 980)", out)

        # recovery notices (only meaningful once we've seen the bad state)
        if crossed("lat_ok", lat, 1000, up=False) and prev.get("lat_hi_ever"):
            log.info("api: latency recovered to %.0f ms, SLO restored", lat)
        if crossed("temp_ok", temp, 80, up=False) and prev.get("temp_hi_ever"):
            log.info("equipment: LINE-01 motor temperature back within range (%.1f°C)", temp)
        prev["lat_hi_ever"] = prev.get("lat_hi_ever") or prev.get("lat_hi")
        prev["temp_hi_ever"] = prev.get("temp_hi_ever") or prev.get("temp_hi")


factory = Factory()


async def run_loop() -> None:
    ticks = 0
    while True:
        factory.step()
        factory.emit_logs()
        telemetry.record_request_trace(factory)
        r = factory.readings
        # Heartbeat every ~30s so Loki always has a recent baseline line.
        if ticks % 15 == 0:
            log.info(
                "health: LINE-01 output=%.0f/hr latency=%.0fms pool=%.0f%% "
                "temp=%.1f°C errors=%.2f%%",
                r["production_output_units_per_hour"],
                r["api_latency_milliseconds"],
                r["database_connection_pool_percent"],
                r["equipment_motor_temperature_celsius"],
                r["api_error_rate_percent"],
            )
        ticks += 1
        await asyncio.sleep(TICK_SECONDS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    telemetry.setup(factory)                          # <-- add this
    task = asyncio.create_task(run_loop())
    yield
    task.cancel()


app = FastAPI(title="Production Control System", lifespan=lifespan)


@app.get("/state")
def get_state():
    return {
        "incident": factory.incident,
        "degradation": round(factory.degradation, 3),
        "readings": factory.readings,
    }


@app.get("/logs")
def get_logs(limit: int = 60):
    """Recent operational log lines, newest last."""
    return {"logs": list(_log_buffer.records)[-limit:]}


@app.post("/incident")
def trigger_incident():
    """Demo control: start the equipment degradation."""
    factory.incident = True
    log.warning("operator: incident triggered on LINE-01 - beginning equipment degradation")
    return {"incident": True}


@app.post("/remediate")
def remediate(action: str = "restart_processing_service"):
    """Called by the agent, only after a human approves."""
    factory.incident = False
    log.info("operator: remediation executed - %s; line recovering", action)
    return {
        "status": "executed",
        "action": action,
        "note": "Recovery takes roughly 20 seconds; verify telemetry before concluding.",
    }