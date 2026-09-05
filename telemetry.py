"""
Factory Guardian - OpenTelemetry wiring.

Exports three signals to Grafana Cloud over OTLP/HTTP:
  metrics -> Mimir  (queried with PromQL)
  logs    -> Loki   (queried with LogQL)
  traces  -> Tempo  (queried with TraceQL)

Credentials come from .env:
  OTEL_EXPORTER_OTLP_ENDPOINT
  OTEL_EXPORTER_OTLP_HEADERS      (use Basic%20... not "Basic ...")
  OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
"""

import logging
import os
import time

from dotenv import load_dotenv
from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

load_dotenv()

SERVICE_NAME = "production-control-system"
LINE_ID = "LINE-01"

RESOURCE = Resource.create(
    {
        "service.name": SERVICE_NAME,
        "service.namespace": "factory-guardian",
        "deployment.environment": "demo",
        "production.line": LINE_ID,
    }
)

# Metric names the agent will discover via list_prometheus_metric_names.
# No `unit` is set on purpose: OTel appends units to metric names, and these
# names already carry their units, so leaving it off keeps them predictable.
METRIC_NAMES = [
    "equipment_motor_temperature_celsius",
    "equipment_vibration_mm_per_second",
    "production_output_units_per_hour",
    "production_processing_time_ms",
    "database_connection_pool_percent",
    "api_latency_milliseconds",
    "api_error_rate_percent",
]

METRIC_DESCRIPTIONS = {
    "equipment_motor_temperature_celsius": "Motor temperature of the production line drive",
    "equipment_vibration_mm_per_second": "Vibration amplitude measured at the drive housing",
    "production_output_units_per_hour": "Units completed per hour by the production line",
    "production_processing_time_ms": "Time to process one unit through the line",
    "database_connection_pool_percent": "Percentage of the database connection pool in use",
    "api_latency_milliseconds": "End to end API response time",
    "api_error_rate_percent": "Percentage of API requests returning an error",
}

_tracer: trace.Tracer | None = None


def _check_credentials() -> None:
    missing = [
        v
        for v in ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS")
        if not os.getenv(v)
    ]
    if missing:
        raise RuntimeError(f"Missing in .env: {', '.join(missing)}")
    headers = os.getenv("OTEL_EXPORTER_OTLP_HEADERS", "")
    if "Basic " in headers:
        raise RuntimeError(
            "OTEL_EXPORTER_OTLP_HEADERS contains a literal space after 'Basic'. "
            "Replace 'Basic ' with 'Basic%20'."
        )


def setup(factory) -> None:
    """Wire up all three signals. `factory` is the live Factory instance."""
    global _tracer
    _check_credentials()

    # --- Metrics -----------------------------------------------------
    # Observable gauges pull from factory.readings on each export cycle,
    # so the simulator loop doesn't need to know about OpenTelemetry.
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(), export_interval_millis=5000
    )
    metrics.set_meter_provider(MeterProvider(resource=RESOURCE, metric_readers=[reader]))
    meter = metrics.get_meter(SERVICE_NAME)

    def make_callback(metric_name):
        def callback(_options):
            value = factory.readings.get(metric_name)
            if value is None:
                return []
            yield metrics.Observation(value, {"line": LINE_ID})

        return callback

    for name in METRIC_NAMES:
        meter.create_observable_gauge(
            name,
            callbacks=[make_callback(name)],
            description=METRIC_DESCRIPTIONS[name],
        )

    # --- Logs --------------------------------------------------------
    logger_provider = LoggerProvider(resource=RESOURCE)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter())
    )
    set_logger_provider(logger_provider)
    logging.getLogger().addHandler(
        LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
    )

    # --- Traces ------------------------------------------------------
    tracer_provider = TracerProvider(resource=RESOURCE)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tracer_provider)
    _tracer = trace.get_tracer(SERVICE_NAME)


def record_request_trace(factory) -> None:
    """
    Emit one synthetic request trace per tick.

    Span durations are set from the simulator's current state using explicit
    start/end timestamps, so no real waiting happens but the trace shows a
    truthful breakdown: how much time went to equipment processing versus
    waiting on a database connection.
    """
    if _tracer is None:
        return

    r = factory.readings
    if not r:
        return

    ms = 1_000_000  # nanoseconds per millisecond
    processing_ns = int(r["production_processing_time_ms"] * ms)
    total_ns = int(r["api_latency_milliseconds"] * ms)
    # Whatever isn't equipment processing is time spent waiting on the pool.
    db_wait_ns = max(5 * ms, total_ns - processing_ns - 20 * ms)

    end = time.time_ns()
    start = end - total_ns

    parent = _tracer.start_span(
        "POST /api/production/status",
        start_time=start,
        attributes={
            "http.request.method": "POST",
            "http.route": "/api/production/status",
            "production.line": LINE_ID,
        },
    )
    with trace.use_span(parent, end_on_exit=False):
        cursor = start + 10 * ms

        equipment = _tracer.start_span(
            "equipment.process_unit",
            start_time=cursor,
            attributes={
                "equipment.id": LINE_ID,
                "equipment.motor_temperature_celsius": r[
                    "equipment_motor_temperature_celsius"
                ],
            },
        )
        cursor += processing_ns
        equipment.end(end_time=cursor)

        db = _tracer.start_span(
            "db.query production_orders",
            start_time=cursor,
            attributes={
                "db.system": "postgresql",
                "db.operation": "SELECT",
                "db.pool.utilization_percent": r["database_connection_pool_percent"],
            },
        )
        cursor += db_wait_ns
        if r["database_connection_pool_percent"] > 95:
            db.set_status(trace.Status(trace.StatusCode.ERROR, "connection acquisition timeout"))
        db.end(end_time=cursor)

    if r["api_error_rate_percent"] > 5:
        parent.set_status(trace.Status(trace.StatusCode.ERROR, "upstream degraded"))
    parent.end(end_time=end)