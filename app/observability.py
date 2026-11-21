"""Structured JSON logging, request-ID propagation, and bounded in-process
metrics (A6). Zero extra dependencies — standard library only."""

import json
import logging
import re
import threading
import time
import uuid
from collections import Counter, deque
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_LATENCY_WINDOW = 5_000


class JsonLogFormatter(logging.Formatter):
    """Single-line JSON log formatter with automatic request_id injection."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        rid = getattr(record, "request_id", None) or request_id_var.get()
        if rid is not None:
            payload["request_id"] = rid
        for attr in ("method", "path", "route", "status_code", "duration_ms"):
            val = getattr(record, attr, None)
            if val is not None:
                payload[attr] = val
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("voltgrid")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonLogFormatter())
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger


http_logger = configure_logging().getChild("http")


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round(pct / 100.0 * (len(sorted_vals) - 1))))
    return round(sorted_vals[idx], 2)


class MetricsRegistry:
    """Thread-safe, bounded-memory operational metrics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at = time.monotonic()
        self._http_requests: Counter[tuple[str, str, int]] = Counter()
        self._latencies_ms: deque[float] = deque(maxlen=_LATENCY_WINDOW)
        self._ingestion: Counter[str] = Counter()
        self._alerts: Counter[str] = Counter()
        self._active_sse: int = 0

    def reset(self) -> None:
        with self._lock:
            self._started_at = time.monotonic()
            self._http_requests.clear()
            self._latencies_ms.clear()
            self._ingestion.clear()
            self._alerts.clear()
            self._active_sse = 0

    def record_http(self, method: str, route: str, status_code: int, duration_ms: float) -> None:
        with self._lock:
            self._http_requests[(method, route, status_code)] += 1
            self._latencies_ms.append(duration_ms)

    def record_ingestion(self, accepted: int, duplicates: int, rejected: int) -> None:
        with self._lock:
            self._ingestion["accepted"] += accepted
            self._ingestion["duplicate"] += duplicates
            self._ingestion["rejected"] += rejected

    def record_alert(self, transition: str) -> None:
        with self._lock:
            self._alerts[transition] += 1

    def sse_connected(self) -> None:
        with self._lock:
            self._active_sse += 1

    def sse_disconnected(self) -> None:
        with self._lock:
            self._active_sse = max(0, self._active_sse - 1)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            total_reqs = sum(self._http_requests.values())
            error_reqs = sum(c for (_, _, st), c in self._http_requests.items() if st >= 400)
            by_route = {
                f"{method} {route} {status}": count
                for (method, route, status), count in sorted(self._http_requests.items())
            }
            lat = sorted(self._latencies_ms)
            ingestion = {
                "accepted": self._ingestion["accepted"],
                "duplicate": self._ingestion["duplicate"],
                "rejected": self._ingestion["rejected"],
            }
            alerts = {
                "opened": self._alerts["opened"],
                "acknowledged": self._alerts["acknowledged"],
                "resolved": self._alerts["resolved"],
            }
            active_sse = self._active_sse
            uptime = round(time.monotonic() - self._started_at, 2)

        return {
            "uptime_seconds": uptime,
            "http_requests_total": total_reqs,
            "http_errors_total": error_reqs,
            "http_requests_by_route": by_route,
            "http_request_duration_ms": {
                "samples": len(lat),
                "avg": round(sum(lat) / len(lat), 2) if lat else 0.0,
                "p50": _percentile(lat, 50),
                "p95": _percentile(lat, 95),
                "max": round(lat[-1], 2) if lat else 0.0,
            },
            "ingestion_readings_total": ingestion,
            "alerts_total": alerts,
            "sse_active_connections": active_sse,
        }

    def prometheus_text(self) -> str:
        snap = self.snapshot()
        lines = [
            "# HELP voltgrid_uptime_seconds Process uptime in seconds.",
            "# TYPE voltgrid_uptime_seconds gauge",
            f"voltgrid_uptime_seconds {snap['uptime_seconds']}",
            "# HELP voltgrid_http_requests_total Total HTTP requests handled.",
            "# TYPE voltgrid_http_requests_total counter",
            f"voltgrid_http_requests_total {snap['http_requests_total']}",
            "# HELP voltgrid_http_errors_total Total HTTP 4xx/5xx responses.",
            "# TYPE voltgrid_http_errors_total counter",
            f"voltgrid_http_errors_total {snap['http_errors_total']}",
            "# HELP voltgrid_ingestion_readings_total Telemetry readings processed by outcome.",
            "# TYPE voltgrid_ingestion_readings_total counter",
        ]
        for outcome, count in snap["ingestion_readings_total"].items():
            lines.append(f'voltgrid_ingestion_readings_total{{status="{outcome}"}} {count}')
        lines += [
            "# HELP voltgrid_alerts_total Alert lifecycle transitions.",
            "# TYPE voltgrid_alerts_total counter",
        ]
        for transition, count in snap["alerts_total"].items():
            lines.append(f'voltgrid_alerts_total{{transition="{transition}"}} {count}')
        lines += [
            "# HELP voltgrid_sse_active_connections Active SSE stream subscribers.",
            "# TYPE voltgrid_sse_active_connections gauge",
            f"voltgrid_sse_active_connections {snap['sse_active_connections']}",
            "",
        ]
        return "\n".join(lines)


metrics = MetricsRegistry()


def _resolve_request_id(header_value: str | None) -> str:
    if header_value and _SAFE_REQUEST_ID.match(header_value):
        return header_value
    return uuid.uuid4().hex


class RequestObservabilityMiddleware(BaseHTTPMiddleware):
    """Assigns/propagates X-Request-ID, logs structured JSON, records metrics."""

    async def dispatch(self, request: Request, call_next) -> Response:
        rid = _resolve_request_id(request.headers.get("X-Request-ID"))
        token = request_id_var.set(rid)
        started = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Request-ID"] = rid
            return response
        finally:
            duration_ms = round((time.perf_counter() - started) * 1000.0, 2)
            route_obj = request.scope.get("route")
            route_path = getattr(route_obj, "path", request.url.path)
            metrics.record_http(request.method, route_path, status_code, duration_ms)
            http_logger.info(
                "request completed",
                extra={
                    "request_id": rid,
                    "method": request.method,
                    "path": request.url.path,
                    "route": route_path,
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                },
            )
            request_id_var.reset(token)
