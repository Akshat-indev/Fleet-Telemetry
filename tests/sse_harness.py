"""Shared helpers for SSE tests: real uvicorn subprocesses + an SSE parser.

This environment's starlette TestClient buffers whole responses, so it
cannot consume an infinite SSE stream. These tests therefore run against
real HTTP servers — which also exercises genuine socket disconnects.
"""

import json
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import httpx2 as httpx

from app.security import compute_signature

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_instances(db_url, count, extra_env=None, ports=None):
    env = os.environ.copy()
    env["DATABASE_URL"] = db_url
    if extra_env:
        env.update(extra_env)
    procs, bases = [], []
    for i in range(count):
        port = ports[i] if ports else free_port()
        procs.append(
            subprocess.Popen(
                [
                    sys.executable, "-m", "uvicorn", "app.main:app",
                    "--host", "127.0.0.1", "--port", str(port),
                ],
                cwd=ROOT,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
        bases.append(f"http://127.0.0.1:{port}")
    deadline = time.time() + 30
    for base in bases:
        while True:
            try:
                if httpx.get(base + "/health", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.time() > deadline:
                stop_instances(procs)
                raise RuntimeError(f"instance at {base} failed to start")
            time.sleep(0.2)
    return procs, bases


def stop_instances(procs):
    for proc in procs:
        proc.terminate()
    for proc in procs:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def post_readings(base, api_key, secret, readings, timestamp=None):
    body = json.dumps({"readings": readings}).encode()
    ts = str(int(time.time())) if timestamp is None else timestamp
    headers = {
        "X-API-Key": api_key,
        "X-Timestamp": ts,
        "X-Signature": compute_signature(secret, api_key, ts, body),
    }
    return httpx.post(
        base + "/v1/telemetry/batch", content=body, headers=headers, timeout=15
    )


def _parse_lines(lines):
    current = {}
    for line in lines:
        if line == "":
            if current:
                yield current
                current = {}
            continue
        if line.startswith(":"):
            # Surface keepalives so time-bounded readers can check their
            # deadline even when no real events are flowing.
            yield {"comment": line}
            continue
        field, _, value = line.partition(":")
        value = value.lstrip(" ")
        if field == "id":
            current["id"] = int(value)
        elif field == "event":
            current["event"] = value
        elif field == "data":
            current["data"] = json.loads(value)
    if current:
        yield current


def iter_sse(response):
    """Parse an SSE byte stream into event dicts. Comment keepalives are
    yielded as {"comment": ...} markers so time-bounded readers keep
    ticking. Control events (no id) carry their event name and data only.

    httpx line iterators are single-use, so the parser is cached on the
    response and returned directly: repeated collect calls continue where
    the last one stopped. (Wrapping it in another generator would close
    the cached parser on garbage collection.)
    """
    parser = getattr(response, "_sse_parser", None)
    if parser is None:
        parser = _parse_lines(response.iter_lines())
        response._sse_parser = parser
    return parser


def collect(response, seconds):
    """Read events for a fixed wall-clock window. Heartbeats guarantee
    the line iterator keeps moving even when no events occur."""
    deadline = time.monotonic() + seconds
    events = []
    for event in iter_sse(response):
        events.append(event)
        if time.monotonic() >= deadline:
            break
    return events


def collect_until(response, predicate, timeout=10.0):
    """Read until predicate(events) is true; returns (events, matched)."""
    deadline = time.monotonic() + timeout
    events = []
    for event in iter_sse(response):
        events.append(event)
        if predicate(events):
            return events, True
        if time.monotonic() >= deadline:
            break
    return events, False


@contextmanager
def open_stream(base, last_event_id=None, timeout=10.0):
    headers = {} if last_event_id is None else {"Last-Event-ID": str(last_event_id)}
    with httpx.Client(timeout=httpx.Timeout(timeout, read=timeout)) as client:
        with client.stream("GET", base + "/v1/stream", headers=headers) as response:
            yield response