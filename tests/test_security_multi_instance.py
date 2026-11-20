"""A2 distributed guarantees, proven with TWO independent uvicorn processes
sharing one PostgreSQL database: the token bucket and the nonce table are
the only coordination points — no in-process state, no Redis."""

import json
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import httpx2 as httpx
import pytest

from app.security import compute_signature

ROOT = Path(__file__).resolve().parents[1]
URL = "/v1/telemetry/batch"


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def two_instances(isolated_client):
    client, engine = isolated_client
    db_url = engine.url.render_as_string(hide_password=False)
    env = os.environ.copy()
    env.update(
        DATABASE_URL=db_url,
        RATE_LIMIT_CAPACITY="4",
        RATE_LIMIT_REFILL_PER_SECOND="0.01",
        AUTH_TIMESTAMP_WINDOW_SECONDS="300",
    )
    procs, bases = [], []
    for _ in range(2):
        port = free_port()
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
    try:
        deadline = time.time() + 30
        for base in bases:
            while True:
                try:
                    if httpx.get(base + "/health", timeout=1).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                if time.time() > deadline:
                    raise RuntimeError(f"instance at {base} failed to start")
                time.sleep(0.2)
        yield SimpleNamespace(
            bases=bases, api_key=client.api_key, secret=client.api_secret
        )
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


def signed_request(env, base, readings, timestamp=None):
    body = json.dumps({"readings": readings}).encode()
    ts = str(int(time.time())) if timestamp is None else timestamp
    headers = {
        "X-API-Key": env.api_key,
        "X-Timestamp": ts,
        "X-Signature": compute_signature(env.secret, env.api_key, ts, body),
    }
    return httpx.post(base + URL, content=body, headers=headers, timeout=10)


def test_shared_bucket_no_overdraw_across_instances(two_instances, payload):
    """Capacity 4, refill ~0: 10 concurrent requests split across both
    instances must yield exactly 4 acceptances — the row lock in PostgreSQL
    arbitrates, not any per-process counter."""

    def fire(i):
        base = two_instances.bases[i % 2]
        return signed_request(two_instances, base, [payload("V-MI", i + 1)])

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(fire, range(10)))

    codes = [r.status_code for r in responses]
    assert codes.count(200) == 4
    assert codes.count(429) == 6
    assert all(
        int(r.headers["Retry-After"]) >= 1 for r in responses if r.status_code == 429
    )


def test_replay_rejected_across_instances(two_instances, payload):
    """A request accepted by instance A must be rejected as a replay when
    the identical bytes + headers arrive at instance B."""
    readings = [payload("V-MI-REPLAY", 1)]
    body = json.dumps({"readings": readings}).encode()
    ts = str(int(time.time()))
    headers = {
        "X-API-Key": two_instances.api_key,
        "X-Timestamp": ts,
        "X-Signature": compute_signature(
            two_instances.secret, two_instances.api_key, ts, body
        ),
    }
    first = httpx.post(
        two_instances.bases[0] + URL, content=body, headers=headers, timeout=10
    )
    second = httpx.post(
        two_instances.bases[1] + URL, content=body, headers=headers, timeout=10
    )
    assert first.status_code == 200
    assert second.status_code == 401
    assert second.json() == {"detail": "authentication failed"}

    # Instance B is healthy and authenticating: a fresh request passes.
    fresh = signed_request(two_instances, two_instances.bases[1], [payload("V-MI-REPLAY", 2)])
    assert fresh.status_code == 200