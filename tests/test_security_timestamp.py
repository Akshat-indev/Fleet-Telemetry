"""A2 timestamp window: exact ±300s boundaries under a frozen clock,
malformed timestamp headers, and signature coverage of the timestamp."""

import pytest

import app.security as security
from app.security import GENERIC_AUTH_ERROR

URL = "/v1/telemetry/batch"
FIXED_NOW = 1_800_000_000


@pytest.fixture
def frozen_clock(monkeypatch):
    monkeypatch.setattr(security.time, "time", lambda: FIXED_NOW)
    return FIXED_NOW


@pytest.mark.parametrize("offset", [-300, -299, -1, 0, 1, 299, 300])
def test_timestamp_inside_window_accepted(auth_env, payload, frozen_clock, offset):
    body = auth_env.body([payload("V-TS", 1)])
    headers = auth_env.headers_for(body, timestamp=str(frozen_clock + offset))
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 200


@pytest.mark.parametrize("offset", [-301, 301, -10_000, 10_000])
def test_timestamp_outside_window_rejected(auth_env, payload, frozen_clock, offset):
    body = auth_env.body([payload("V-TS", 1)])
    headers = auth_env.headers_for(body, timestamp=str(frozen_clock + offset))
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 401
    assert r.json() == {"detail": GENERIC_AUTH_ERROR}


@pytest.mark.parametrize(
    "bad_ts",
    ["abc", "1800000000.0", "1800000000.5", "01800000000", "+1800000000", "1e9"],
)
def test_malformed_timestamp_rejected(auth_env, payload, frozen_clock, bad_ts):
    body = auth_env.body([payload("V-TS", 1)])
    headers = auth_env.headers_for(body, timestamp=bad_ts)
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 401


def test_timestamp_is_covered_by_signature(auth_env, payload, frozen_clock):
    """Swapping in a different (still in-window) timestamp must invalidate
    the signature — the timestamp is part of the signed message."""
    body = auth_env.body([payload("V-TS", 1)])
    headers = auth_env.headers_for(body, timestamp=str(frozen_clock - 10))
    headers["X-Timestamp"] = str(frozen_clock + 10)
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 401