"""A2 rate limiting: PostgreSQL-backed token bucket. Capacity, 429 shape,
Retry-After math, time-based refill, per-key isolation, token accounting on
auth failures, and concurrent no-overdraw on a real database."""

import secrets
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import text

from app.config import get_settings

URL = "/v1/telemetry/batch"


@pytest.fixture
def bucket(monkeypatch):
    def _configure(capacity, refill):
        monkeypatch.setenv("RATE_LIMIT_CAPACITY", str(capacity))
        monkeypatch.setenv("RATE_LIMIT_REFILL_PER_SECOND", str(refill))
        get_settings.cache_clear()

    yield _configure
    get_settings.cache_clear()


def send(auth_env, payload, seq, **header_kwargs):
    body = auth_env.body([payload("V-RATE", seq)])
    return auth_env.plain.post(
        URL, content=body, headers=auth_env.headers_for(body, **header_kwargs)
    )


def test_capacity_then_429_with_retry_after(auth_env, payload, bucket):
    bucket(5, 0.01)
    for seq in range(1, 6):
        assert send(auth_env, payload, seq).status_code == 200
    limited = send(auth_env, payload, 6)
    assert limited.status_code == 429
    assert limited.json() == {"detail": "rate limit exceeded"}
    retry_after = int(limited.headers["Retry-After"])
    assert retry_after >= 1
    assert auth_env.secret not in limited.text


def test_retry_after_reflects_refill_rate(auth_env, payload, bucket):
    bucket(2, 0.5)
    assert send(auth_env, payload, 1).status_code == 200
    assert send(auth_env, payload, 2).status_code == 200
    limited = send(auth_env, payload, 3)
    assert limited.status_code == 429
    # ~0 tokens left, 0.5 tokens/s => ~2s until one token is available.
    assert int(limited.headers["Retry-After"]) == 2


def test_tokens_refill_over_time(auth_env, payload, bucket):
    bucket(1, 5.0)
    assert send(auth_env, payload, 1).status_code == 200
    assert send(auth_env, payload, 2).status_code == 429
    time.sleep(0.3)  # 5 tokens/s * 0.3s = 1.5 tokens
    assert send(auth_env, payload, 3).status_code == 200


def test_concurrent_requests_cannot_overdraw_bucket(auth_env, payload, bucket):
    bucket(5, 0.01)

    def fire(seq):
        return send(auth_env, payload, seq)

    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(fire, range(1, 21)))

    codes = [r.status_code for r in responses]
    assert codes.count(200) == 5
    assert codes.count(429) == 15
    with auth_env.engine.connect() as conn:
        tokens = conn.execute(
            text("SELECT tokens FROM rate_limit_buckets WHERE api_key = :key"),
            {"key": auth_env.api_key},
        ).scalar_one()
    assert tokens >= 0


def test_limit_is_per_device_group(auth_env, payload, bucket):
    bucket(1, 0.01)
    other_key = "vg-other-" + secrets.token_hex(8)
    other_secret = secrets.token_hex(32)
    with auth_env.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO device_groups (api_key, secret, name) "
                "VALUES (:key, :secret, 'other')"
            ),
            {"key": other_key, "secret": other_secret},
        )

    assert send(auth_env, payload, 1).status_code == 200
    assert send(auth_env, payload, 2).status_code == 429

    body = auth_env.body([payload("V-RATE-OTHER", 1)])
    headers = auth_env.headers_for(body, api_key=other_key, secret=other_secret)
    assert auth_env.plain.post(URL, content=body, headers=headers).status_code == 200


def test_failed_auth_does_not_consume_tokens(auth_env, payload, bucket):
    bucket(1, 0.01)
    body = auth_env.body([payload("V-RATE", 1)])
    bad = auth_env.headers_for(body, secret="0" * 64)
    assert auth_env.plain.post(URL, content=body, headers=bad).status_code == 401
    assert send(auth_env, payload, 1).status_code == 200


def test_replay_rejection_refunds_token(auth_env, payload, bucket):
    bucket(2, 0.01)
    body = auth_env.body([payload("V-RATE", 1)])
    headers = auth_env.headers_for(body)
    assert auth_env.plain.post(URL, content=body, headers=headers).status_code == 200
    # Replay fails at the nonce insert; the consumed token must be refunded
    # by the transaction rollback, leaving one token for a fresh request.
    assert auth_env.plain.post(URL, content=body, headers=headers).status_code == 401
    assert send(auth_env, payload, 2).status_code == 200
    assert send(auth_env, payload, 3).status_code == 429