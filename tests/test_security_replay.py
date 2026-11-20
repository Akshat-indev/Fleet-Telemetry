"""A2 replay protection: the signature is the replay identifier, arbitrated
by a PostgreSQL primary key. Concurrent copies race on a real database."""

import time
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.security import cleanup_expired_nonces

URL = "/v1/telemetry/batch"


def reading_total(engine, vehicle_id):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT count(*) FROM telemetry_readings t "
                "JOIN vehicles v ON v.id = t.vehicle_id "
                "WHERE v.vehicle_id = :v"
            ),
            {"v": vehicle_id},
        ).scalar_one()


def test_exact_replay_rejected(auth_env, payload):
    body = auth_env.body([payload("V-REPLAY", 1)])
    headers = auth_env.headers_for(body)
    first = auth_env.plain.post(URL, content=body, headers=headers)
    second = auth_env.plain.post(URL, content=body, headers=headers)
    assert first.status_code == 200
    assert second.status_code == 401
    assert reading_total(auth_env.engine, "V-REPLAY") == 1


def test_same_body_fresh_timestamp_is_a_new_request(auth_env, payload):
    body = auth_env.body([payload("V-REPLAY", 1)])
    now = int(time.time())
    first = auth_env.plain.post(
        URL, content=body, headers=auth_env.headers_for(body, timestamp=str(now))
    )
    second = auth_env.plain.post(
        URL,
        content=body,
        headers=auth_env.headers_for(body, timestamp=str(now - 1)),
    )
    assert first.status_code == 200
    assert second.status_code == 200
    # Data-level idempotency still applies: one stored reading.
    assert second.json()["duplicates"] == 1
    assert reading_total(auth_env.engine, "V-REPLAY") == 1


def test_concurrent_replay_exactly_one_winner(auth_env, payload):
    body = auth_env.body([payload("V-RACE", 1)])
    headers = auth_env.headers_for(body)

    def fire(_):
        return auth_env.plain.post(URL, content=body, headers=headers)

    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(fire, range(20)))

    codes = [r.status_code for r in responses]
    assert codes.count(200) == 1
    assert codes.count(401) == 19
    assert reading_total(auth_env.engine, "V-RACE") == 1


def test_nonce_ttl_is_two_windows(auth_env, payload):
    body = auth_env.body([payload("V-TTL", 1)])
    headers = auth_env.headers_for(body)
    assert auth_env.plain.post(URL, content=body, headers=headers).status_code == 200
    with auth_env.engine.connect() as conn:
        ttl = conn.execute(
            text(
                "SELECT EXTRACT(EPOCH FROM (expires_at - seen_at)) "
                "FROM request_nonces WHERE signature = :sig"
            ),
            {"sig": headers["X-Signature"]},
        ).scalar_one()
    assert ttl == 600


def test_cleanup_removes_only_expired_nonces(auth_env):
    with auth_env.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO request_nonces (signature, api_key, expires_at) "
                "VALUES (:dead, :key, now() - interval '1 second'), "
                "       (:live, :key, now() + interval '1 hour')"
            ),
            {"dead": "d" * 64, "live": "e" * 64, "key": auth_env.api_key},
        )
    with Session(auth_env.engine) as session:
        with session.begin():
            removed = cleanup_expired_nonces(session)
    assert removed == 1
    with auth_env.engine.connect() as conn:
        remaining = conn.execute(
            text("SELECT signature FROM request_nonces WHERE signature IN (:d, :l)"),
            {"d": "d" * 64, "l": "e" * 64},
        ).scalars().all()
    assert remaining == ["e" * 64]


def test_replay_accepted_after_nonce_expires_and_is_cleaned(auth_env, payload):
    """TTL tradeoff: a nonce always outlives its signature's validity window
    (TTL = 2x window), so replays are blocked for as long as the signature
    could be accepted. Forcing expiry here proves cleanup actually unblocks
    the signature — data-level dedup still prevents double ingestion."""
    body = auth_env.body([payload("V-EXPIRE", 1)])
    headers = auth_env.headers_for(body)
    assert auth_env.plain.post(URL, content=body, headers=headers).status_code == 200

    with auth_env.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE request_nonces SET expires_at = now() - interval '1 second' "
                "WHERE signature = :sig"
            ),
            {"sig": headers["X-Signature"]},
        )
    with Session(auth_env.engine) as session:
        with session.begin():
            cleanup_expired_nonces(session)

    again = auth_env.plain.post(URL, content=body, headers=headers)
    assert again.status_code == 200
    assert again.json()["duplicates"] == 1


def test_rate_limited_request_does_not_burn_nonce(auth_env, payload, monkeypatch):
    from app.config import get_settings

    monkeypatch.setenv("RATE_LIMIT_CAPACITY", "1")
    monkeypatch.setenv("RATE_LIMIT_REFILL_PER_SECOND", "0.01")
    get_settings.cache_clear()

    now = int(time.time())
    body1 = auth_env.body([payload("V-NONCE", 1)])
    headers1 = auth_env.headers_for(body1, timestamp=str(now))
    body2 = auth_env.body([payload("V-NONCE", 2)])
    headers2 = auth_env.headers_for(body2, timestamp=str(now))

    assert auth_env.plain.post(URL, content=body1, headers=headers1).status_code == 200
    assert auth_env.plain.post(URL, content=body2, headers=headers2).status_code == 429

    # Restore capacity; the exact request that got 429 must still be usable.
    monkeypatch.setenv("RATE_LIMIT_CAPACITY", "120")
    monkeypatch.setenv("RATE_LIMIT_REFILL_PER_SECOND", "20.0")
    get_settings.cache_clear()
    time.sleep(0.1)  # let the bucket refill >= 1 token at 20/s
    r = auth_env.plain.post(URL, content=body2, headers=headers2)
    assert r.status_code == 200
    assert r.json()["accepted"] == 1