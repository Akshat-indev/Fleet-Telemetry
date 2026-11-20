"""A2 authentication: header checks, HMAC over raw bytes, generic errors,
constant-time comparison, and secret hygiene. Real PostgreSQL throughout."""

import hmac
import json

import pytest

from app.security import GENERIC_AUTH_ERROR, compute_signature

URL = "/v1/telemetry/batch"


def test_valid_signed_request_accepted(auth_env, payload):
    body = auth_env.body([payload("V-AUTH", 1)])
    r = auth_env.plain.post(URL, content=body, headers=auth_env.headers_for(body))
    assert r.status_code == 200
    assert r.json()["accepted"] == 1


@pytest.mark.parametrize("dropped", ["X-API-Key", "X-Timestamp", "X-Signature"])
def test_missing_header_rejected(auth_env, payload, dropped):
    body = auth_env.body([payload("V-AUTH", 1)])
    headers = auth_env.headers_for(body)
    del headers[dropped]
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 401
    assert r.json() == {"detail": GENERIC_AUTH_ERROR}


def test_no_auth_headers_rejected(auth_env, payload):
    body = auth_env.body([payload("V-AUTH", 1)])
    r = auth_env.plain.post(URL, content=body)
    assert r.status_code == 401
    assert r.json() == {"detail": GENERIC_AUTH_ERROR}


def test_unknown_api_key_rejected(auth_env, payload):
    body = auth_env.body([payload("V-AUTH", 1)])
    headers = auth_env.headers_for(body, api_key="vg-no-such-group")
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 401
    assert r.json() == {"detail": GENERIC_AUTH_ERROR}


def test_wrong_secret_rejected(auth_env, payload):
    body = auth_env.body([payload("V-AUTH", 1)])
    headers = auth_env.headers_for(body, secret="0" * 64)
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 401


def test_modified_body_rejected(auth_env, payload):
    signed_body = auth_env.body([payload("V-AUTH", 1)])
    headers = auth_env.headers_for(signed_body)
    tampered = auth_env.body([payload("V-AUTH", 1, soc_pct=5.0)])
    r = auth_env.plain.post(URL, content=tampered, headers=headers)
    assert r.status_code == 401


@pytest.mark.parametrize(
    "bad_signature",
    ["not-hex", "a" * 63, "a" * 65, "A" * 64, "g" * 64],
)
def test_malformed_signature_rejected(auth_env, payload, bad_signature):
    body = auth_env.body([payload("V-AUTH", 1)])
    headers = auth_env.headers_for(body)
    headers["X-Signature"] = bad_signature
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 401


def test_uppercase_of_valid_signature_rejected(auth_env, payload):
    body = auth_env.body([payload("V-AUTH", 1)])
    headers = auth_env.headers_for(body)
    headers["X-Signature"] = headers["X-Signature"].upper()
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 401


def test_all_auth_failures_return_identical_generic_error(auth_env, payload):
    """No oracle: key existence, failure reason, and secret material must
    be indistinguishable across every authentication failure mode."""
    import time

    body = auth_env.body([payload("V-AUTH", 1)])
    stale = auth_env.headers_for(body, timestamp=str(int(time.time()) - 4000))
    wrong_sig = auth_env.headers_for(body, secret="f" * 64)
    unknown_key = auth_env.headers_for(body, api_key="vg-no-such-group")
    missing = auth_env.headers_for(body)
    del missing["X-Signature"]

    responses = [
        auth_env.plain.post(URL, content=body, headers=h)
        for h in (stale, wrong_sig, unknown_key, missing)
    ]
    responses.append(auth_env.plain.post(URL, content=body))
    assert all(r.status_code == 401 for r in responses)
    assert all(r.json() == {"detail": GENERIC_AUTH_ERROR} for r in responses)
    assert len({r.text for r in responses}) == 1


def test_signature_covers_raw_bytes_not_reserialized_json(auth_env, payload):
    readings = [payload("V-AUTH", 1)]
    compact = json.dumps({"readings": readings}, separators=(",", ":")).encode()
    pretty = json.dumps({"readings": readings}, indent=2).encode()
    reordered = json.dumps(
        {"readings": [{k: v for k, v in reversed(list(readings[0].items()))}]}
    ).encode()
    assert compact != pretty != reordered

    # Signing one serialization and sending another must fail.
    r = auth_env.plain.post(
        URL, content=pretty, headers=auth_env.headers_for(compact)
    )
    assert r.status_code == 401
    r = auth_env.plain.post(
        URL, content=reordered, headers=auth_env.headers_for(compact)
    )
    assert r.status_code == 401

    # Signing the exact bytes sent must succeed, whatever the serialization.
    r = auth_env.plain.post(
        URL, content=pretty, headers=auth_env.headers_for(pretty)
    )
    assert r.status_code == 200


def test_constant_time_comparison_is_used(auth_env, payload, monkeypatch):
    import app.security as security

    calls = []
    real_compare = hmac.compare_digest

    def spy(a, b):
        calls.append((a, b))
        return real_compare(a, b)

    monkeypatch.setattr(security.hmac, "compare_digest", spy)
    body = auth_env.body([payload("V-AUTH", 1)])
    headers = auth_env.headers_for(body)
    r = auth_env.plain.post(URL, content=body, headers=headers)
    assert r.status_code == 200
    expected = compute_signature(
        auth_env.secret, auth_env.api_key, headers["X-Timestamp"], body
    )
    assert calls == [(expected, headers["X-Signature"])]


def test_secret_never_appears_in_responses(auth_env, payload):
    body = auth_env.body([payload("V-AUTH", 1)])
    responses = [
        auth_env.plain.post(URL, content=body),
        auth_env.plain.post(
            URL, content=body, headers=auth_env.headers_for(body, secret="x" * 64)
        ),
        auth_env.plain.post(
            URL, content=body, headers=auth_env.headers_for(body)
        ),
        auth_env.plain.get("/v1/vehicles"),
        auth_env.plain.get("/v1/fleet/summary"),
        auth_env.plain.get("/v1/alerts"),
    ]
    for r in responses:
        assert auth_env.secret not in r.text


def test_auth_failure_ingests_nothing(auth_env, payload):
    from sqlalchemy import text

    body = auth_env.body([payload("V-GHOST", 1)])
    r = auth_env.plain.post(
        URL, content=body, headers=auth_env.headers_for(body, secret="0" * 64)
    )
    assert r.status_code == 401
    with auth_env.engine.connect() as conn:
        count = conn.execute(
            text(
                "SELECT count(*) FROM telemetry_readings t "
                "JOIN vehicles v ON v.id = t.vehicle_id "
                "WHERE v.vehicle_id = 'V-GHOST'"
            )
        ).scalar_one()
    assert count == 0


@pytest.mark.parametrize(
    "path", ["/health", "/ready", "/v1/vehicles", "/v1/fleet/summary", "/v1/alerts"]
)
def test_non_ingestion_endpoints_stay_open(auth_env, path):
    """A2 covers device ingestion; read/ops endpoints are unchanged."""
    assert auth_env.plain.get(path).status_code == 200