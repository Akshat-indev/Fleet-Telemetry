"""Device authentication for the ingestion API (A2).

Protocol (documented in README):
  X-API-Key:    device group api key
  X-Timestamp:  Unix epoch seconds (integer string)
  X-Signature:  hex HMAC-SHA256(secret, api_key + "\\n" + timestamp + "\\n" + raw_body)

Verification order: header format -> key lookup -> timestamp window ->
constant-time signature compare -> token bucket -> replay insert.
Every authentication failure raises AuthenticationError with one generic
message: responses must not reveal whether a key exists, which check
failed, or anything about the secret.
"""

import hashlib
import hmac
import math
import time

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import get_settings

GENERIC_AUTH_ERROR = "authentication failed"


class AuthenticationError(Exception):
    pass


class RateLimited(Exception):
    def __init__(self, retry_after: int):
        self.retry_after = retry_after


def compute_signature(secret: str, api_key: str, timestamp: str, raw_body: bytes) -> str:
    message = api_key.encode() + b"\n" + timestamp.encode() + b"\n" + raw_body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


_GET_GROUP = text("SELECT secret FROM device_groups WHERE api_key = :key")

# The signature is the replay identifier: it covers key + timestamp + exact
# body, so a duplicate signature is a duplicate request. The primary key
# arbitrates concurrent copies — exactly one INSERT returns a row.
_INSERT_NONCE = text("""
    INSERT INTO request_nonces (signature, api_key, expires_at)
    VALUES (:signature, :key, now() + make_interval(secs => :ttl))
    ON CONFLICT (signature) DO NOTHING
    RETURNING signature
""")

_ENSURE_BUCKET = text("""
    INSERT INTO rate_limit_buckets (api_key, tokens, last_refill)
    VALUES (:key, :capacity, now())
    ON CONFLICT (api_key) DO NOTHING
""")

# One atomic statement: refill from elapsed time, cap, and consume a token
# only if at least one is available. The row lock serializes all instances;
# under READ COMMITTED the WHERE clause is re-evaluated after lock wait, so
# concurrent consumers can never overdraw the bucket.
_CONSUME_TOKEN = text("""
    UPDATE rate_limit_buckets
    SET tokens = LEAST(:capacity,
                       tokens + EXTRACT(EPOCH FROM (now() - last_refill)) * :refill) - 1,
        last_refill = now()
    WHERE api_key = :key
      AND LEAST(:capacity,
                tokens + EXTRACT(EPOCH FROM (now() - last_refill)) * :refill) >= 1
    RETURNING tokens
""")

_BUCKET_STATE = text(
    "SELECT tokens, EXTRACT(EPOCH FROM (now() - last_refill)) AS elapsed "
    "FROM rate_limit_buckets WHERE api_key = :key"
)

_CLEANUP_NONCES = text("DELETE FROM request_nonces WHERE expires_at < now()")


def _check_timestamp(timestamp_header: str, window: int) -> None:
    try:
        ts = int(timestamp_header)
    except (TypeError, ValueError):
        raise AuthenticationError(GENERIC_AUTH_ERROR)
    if str(ts) != timestamp_header.strip():
        raise AuthenticationError(GENERIC_AUTH_ERROR)
    now = int(time.time())
    if abs(now - ts) > window:
        raise AuthenticationError(GENERIC_AUTH_ERROR)


def _consume_token(session: Session, api_key: str) -> None:
    settings = get_settings()
    capacity = settings.rate_limit_capacity
    refill = settings.rate_limit_refill_per_second
    session.execute(_ENSURE_BUCKET, {"key": api_key, "capacity": capacity})
    consumed = session.execute(
        _CONSUME_TOKEN, {"key": api_key, "capacity": capacity, "refill": refill}
    ).first()
    if consumed is not None:
        return
    row = session.execute(_BUCKET_STATE, {"key": api_key}).one()
    available = min(capacity, row.tokens + float(row.elapsed) * refill)
    retry_after = max(1, math.ceil((1 - available) / refill))
    raise RateLimited(retry_after)


def authenticate(
    session: Session,
    *,
    api_key: str | None,
    timestamp: str | None,
    signature: str | None,
    raw_body: bytes,
) -> None:
    settings = get_settings()
    if not api_key or not timestamp or not signature:
        raise AuthenticationError(GENERIC_AUTH_ERROR)
    if len(signature) != 64 or any(c not in "0123456789abcdef" for c in signature):
        raise AuthenticationError(GENERIC_AUTH_ERROR)

    secret = session.execute(_GET_GROUP, {"key": api_key}).scalar_one_or_none()
    if secret is None:
        raise AuthenticationError(GENERIC_AUTH_ERROR)

    _check_timestamp(timestamp, settings.auth_timestamp_window_seconds)

    expected = compute_signature(secret, api_key, timestamp, raw_body)
    if not hmac.compare_digest(expected, signature):
        raise AuthenticationError(GENERIC_AUTH_ERROR)

    _consume_token(session, api_key)

    # Two windows: a signature stays valid for ±window around its timestamp,
    # so the nonce must outlive the newest acceptance time by a full window.
    ttl = 2 * settings.auth_timestamp_window_seconds
    inserted = session.execute(
        _INSERT_NONCE, {"signature": signature, "key": api_key, "ttl": ttl}
    ).first()
    if inserted is None:
        raise AuthenticationError(GENERIC_AUTH_ERROR)


def cleanup_expired_nonces(session: Session) -> int:
    return session.execute(_CLEANUP_NONCES).rowcount