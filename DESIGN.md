# Design Notes — Alert Engine (A3), Query API (A4), Security (A2)

Decisions, tradeoffs, and alternatives. Ingestion design (A1) is documented
in the README; this file covers alerting (A3), the query API (A4), and
device authentication / rate limiting (A2).

## 1. Where rules run: inside the ingestion transaction

Three rules (overheat, rapid discharge, geofence) are evaluated synchronously
in the **same transaction** that inserts a reading, after the vehicle row has
been locked by the ingestion upsert / `last_seen_at` update.

- **Why:** the vehicle row lock already serializes all writes for one vehicle.
  Evaluating alerts under that same lock makes "read history → decide →
  create/resolve alert" atomic with the reading itself. No alert can be based
  on a half-committed history, and two concurrent batches for one vehicle
  cannot both open the same alert.
- **Cost:** a few extra indexed queries per reading (frontier, last-10 temps,
  5-minute SOC window, active alerts). All are covered by
  `ix_telemetry_readings_vehicle_ts` and the alerts indexes. At the target
  ~400 readings/sec across 2,000 vehicles this is cheap; per-vehicle lock
  contention is bounded because each vehicle sends ~1 reading / 5 s.
- **Alternative rejected:** a separate polling/queue worker for all rules.
  It adds latency and a second consumer to keep consistent, and the
  assignment forbids new infrastructure (Redis/Kafka/Celery). Offline is the
  one rule that *cannot* be ingestion-driven (absence of data), so it alone
  gets a worker.

## 2. Order-independence: frontier evaluation + reconcile

Devices send out-of-order and replay data. The engine never asks "does this
*arriving* reading trigger a rule?" Instead, after each insert it:

1. Finds the vehicle's **frontier** — newest reading by `(ts DESC, seq DESC)`.
2. Recomputes each rule from device-time-ordered history windows.
3. **Reconciles** against the active alert:
   - trigger true + no active alert → create
   - resolve true + active alert → resolve
   - otherwise → keep (hysteresis)

Consequences:

- A late reading older than the frontier still lands in history and can fill
  window holes, but the decision is always made at the frontier — arrival
  order does not change the resulting active-alert state.
- Duplicates are never evaluated (evaluation runs only when the
  `ON CONFLICT DO NOTHING` insert actually inserted).
- Hysteresis is explicit: overheat needs 3 consecutive > 55 °C to open and
  3 consecutive ≤ 50 °C to resolve; 52 °C does neither. Discharge resolves
  when the computed drop falls back to ≤ 8 points (the high-SOC reading
  slides out of the 5-minute window or charging intervenes). Geofence uses
  the frontier point, with edges/vertices counted as inside (ray casting
  with an on-segment tolerance).

**Known limitation:** the *active* state converges regardless of order, but
historical alert rows can differ under extreme reordering: an episode that
both starts and ends entirely within a late-arriving window may never
materialize as a row, because no evaluation ever saw it at the frontier.
Capturing every historical episode would require replaying evaluations over
the full timeline on every insert — rejected as disproportionate cost for
this service.

## 3. Dedup: partial unique index, not application checks

`uq_alerts_active_vehicle_rule ON alerts (vehicle_id, rule_type)
WHERE status <> 'resolved'` (migration `e929c0d46158`), combined with
`INSERT … ON CONFLICT (vehicle_id, rule_type) WHERE status <> 'resolved'
DO NOTHING`.

- The database — not application logic — guarantees at most one active alert
  per (vehicle, rule), across threads, batches, and API instances.
- Resolved alerts are excluded from the index, so a recurrence after
  resolution opens a fresh alert and full history is preserved.
- **Alternative rejected:** `SELECT`-then-`INSERT` dedup. It races under
  concurrency and would need `SERIALIZABLE` or explicit locking to be safe.
- Verified: 20 concurrent clients each delivering a different triggering
  reading produce exactly one active alert.

## 4. Offline detection: in-process periodic sweep

An asyncio task started in the FastAPI **lifespan** runs `sweep_offline()`
every `OFFLINE_CHECK_INTERVAL_SECONDS` (15 s) via `run_in_threadpool`.

- **Server time, not device time:** staleness is measured against
  `vehicles.last_seen_at`, which ingestion sets to server `now()` for every
  valid reading (including duplicates). A device with a broken clock cannot
  appear online by sending future timestamps.
- **One set-based statement:** `INSERT INTO alerts … SELECT … FROM vehicles
  WHERE last_seen_at < now() - interval FOR UPDATE SKIP LOCKED ON CONFLICT
  DO NOTHING`. No per-vehicle loop; the sweep is one short transaction.
- **Multi-instance safety:** `pg_try_advisory_xact_lock(730001)` — only one
  instance sweeps per cycle; the lock auto-releases at transaction end (no
  leaked locks on crash). `SKIP LOCKED` means the sweep never blocks
  ingestion: if a reading commits first the vehicle looks fresh; if the
  sweep commits first, ingestion resolves the just-opened alert.
- **Resolution:** every valid reading (even a duplicate — it proves the
  device is alive) resolves the active offline alert in the ingestion
  transaction. Worst-case detection latency ≈ threshold + sweep interval.
- **Alternatives rejected:** per-vehicle timers (2,000 timers, restart
  fragility), Redis keyspace expiry / Celery beat (forbidden infrastructure),
  Postgres `LISTEN/NOTIFY` scheduling (no durability benefit here).
- Failures are logged with tracebacks, never swallowed; the loop survives
  transient DB outages and retries next cycle.

## 5. Lifecycle and optimistic concurrency

States: `open → acknowledged → resolved` (DB check constraint). The engine
may resolve an acknowledged alert (the condition cleared — resolution is
system-driven, acknowledgement is operator-driven).

Acknowledge is a single atomic statement:

```sql
UPDATE alerts SET status='acknowledged', version=version+1, ...
WHERE id=:id AND version=:expected AND status='open'
RETURNING id
```

Zero rows updated → diagnose with a follow-up read: missing → 404;
not `open` → 409 invalid transition; otherwise → 409 stale version. The
client re-fetches and decides whether to retry. No row locks are held while
the operator thinks; no lost updates are possible. Verified with 10
simultaneous acks at the same version: exactly one 200, nine 409.

## 6. Rule parameters

All thresholds are configuration (`app/config.py`), not hard-coded in SQL:
`OFFLINE_AFTER_SECONDS=90`, `OFFLINE_CHECK_INTERVAL_SECONDS=15`,
`DISCHARGE_DROP_PCT=8`, `DISCHARGE_WINDOW_SECONDS=300`,
`GEOFENCE_POLYGON` (JSON `[lat, lng]` vertices; default: Chennai rectangle).
Overheat thresholds (55/50/3) are constants in `app/alerts/rules.py` — the
assignment fixes them; promoting them to settings is trivial if needed.

Rules are **pure functions** over plain values (`app/alerts/rules.py`),
unit-tested without a database; the engine (`engine.py`) owns all SQL.

## 7. Testing strategy

- Pure unit tests for rule math (thresholds, consecutive-run breaks,
  charging interruption, window edges, polygon edges/vertices).
- Integration tests against real PostgreSQL (no mocks): trigger, resolve,
  hysteresis, out-of-order equivalence, duplicate no-ops, recurrence after
  resolution, offline sweep/resolve cycles, ack conflicts, and concurrency
  (20 triggering clients, 10 racing acks).
- Offline tests call `sweep_offline()` directly — `TestClient` without a
  context manager does not run the lifespan, so the watchdog never fires
  during tests and results are deterministic. Assertions are scoped to each
  test's vehicle-id prefix because the sweep legitimately touches every
  stale vehicle in a shared dev database.

## 8. Query API (A4): keyset pagination

`GET /v1/vehicles` sorts by `vehicle_id ASC` and pages with
`WHERE vehicle_id > :cursor LIMIT n+1` — never OFFSET.

- **Why this sort key:** `vehicle_id` is unique, so it is its own
  tie-breaker; no composite cursor is needed and the order is total and
  immutable (ids never change). OFFSET would re-scan skipped rows (O(n²)
  over a full listing) and would shift pages when vehicles are inserted
  mid-listing; the keyset predicate is immune to insertions before the
  cursor. The n+1 fetch yields `has_more` without a second COUNT query.
- **Cursor format:** URL-safe base64 of `{"vehicle_id": ...}` — opaque to
  clients, validated server-side (bad encoding or wrong shape → 422).
- **Status is derived, not stored:** one shared SQL `CASE` (offline >
  charging > active > idle) used by both the listing filter and the summary
  counts, so the endpoints can never disagree. Storing status would add a
  write-path obligation (every reading and every offline sweep would have
  to maintain it) for a value that is a pure function of existing columns.
- **No new index:** the unique `vehicle_id` index serves the keyset scan;
  the vehicles table has one row per vehicle (2,000 today), so status/bbox
  filters over it are cheap. Adding indexes here would be premature.

## 9. Query API: min/max downsampling in SQL

`GET /v1/vehicles/{id}/telemetry?from=&to=&points=` splits the range into
`(points−2)//2` buckets and returns, per bucket, the **actual rows** with
the lowest and highest `soc_pct` (`DISTINCT ON (bucket)`), UNION'd with the
exact first and last rows, re-sorted by `(ts, seq)`.

- **Why min/max over LTTB:** min/max guarantees spike/trough preservation
  (the extremum row is returned verbatim, with all its fields), is pure SQL
  (no Python pass over 17,000 rows), and is easy to verify. LTTB preserves
  visual shape better for smooth series but can discard a narrow extreme;
  for telemetry alerting review, keeping the true peaks matters more.
- **Correctness details:** UNION (not UNION ALL) deduplicates rows chosen
  by several branches (flat series, bucket min = range first point, etc.);
  ties within a bucket break by `(ts, seq)` so results are deterministic;
  the bucket count formula caps output at `points` including endpoints.
- **Bounds:** `from`/`to` required, tz-aware, `from < to`, window ≤ 31 days;
  raw mode (no `points`) caps at 10,000 rows with a `(ts, seq)` keyset
  cursor. The `(vehicle_id, ts)` index serves the range scan; the bucketed
  CTE is scanned four times, which is cheap at the assignment's scale
  (17,000 rows / 24 h) and measured in the tests.

## 10. Query API: fleet summary and the seq-counter columns

`GET /v1/fleet/summary` runs exactly two queries: one aggregate over
`vehicles` (status counts, avg SOC, completeness) and one `GROUP BY` over
open alerts (`ix_alerts_rule_type_status`).

- **The one schema addition:** `vehicles.min_seq / max_seq / received_count`,
  maintained in the ingestion transaction **only on a genuine insert**
  (duplicates and rollbacks never count), backfilled by migration
  `3e21795c6853`. Fleet completeness is then
  `sum(received_count) / sum(max_seq − min_seq + 1)` over ~2,000 rows.
- **Why justified:** the alternative — `GROUP BY vehicle_id` over
  `telemetry_readings` — touches all 5M rows on every request. Measured at
  5,000,027 readings: the naive plan is a 492 ms parallel seq scan (3× over
  the 150 ms target); the counter-based aggregate runs in 1.37 ms and the
  alerts index-only scan in 0.29 ms. End-to-end HTTP p95 = 11.6 ms
  (200 samples after warm-up; see README for methodology). No caching, no
  Redis, no staleness — counters commit with the reading itself.
- **Consistency:** counters live in the same transaction as the insert, so
  they can never drift from history; the per-batch gap report still derives
  its numbers from the readings table, and the two agree by construction.
- Empty fleet → zeros and nulls (avg SOC, completeness), never division by
  zero. `sum(bigint)` arrives as `numeric` from PostgreSQL; the service
  coerces to int/float so response types are stable.

## 11. Query API: known limitations

- One operating polygon at a time (config), not multi-region geofencing.
- History downsampling optimizes `soc_pct` extremes; a spike visible only
  in another field (e.g. temperature) may not be selected unless it
  coincides with a SOC extreme or a bucket endpoint.
- Raw history mode caps at 10,000 rows per page; very large raw exports
  must page with the cursor. History windows are capped at 31 days.
- Vehicle listing sorts by `vehicle_id` only; sorting by last-seen or SOC
  would need a composite keyset cursor and a supporting index.
- Summary reflects committed state at query time (two separate queries, no
  snapshot export); under heavy ingestion the vehicles and alerts halves
  can be milliseconds apart. Acceptable for a dashboard; a repeatable-read
  transaction would fix it if exact consistency were required.

## 12. Security (A2): signing protocol and secret handling

The assignment requires HMAC request signing but does not prescribe a wire
format, so the protocol is defined explicitly (README has the client guide):
`X-Signature = hex HMAC-SHA256(secret, api_key + "\n" + timestamp + "\n" + raw_body)`.

- **Raw body, not reserialized JSON.** The endpoint reads `await request.body()`
  and authenticates *before* any JSON parsing. JSON reserialization is not
  byte-stable (whitespace, key order, float formatting), so signing a parsed
  object would either reject legitimate requests or force a canonicalization
  ruleset on every client. Signing the exact transmitted bytes is simpler
  and stricter: any mutation of the payload invalidates the signature.
- **Timestamp in the signed message.** `X-Timestamp` (integer epoch seconds)
  is part of the HMAC input, so an attacker cannot slide a captured request
  to a fresh timestamp without the secret. The server accepts ±300 s
  (clock skew) and rejects non-canonical integer strings.
- **One generic 401 for every failure.** Missing header, unknown key, stale
  timestamp, bad signature, replay — all return the identical
  `{"detail": "authentication failed"}`. Distinct messages would be an
  oracle (key enumeration, timing-window probing). Signature comparison
  uses `hmac.compare_digest` (constant time); a test spies on the call to
  prove it is actually used.
- **Secret storage.** HMAC verification must recompute the MAC, so the
  secret is stored retrievably (unlike passwords, which are one-way hashed).
  It is generated with `secrets.token_hex(32)`, printed once at provisioning,
  never returned by any endpoint, never logged, and absent from all error
  paths (tests assert its absence from responses). Encryption at rest with
  a KMS-managed key is the production hardening step; out of scope here and
  noted as a limitation.
- **Scope.** A2 protects the device write path (`POST /v1/telemetry/batch`).
  Read/ops endpoints keep their existing behavior — the assignment's auth
  requirement is about device ingestion, and changing A1/A3/A4 semantics
  was explicitly out of scope.

## 13. Security: replay protection — the signature is the nonce

`request_nonces` has the **signature as its primary key**. The signature
already covers key + timestamp + exact body, so it is a perfect request
identifier — no separate nonce header or client-side nonce state is needed.

- **Concurrency arbitrator:** `INSERT … ON CONFLICT (signature) DO NOTHING
  RETURNING`. Twenty concurrent copies of one request race on the primary
  key; exactly one INSERT returns a row, the rest get 401. The guarantee
  lives in PostgreSQL, not in application check-then-insert logic (which
  would race). Verified with 20 threads and across two uvicorn processes.
- **TTL = 2 × window (600 s).** A signature with timestamp *t* is acceptable
  until *t + 300*. Worst case, it is first used at *t − 300*, so the nonce
  must live until *(t − 300) + 600 = t + 300* — exactly the last moment the
  signature could be accepted. A shorter TTL would reopen a replay hole at
  the future edge of the window; a longer one only wastes storage.
- **Cleanup:** the existing offline watchdog also deletes expired nonces
  each cycle (indexed by `expires_at`). No new worker, no new infrastructure.
- **Tradeoffs:** (1) a byte-identical legitimate retry signed in the same
  second is indistinguishable from a replay and gets 401 — clients must
  re-sign retries with a fresh timestamp (data-level dedup makes the retry
  safe anyway); (2) after a nonce expires and is cleaned, the same signature
  would pass again *if its timestamp were still in-window* — impossible by
  the TTL math above, so the only reuse path is outside the acceptance
  window, which the timestamp check already rejects.

## 14. Security: distributed token bucket in one UPDATE

`rate_limit_buckets` holds one row per API key (`tokens`, `last_refill`).
Refill and consumption are a **single atomic statement**:

```sql
UPDATE rate_limit_buckets
SET tokens = LEAST(:capacity, tokens + elapsed * :refill) - 1, last_refill = now()
WHERE api_key = :key AND LEAST(:capacity, tokens + elapsed * :refill) >= 1
RETURNING tokens
```

- **Why this is safe across instances:** the UPDATE takes a row lock; under
  READ COMMITTED, a waiter re-evaluates the `WHERE` clause against the
  committed row after the lock releases, so two concurrent consumers can
  never both see the last token. No SELECT-then-UPDATE, no application
  mutex, no Redis — the database row *is* the distributed lock. Verified
  with 20 concurrent requests (exactly `capacity` acceptances) and with two
  independent uvicorn processes sharing one database.
- **Ordering:** rate limit runs *before* the nonce insert, so a 429 never
  burns the request's nonce — the client can retry the identical request
  after `Retry-After`. Any later failure (replay) rolls the transaction
  back, refunding the consumed token. Failed signatures never reach the
  bucket at all.
- **Retry-After:** `max(1, ceil((1 − available_tokens) / refill_rate))` —
  the seconds until one whole token exists, computed from the same row.
- **Defaults (120 burst / 20 per second):** the assignment's ~400
  readings/sec arrives as batches, so per-group request rates are modest;
  the bucket absorbs legitimate bursts while capping sustained abuse.
  Both values are configuration, per group isolation is inherent (one row
  per key).
- **Alternatives rejected:** in-memory counters (break with >1 instance —
  the assignment explicitly tests two), Redis (forbidden infrastructure),
  sliding-window log tables (write amplification per request; the bucket
  is O(1) storage per group).

## 15. Security: verification and limitations

Every §8 requirement is covered by real-database tests (no mocked DB):
header/HMAC failure modes, frozen-clock ±300/±301 boundaries, raw-byte
sensitivity, constant-time comparison, 20-thread replay race, nonce TTL
and cleanup, capacity/refill/Retry-After math, 20-thread no-overdraw,
per-group isolation, token refund on replay, provisioning CLI, and two
independent uvicorn instances proving shared bucket + shared replay state.

Limitations:
- Secrets are stored retrievably (HMAC requires it); production would add
  encryption at rest and a rotation flow (dual-secret overlap), neither of
  which the assignment requires.
- No per-vehicle identity within a group: any holder of the group secret
  can send for any vehicle id. Vehicle-level keys would multiply credential
  management 2,000×; group-level is the assignment's model.
- The timestamp window trusts server clock sync (NTP); ±300 s is generous
  for device drift but means a captured request is replayable within the
  window *until first use* — after first use the nonce blocks it.
- Rate limiting is per group, not per IP; a leaked secret can consume the
  group's whole budget (mitigation: rotate the group secret).