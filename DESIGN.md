# Design Notes — Alert Engine (A3)

Decisions, tradeoffs, and alternatives for the Phase 4 alert engine.
Ingestion design (A1) is documented in the README; this file focuses on
alerting.

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