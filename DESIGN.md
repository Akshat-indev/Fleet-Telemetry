# VoltGrid Fleet Telemetry — Design & Reasoning (Part C)

## 1. Scale (100× growth: 200,000 vehicles / ~40,000 readings/s)

Today's single-node deployment targets 2,000 vehicles (~400 readings/s) and sustains ~130 accepted readings/s on a single uvicorn worker over Docker Desktop (`POST /v1/telemetry/batch` runs ~8 SQL queries and commits once per reading in `app/ingestion.py`). Scaling 100× to 40,000 readings/s (~3.45B rows/day) breaks the system in this order:

1. **Per-reading transaction & round-trip overhead (`app/ingestion.py`).** Each reading commits independently with vehicle upsert, history insert, state compare-and-set, seq-counter update, alert window reads, and outbox insert. At 40k/s, WAL `fsync` commits and connection-pool wait times saturate immediately.
   *Fix:* Switch to set-based batch SQL (`UNNEST` / staging table) committing once per batch (with savepoint fallback only on constraint violation), horizontally scale stateless API replicas behind a load balancer, and place PgBouncer (transaction pooling) in front of PostgreSQL.
2. **Unpartitioned `telemetry_readings` B-tree & autovacuum bloat.** At 3.45B rows/day, `uq_telemetry_readings_vehicle_seq` and `ix_telemetry_readings_vehicle_ts` exceed RAM, turning every insert into random disk I/O while autovacuum falls behind.
   *Fix:* Partition `telemetry_readings` by day (`RANGE (ts)`) so only today's hot partition indexes stay resident in RAM and old partitions drop in O(1).
3. **`fleet_events` outbox + SSE polling and `vehicles` summary scan.** Writing 40k+ outbox rows/s while N SSE connections poll PostgreSQL (and the watchdog runs `DELETE` retention) saturates IOPS; `GET /v1/fleet/summary` scanning 200,000 `vehicles` rows grows from ~1.5 ms to >150 ms under write lock contention.
   *Fix:* Stream outbox changes via CDC (Debezium) into Kafka/NATS for SSE fan-out; serve `/v1/fleet/summary` and `/v1/vehicles` from a read replica (or pre-aggregated status counters).
4. **Single-primary PostgreSQL write ceiling & hot auth rows.** Even batched, 40k/s durable writes plus per-request `request_nonces` PK inserts and `rate_limit_buckets` row locks hit single-node WAL throughput.
   *Fix:* Move rate limiting and short-TTL replay nonces to Redis (`EVAL` token bucket + `SET NX EX`), and shard `vehicles`, `telemetry_readings`, and `alerts` by `hash(vehicle_id)` across PostgreSQL/Citus shards.

## 2. Delivery Guarantees

**Transport is at-least-once; stored state is effectively-once (idempotent).**

- `telemetry_readings` enforces `UNIQUE (vehicle_id, seq)` (`uq_telemetry_readings_vehicle_seq`) with `INSERT ... ON CONFLICT DO NOTHING RETURNING id` (`app/ingestion.py`). Only a genuine insert updates `vehicles.min_seq/max_seq/received_count`, runs `evaluate_reading()`, and publishes `fleet_events`—all in one ACID transaction. Proven under 20 concurrent identical-batch clients (`tests/test_ingestion_idempotency.py`).
- Current state updates via atomic compare-and-set (`UPDATE vehicles ... WHERE latest_device_ts IS NULL OR latest_device_ts < :ts`), so retries and late arrivals never overwrite newer state.
- Retrying clients must re-sign with a fresh `X-Timestamp` because `request_nonces` rejects byte-identical replays within the same second.

**Where it can still go wrong:** (a) if a device resets its `seq` counter to 1 after a firmware wipe, new readings silently collide with historical `(vehicle_id, seq)` and are dropped as duplicates; (b) if two *different* payloads share `(vehicle_id, seq)`, first write wins silently; (c) if a client discards a batch on network timeout before server commit without retrying, data is lost (device-side buffering is required for true at-least-once).

## 3. Time

We separate two clocks with distinct trust boundaries:

- **Device timestamp (`ts`)** governs vehicle current state (`latest_device_ts`), history ordering, and stateful alert windows (`overheat`, `rapid_discharge`, `geofence_breach`). Mobile networks reorder and delay packets by minutes, so arrival time cannot reconstruct physical timelines.
- **Server receive time (`now()`)** governs liveness (`vehicles.last_seen_at` → `offline` rule), security (`X-Timestamp` ±300s window in `app/security.py`), `received_at`, and `fleet_events` retention. A broken or malicious device clock must never spoof online status or bypass replay protection.

**Clock 25 seconds fast:** Handled seamlessly—`X-Timestamp` passes the ±300s auth window, `ts` remains self-consistent for that vehicle's `latest_device_ts` and 5-minute discharge window, and `offline` relies on server `last_seen_at`.

**Clock wrong by a day:** If `X-Timestamp` is off by a day, `app/security.py` rejects the request immediately with `401`. If a gateway signs with valid time while the reading payload's `ts` is 1 day in the **future**, `TelemetryReadingIn` currently accepts any valid tz-aware ISO-8601 `ts`—a known gap: that single reading pins `vehicles.latest_device_ts` 24 hours ahead, blocking legitimate readings from updating current state for a day. Production fix: reject or quarantine payload `ts > now() + 5m` while continuing to accept past timestamps for buffered late telemetry.

## 4. Storage and Cost (~35M raw rows/day)

At 400 readings/s, `telemetry_readings` grows by ~34.56M rows/day (~7–9 GB/day with indexes, ~2.8 TB/year). Proposed three-tier policy:

- **Hot (0–30 days):** Daily range-partitioned raw 5-second rows in PostgreSQL (~250 GB) for live alerting, raw exports, and incident debugging.
- **Warm (31–365 days):** A nightly rollup aggregates closed daily partitions into `telemetry_rollups_1m` (1-minute buckets storing `min/max/avg/first/last` of `soc_pct`, `battery_temp_c`, `speed_kmh`, last `(lat, lng)`, `max(odometer_km)`, union of `fault_codes`, and `reading_count`), then drops the raw partition (`DROP TABLE`, zero vacuum cost), cutting storage ~12× (~200 GB/year).
- **Cold (>30 days raw archive):** Export detached raw partitions as Zstd-compressed Parquet to object storage (S3/GCS, ~10× compression) before dropping; retain `alerts` and `vehicles` indefinitely.

**What an operator can no longer see after raw drops (>30 days):** Exact per-`seq` missing sequence numbers (only per-minute counts remain), sub-minute spikes or exact 3-consecutive-reading overheat trigger timing, intra-minute GPS paths / brief geofence edge crossings, and exact same-tick correlation between two metrics (e.g., whether peak speed and peak battery temperature occurred on the exact same 5-second reading).

## 5. Threat Model (Device Ingestion API)

1. **Spoofed or tampered telemetry** (forging readings to trigger false alerts or corrupt fleet state): Mitigated by `HMAC-SHA256(secret, api_key + "\n" + timestamp + "\n" + raw_body)` verified over raw wire bytes *before* JSON parsing with constant-time `hmac.compare_digest` and uniform `401` responses (no key-enumeration oracle), backed by strict type/range Pydantic validators (`app/schemas.py`) and independent PostgreSQL `CHECK` constraints on both `telemetry_readings` and `vehicles` (migration `7b3c9e1a2d4f`).
2. **Replay attacks** (re-sending captured legitimate signed batches): Mitigated by signing `X-Timestamp` (±300s window) and inserting every accepted signature as PK into `request_nonces` (`ON CONFLICT DO NOTHING`, TTL = 2× window = 600s, swept by watchdog)—verified across 20 concurrent threads and 2 API instances.
3. **Volumetric / DoS flooding** (compromised key starving DB or SSE memory): Mitigated by a distributed PostgreSQL token bucket (`rate_limit_buckets`, single atomic `UPDATE` returning `429` + `Retry-After`, evaluated *before* nonce insert so `429` never burns a nonce), hard 500-reading batch limit (`422`), and bounded `Coalescer` memory (≤2 entries/vehicle) on `/v1/stream`.

## 6. Key Trade-offs

1. **No Redis/Kafka/Celery:** All state, distributed rate limits, replay nonces, and the SSE outbox live in PostgreSQL. *Consequence:* Single-command `docker compose up` with full ACID guarantees, at the cost of sharing one PostgreSQL write primary (limits scale before Q1 refactoring).
2. **Frontier-only alert evaluation (`app/alerts/engine.py`):** Rules evaluate at the vehicle's newest `(ts, seq)` frontier rather than replaying full history on every late reading. *Consequence:* Active alert state is order-independent and fast per insert, but a transient episode that both starts and ends inside a late-arriving historical window may never create a historical alert row.
3. **Group-level HMAC keys without per-vehicle identity or KMS encryption-at-rest:** One key/secret per device group (`device_groups`) stored retrievably for HMAC recomputation. *Consequence:* Simple provisioning without 2,000-device PKI, but a leaked group secret lets an attacker forge readings for any vehicle in that group until rotated.

## 7. A Real Bug We Hit and Proved Fixed

**Bug:** Under concurrent ingestion, `GET /v1/stream` emitted false `stream_gap` notices and permanently skipped committed events.

**Root cause & discovery:** During our first concurrent 2,000-vehicle simulator run, live SSE subscribers saw spurious gaps. PostgreSQL `BIGINT GENERATED ALWAYS AS IDENTITY` reserves `fleet_events.id` at `INSERT` time, but rows become visible at `COMMIT` time. When transaction A reserved `id = N` and committed slightly after transaction B (`id = N + 1`), our poller saw `rows[0].id > cursor + 1`, assumed `id = N` was missing, emitted `stream_gap`, and advanced `cursor` past `N`—permanently skipping `N`.

**Fix & proof:** Updated `app/stream.py` to advance `cursor` only through **contiguous** IDs and hold any detected hole for `SSE_GAP_GRACE_SECONDS` (30s) before emitting `stream_gap` (distinguishing `retention_expired` from `events_unavailable`). Proven via `tests/test_stream_sse.py::test_in_flight_commit_is_not_a_gap` (holds an uncommitted insert for `id = N`, commits `N + 1, N + 2`, asserts 0 early delivery and 0 gaps, then commits `N` and asserts exact `[N, N+1, N+2]` delivery) and `test_gap_reported_when_retention_trims_lagging_cursor`.
