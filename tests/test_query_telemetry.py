"""Telemetry history tests. Per-vehicle data is naturally isolated by the
test's vehicle-id prefix. Bulk datasets are inserted with set-based SQL
against the real database (still real PostgreSQL, no mocks)."""

from datetime import datetime, timedelta, timezone

from sqlalchemy import text

T0 = datetime(2026, 11, 20, 0, 0, 0, tzinfo=timezone.utc)
DAY = {"from": "2026-11-20T00:00:00Z", "to": "2026-11-21T00:00:00Z"}


def post(client, readings):
    r = client.post("/v1/telemetry/batch", json={"readings": readings})
    assert r.status_code == 200
    return r.json()


def history(client, vid, **params):
    return client.get(f"/v1/vehicles/{vid}/telemetry", params={**DAY, **params})


def bulk_insert(vid, count, start_seq=1, interval_seconds=5, soc_expr="50 + (s % 7)"):
    from app.database import engine

    with engine.begin() as conn:
        db_id = conn.execute(
            text("SELECT id FROM vehicles WHERE vehicle_id = :v"), {"v": vid}
        ).scalar_one()
        conn.execute(
            text(f"""
                INSERT INTO telemetry_readings
                    (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh,
                     lat, lng, odometer_km, charging, fault_codes)
                SELECT :id, s, :t0 + make_interval(secs => s * :step),
                       {soc_expr}, 35, 20, 13.08, 80.27, 1000 + s * 0.01,
                       false, '[]'::jsonb
                FROM generate_series(CAST(:start AS bigint), CAST(:end AS bigint)) s
                ON CONFLICT ON CONSTRAINT uq_telemetry_readings_vehicle_seq DO NOTHING
            """),
            {"id": db_id, "t0": T0, "step": interval_seconds,
             "start": start_seq, "end": start_seq + count - 1},
        )


def test_valid_range_chronological_order(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    # Arrive out of order; output must be device-time order.
    post(client, [
        payload(vid, 3, ts=T0 + timedelta(seconds=10)),
        payload(vid, 1, ts=T0),
        payload(vid, 2, ts=T0 + timedelta(seconds=5)),
    ])
    body = history(client, vid).json()
    assert body["raw_count"] == 3 and body["returned_count"] == 3
    assert body["downsampled"] is False
    assert [r["seq"] for r in body["readings"]] == [1, 2, 3]
    assert body["readings"][0]["ts"].startswith("2026-11-20T00:00:00")


def test_time_range_filtering(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [
        payload(vid, 1, ts=T0 + timedelta(hours=1)),
        payload(vid, 2, ts=T0 + timedelta(hours=5)),
        payload(vid, 3, ts=T0 + timedelta(hours=9)),
    ])
    body = history(client, vid, **{
        "from": "2026-11-20T04:00:00Z", "to": "2026-11-20T06:00:00Z",
    }).json()
    assert body["raw_count"] == 1
    assert [r["seq"] for r in body["readings"]] == [2]


def test_empty_history(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1, ts=T0)])
    body = history(client, vid, **{
        "from": "2026-11-20T12:00:00Z", "to": "2026-11-20T13:00:00Z",
    }).json()
    assert body["raw_count"] == 0 and body["readings"] == []
    assert body["downsampled"] is False and body["next_cursor"] is None


def test_unknown_vehicle_404(ingest):
    client, prefix = ingest
    assert history(client, prefix + "NOPE").status_code == 404


def test_range_validation(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1, ts=T0)])
    # from >= to
    assert history(client, vid, **{
        "from": "2026-11-20T05:00:00Z", "to": "2026-11-20T04:00:00Z",
    }).status_code == 422
    # naive timestamps
    assert history(client, vid, **{
        "from": "2026-11-20T00:00:00", "to": "2026-11-21T00:00:00Z",
    }).status_code == 422
    # window beyond 31 days
    assert history(client, vid, **{
        "from": "2026-01-01T00:00:00Z", "to": "2026-11-21T00:00:00Z",
    }).status_code == 422
    # missing required params
    assert client.get(f"/v1/vehicles/{vid}/telemetry").status_code == 422


def test_points_validation(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1, ts=T0)])
    assert history(client, vid, points=3).status_code == 422
    assert history(client, vid, points=5001).status_code == 422
    assert history(client, vid, points=10, cursor="x").status_code == 422
    assert history(client, vid, cursor="garbage!!").status_code == 422


def test_minmax_preserves_spikes_troughs_and_endpoints(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    readings = [
        payload(vid, s, ts=T0 + timedelta(seconds=s * 60),
                soc_pct=95 if s == 50 else (10 if s == 70 else 50))
        for s in range(1, 101)
    ]
    post(client, readings)

    body = history(client, vid, points=10).json()
    assert body["downsampled"] is True and body["raw_count"] == 100
    assert body["returned_count"] <= 10
    assert body["bucket_seconds"] > 0
    seqs = [r["seq"] for r in body["readings"]]
    assert len(seqs) == len(set(seqs))          # no duplicate points
    assert seqs == sorted(seqs)                 # chronological
    assert seqs[0] == 1 and seqs[-1] == 100     # first and last preserved
    assert 50 in seqs and 70 in seqs            # spike and trough survive
    by_seq = {r["seq"]: r for r in body["readings"]}
    assert by_seq[50]["soc_pct"] == 95 and by_seq[70]["soc_pct"] == 10


def test_minmax_flat_series_no_duplicates(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [
        payload(vid, s, ts=T0 + timedelta(seconds=s * 60), soc_pct=50)
        for s in range(1, 51)
    ])
    body = history(client, vid, points=8).json()
    seqs = [r["seq"] for r in body["readings"]]
    assert len(seqs) == len(set(seqs))
    assert seqs[0] == 1 and seqs[-1] == 50
    assert body["returned_count"] <= 8


def test_small_result_not_downsampled(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, s, ts=T0 + timedelta(seconds=s)) for s in range(1, 6)])
    body = history(client, vid, points=100).json()
    assert body["downsampled"] is False and body["returned_count"] == 5


def test_raw_mode_bounded_with_keyset_cursor(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1, ts=T0)])
    bulk_insert(vid, 10_001, start_seq=2, interval_seconds=1)

    page1 = history(client, vid).json()
    assert page1["raw_count"] == 10_002
    assert page1["returned_count"] == 10_000
    assert page1["next_cursor"] is not None
    page2 = history(client, vid, cursor=page1["next_cursor"]).json()
    assert page2["returned_count"] == 2

    seqs = [r["seq"] for r in page1["readings"]] + [r["seq"] for r in page2["readings"]]
    assert seqs == list(range(1, 10_003))  # complete, ordered, no duplicates


def test_large_history_downsampled_bounded(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1, ts=T0)])
    # ~24h at 5s cadence: 17,000 readings, with one spike and one trough.
    bulk_insert(
        vid, 16_999, start_seq=2, interval_seconds=5,
        soc_expr="CASE WHEN s = 8500 THEN 99 WHEN s = 12000 THEN 5 "
                 "ELSE 50 + (s % 13) - (s % 7) END",
    )
    body = history(client, vid, points=500).json()
    assert body["raw_count"] == 17_000
    assert body["downsampled"] is True
    assert body["returned_count"] <= 500
    seqs = [r["seq"] for r in body["readings"]]
    assert len(seqs) == len(set(seqs)) and seqs == sorted(seqs)
    assert seqs[0] == 1 and seqs[-1] == 17_000
    assert 8500 in seqs and 12000 in seqs