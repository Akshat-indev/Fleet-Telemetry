from concurrent.futures import ThreadPoolExecutor


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def test_same_reading_twice_is_duplicate(ingest, payload, reading_count, signed_post):
    client, prefix = ingest
    vid = prefix + "V1"
    reading = payload(vid, 1)

    first = signed_post(client, [reading]).json()
    second = signed_post(client, [reading], offset=-1).json()

    assert first["results"][0]["status"] == "accepted"
    assert second["results"][0]["status"] == "duplicate"
    assert second["duplicates"] == 1
    assert reading_count(vid) == 1


def test_same_batch_twice(ingest, payload, reading_count, signed_post):
    client, prefix = ingest
    vid = prefix + "V1"
    batch = [payload(vid, i) for i in range(1, 11)]

    first = signed_post(client, batch).json()
    second = signed_post(client, batch, offset=-1).json()

    assert first["accepted"] == 10
    assert second["accepted"] == 0 and second["duplicates"] == 10
    assert reading_count(vid) == 10


def test_duplicate_seq_within_one_batch(ingest, payload, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    response = post(client, [payload(vid, 1), payload(vid, 1)]).json()

    assert [r["status"] for r in response["results"]] == ["accepted", "duplicate"]
    assert reading_count(vid) == 1


def test_duplicate_does_not_change_current_state(
    ingest, payload, vehicle_state, signed_post
):
    client, prefix = ingest
    vid = prefix + "V1"
    reading = payload(vid, 1)
    signed_post(client, [reading])
    before = vehicle_state(vid)

    second = signed_post(client, [reading], offset=-1)
    assert second.status_code == 200
    assert second.json()["duplicates"] == 1
    after = vehicle_state(vid)

    assert after["latest_seq"] == before["latest_seq"]
    assert after["latest_device_ts"] == before["latest_device_ts"]
    assert after["soc_pct"] == before["soc_pct"]


def test_twenty_concurrent_clients_same_batch(
    ingest, payload, reading_count, signed_post
):
    client, prefix = ingest
    vid = prefix + "V1"
    batch = [payload(vid, i) for i in range(1, 11)]

    # Distinct timestamps => distinct signatures: every request passes
    # replay protection, so the race is purely about data-level dedup.
    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(
            pool.map(lambda i: signed_post(client, batch, offset=-i), range(20))
        )

    assert all(r.status_code == 200 for r in responses)
    bodies = [r.json() for r in responses]
    # Exactly one client's insert wins per reading; the rest are duplicates.
    assert sum(b["accepted"] for b in bodies) == 10
    assert sum(b["duplicates"] for b in bodies) == 190
    assert reading_count(vid) == 10


def test_concurrent_same_reading_different_batches(
    ingest, payload, reading_count, signed_post
):
    client, prefix = ingest
    vid = prefix + "V1"
    reading = payload(vid, 42)

    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(
            pool.map(lambda i: signed_post(client, [reading], offset=-i), range(20))
        )

    assert all(r.status_code == 200 for r in responses)
    bodies = [r.json() for r in responses]
    assert sum(b["accepted"] for b in bodies) == 1
    assert sum(b["duplicates"] for b in bodies) == 19
    assert reading_count(vid) == 1


def test_concurrent_overlapping_multi_vehicle_batches_deadlock_free(
    ingest, payload, reading_count, signed_post
):
    """Concurrent batches touching the same vehicles in opposite input orders
    must acquire vehicle row locks in canonical order without deadlocking."""
    client, prefix = ingest
    vids = [prefix + f"V{i}" for i in range(1, 6)]

    def make_batch(worker_idx: int):
        ordered = vids if worker_idx % 2 == 0 else list(reversed(vids))
        return [payload(vid, worker_idx + 1) for vid in ordered]

    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(
            pool.map(
                lambda i: signed_post(client, make_batch(i), offset=-i),
                range(12),
            )
        )

    assert all(r.status_code == 200 for r in responses)
    assert sum(r.json()["accepted"] for r in responses) == 60
    for vid in vids:
        assert reading_count(vid) == 12
