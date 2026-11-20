from concurrent.futures import ThreadPoolExecutor


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def test_same_reading_twice_is_duplicate(ingest, payload, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    reading = payload(vid, 1)

    first = post(client, [reading]).json()
    second = post(client, [reading]).json()

    assert first["results"][0]["status"] == "accepted"
    assert second["results"][0]["status"] == "duplicate"
    assert second["duplicates"] == 1
    assert reading_count(vid) == 1


def test_same_batch_twice(ingest, payload, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    batch = [payload(vid, i) for i in range(1, 11)]

    first = post(client, batch).json()
    second = post(client, batch).json()

    assert first["accepted"] == 10
    assert second["accepted"] == 0 and second["duplicates"] == 10
    assert reading_count(vid) == 10


def test_duplicate_seq_within_one_batch(ingest, payload, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    response = post(client, [payload(vid, 1), payload(vid, 1)]).json()

    assert [r["status"] for r in response["results"]] == ["accepted", "duplicate"]
    assert reading_count(vid) == 1


def test_duplicate_does_not_change_current_state(ingest, payload, vehicle_state):
    client, prefix = ingest
    vid = prefix + "V1"
    reading = payload(vid, 1)
    post(client, [reading])
    before = vehicle_state(vid)

    post(client, [reading])
    after = vehicle_state(vid)

    assert after["latest_seq"] == before["latest_seq"]
    assert after["latest_device_ts"] == before["latest_device_ts"]
    assert after["soc_pct"] == before["soc_pct"]


def test_twenty_concurrent_clients_same_batch(ingest, payload, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    batch = [payload(vid, i) for i in range(1, 11)]

    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(lambda _: post(client, batch), range(20)))

    assert all(r.status_code == 200 for r in responses)
    bodies = [r.json() for r in responses]
    # Exactly one client's insert wins per reading; the rest are duplicates.
    assert sum(b["accepted"] for b in bodies) == 10
    assert sum(b["duplicates"] for b in bodies) == 190
    assert reading_count(vid) == 10


def test_concurrent_same_reading_different_batches(ingest, payload, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    reading = payload(vid, 42)

    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(lambda _: post(client, [reading]), range(20)))

    bodies = [r.json() for r in responses]
    assert sum(b["accepted"] for b in bodies) == 1
    assert sum(b["duplicates"] for b in bodies) == 19
    assert reading_count(vid) == 1