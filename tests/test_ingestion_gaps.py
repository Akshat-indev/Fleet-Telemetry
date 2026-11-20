def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def summary_for(body, vehicle_id):
    return next(v for v in body["vehicles"] if v["vehicle_id"] == vehicle_id)


def test_sequential_readings_no_gaps(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    body = post(client, [payload(vid, i) for i in range(1, 4)]).json()

    summary = summary_for(body, vid)
    assert summary["missing_count"] == 0
    assert summary["missing_seqs"] == []
    assert summary["completeness_pct"] == 100.0


def test_single_missing_sequence(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    body = post(client, [payload(vid, 101), payload(vid, 102), payload(vid, 105)]).json()

    summary = summary_for(body, vid)
    assert summary["min_seq"] == 101 and summary["max_seq"] == 105
    assert summary["received_count"] == 3
    assert summary["missing_count"] == 2
    assert summary["missing_seqs"] == [103, 104]
    assert summary["completeness_pct"] == 60.0


def test_multiple_missing_sequences(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    body = post(client, [payload(vid, 1), payload(vid, 4), payload(vid, 8)]).json()

    summary = summary_for(body, vid)
    assert summary["missing_count"] == 5
    assert summary["missing_seqs"] == [2, 3, 5, 6, 7]


def test_missing_sequence_arrives_later(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 101), payload(vid, 102), payload(vid, 105)])

    body = post(client, [payload(vid, 103)]).json()
    summary = summary_for(body, vid)

    assert body["results"][0]["status"] == "accepted"
    assert summary["gaps_filled"] == [103]
    assert summary["missing_count"] == 1
    assert summary["missing_seqs"] == [104]
    assert summary["completeness_pct"] == 80.0

    body = post(client, [payload(vid, 104)]).json()
    summary = summary_for(body, vid)
    assert summary["gaps_filled"] == [104]
    assert summary["missing_count"] == 0
    assert summary["completeness_pct"] == 100.0


def test_duplicate_does_not_report_gap_filled(ingest, payload):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1), payload(vid, 3)])

    body = post(client, [payload(vid, 1)]).json()  # duplicate, not a fill
    summary = summary_for(body, vid)
    assert body["duplicates"] == 1
    assert summary["gaps_filled"] == []
    assert summary["missing_seqs"] == [2]