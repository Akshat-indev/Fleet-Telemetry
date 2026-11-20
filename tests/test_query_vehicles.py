"""Vehicle listing runs against a fresh scratch database per test, so
pagination and filter assertions are exact (no other fleet data)."""

from sqlalchemy import text


def seed(client, payload, readings):
    r = client.post("/v1/telemetry/batch", json={"readings": readings})
    assert r.status_code == 200
    return r.json()


def list_all(client, **params):
    vehicles, cursor = [], None
    while True:
        query = dict(params)
        if cursor:
            query["cursor"] = cursor
        r = client.get("/v1/vehicles", params=query)
        assert r.status_code == 200
        body = r.json()
        vehicles.extend(body["vehicles"])
        if not body["has_more"]:
            assert body["next_cursor"] is None
            return vehicles
        cursor = body["next_cursor"]


def test_empty_fleet(isolated_client):
    client, _ = isolated_client
    r = client.get("/v1/vehicles")
    assert r.status_code == 200
    assert r.json() == {
        "vehicles": [], "limit": 50, "has_more": False, "next_cursor": None
    }


def test_listing_sorted_and_fields(isolated_client, payload):
    client, _ = isolated_client
    seed(client, payload, [payload("V-B", 1), payload("V-A", 1)])
    body = client.get("/v1/vehicles").json()
    assert [v["vehicle_id"] for v in body["vehicles"]] == ["V-A", "V-B"]
    v = body["vehicles"][0]
    assert v["status"] == "active"  # fresh reading, speed 23.5 > 0
    assert v["soc_pct"] == 71.4 and v["received_count"] == 1
    assert v["latest_seq"] == 1 and v["last_seen_at"] is not None


def test_page_size_validation(isolated_client):
    client, _ = isolated_client
    assert client.get("/v1/vehicles", params={"limit": 0}).status_code == 422
    assert client.get("/v1/vehicles", params={"limit": 201}).status_code == 422
    assert client.get("/v1/vehicles", params={"limit": "abc"}).status_code == 422


def test_keyset_pagination_no_gaps_or_duplicates(isolated_client, payload):
    client, _ = isolated_client
    ids = [f"V-{i:03d}" for i in range(25)]
    seed(client, payload, [payload(v, 1) for v in ids])

    seen, cursor, page_sizes = [], None, []
    while True:
        params = {"limit": 10}
        if cursor:
            params["cursor"] = cursor
        body = client.get("/v1/vehicles", params=params).json()
        page_sizes.append(len(body["vehicles"]))
        seen.extend(v["vehicle_id"] for v in body["vehicles"])
        if not body["has_more"]:
            break
        cursor = body["next_cursor"]

    assert page_sizes == [10, 10, 5]
    assert seen == ids  # sorted, complete, no duplicates


def test_insert_between_pages_does_not_duplicate(isolated_client, payload):
    client, _ = isolated_client
    seed(client, payload, [payload(f"V-{i:03d}", 1) for i in range(10)])
    page1 = client.get("/v1/vehicles", params={"limit": 5}).json()
    cursor = page1["next_cursor"]

    # New vehicles arrive on both sides of the cursor while paging.
    seed(client, payload, [payload("V-000-NEW", 1), payload("V-999-NEW", 1)])
    page2 = client.get("/v1/vehicles", params={"limit": 5, "cursor": cursor}).json()

    ids1 = [v["vehicle_id"] for v in page1["vehicles"]]
    ids2 = [v["vehicle_id"] for v in page2["vehicles"]]
    assert not set(ids1) & set(ids2)
    assert "V-000-NEW" not in ids2  # before the cursor: not revisited
    rest = list_all(client, limit=5, cursor=cursor)
    assert "V-999-NEW" in [v["vehicle_id"] for v in rest]


def test_lexicographic_order_is_stable(isolated_client, payload):
    client, _ = isolated_client
    seed(client, payload, [payload(v, 1) for v in ("V-2", "V-10", "V-1")])
    body = client.get("/v1/vehicles").json()
    # vehicle_id is unique, so it is its own tie-breaker; order is total.
    assert [v["vehicle_id"] for v in body["vehicles"]] == ["V-1", "V-10", "V-2"]


def test_status_filtering(isolated_client, payload):
    client, engine = isolated_client
    seed(client, payload, [
        payload("V-CHARGING", 1, charging=True),
        payload("V-ACTIVE", 1, speed_kmh=30),
        payload("V-IDLE", 1, speed_kmh=0),
        payload("V-OFFLINE", 1),
    ])
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE vehicles SET last_seen_at = now() - interval '120 seconds' "
                 "WHERE vehicle_id = 'V-OFFLINE'")
        )
    for status, expected in [
        ("charging", ["V-CHARGING"]),
        ("active", ["V-ACTIVE"]),
        ("idle", ["V-IDLE"]),
        ("offline", ["V-OFFLINE"]),
    ]:
        body = client.get("/v1/vehicles", params={"status": status}).json()
        assert [v["vehicle_id"] for v in body["vehicles"]] == expected
        assert body["vehicles"][0]["status"] == status


def test_invalid_status(isolated_client):
    client, _ = isolated_client
    assert client.get("/v1/vehicles", params={"status": "bogus"}).status_code == 422


def test_bounding_box_filter(isolated_client, payload):
    client, _ = isolated_client
    seed(client, payload, [
        payload("V-CHENNAI-1", 1, lat=13.0, lng=80.2),
        payload("V-CHENNAI-2", 1, lat=12.9, lng=80.4),
        payload("V-MUMBAI", 1, lat=19.076, lng=72.877),
    ])
    body = client.get("/v1/vehicles", params={
        "min_lat": 12.8, "min_lng": 79.9, "max_lat": 13.3, "max_lng": 80.5,
    }).json()
    assert [v["vehicle_id"] for v in body["vehicles"]] == ["V-CHENNAI-1", "V-CHENNAI-2"]


def test_bounding_box_validation(isolated_client):
    client, _ = isolated_client
    full = {"min_lat": 12.8, "min_lng": 79.9, "max_lat": 13.3, "max_lng": 80.5}
    # Partial box.
    partial = dict(full)
    del partial["max_lng"]
    assert client.get("/v1/vehicles", params=partial).status_code == 422
    # Inverted ranges.
    assert client.get("/v1/vehicles", params={**full, "min_lat": 14}).status_code == 422
    assert client.get("/v1/vehicles", params={**full, "min_lng": 81}).status_code == 422
    # Out-of-world coordinates.
    assert client.get("/v1/vehicles", params={**full, "max_lat": 91}).status_code == 422
    assert client.get("/v1/vehicles", params={**full, "min_lng": -181}).status_code == 422


def test_invalid_cursor(isolated_client):
    client, _ = isolated_client
    assert client.get("/v1/vehicles", params={"cursor": "garbage!!"}).status_code == 422
    import base64
    import json
    wrong = base64.urlsafe_b64encode(json.dumps({"id": 1}).encode()).decode()
    assert client.get("/v1/vehicles", params={"cursor": wrong}).status_code == 422