import base64
import json

import pytest

from app.queries import InvalidCursor, decode_cursor, encode_cursor


def test_cursor_roundtrip():
    payload = {"vehicle_id": "VG-00417"}
    assert decode_cursor(encode_cursor(payload), {"vehicle_id"}) == payload


def test_cursor_roundtrip_history():
    payload = {"ts": "2026-11-20T12:00:00+00:00", "seq": 42}
    assert decode_cursor(encode_cursor(payload), {"ts", "seq"}) == payload


def test_cursor_not_base64():
    with pytest.raises(InvalidCursor):
        decode_cursor("%%%not-base64%%%", {"vehicle_id"})


def test_cursor_not_json():
    raw = base64.urlsafe_b64encode(b"plain text").decode()
    with pytest.raises(InvalidCursor):
        decode_cursor(raw, {"vehicle_id"})


def test_cursor_wrong_keys():
    raw = base64.urlsafe_b64encode(json.dumps({"id": 1}).encode()).decode()
    with pytest.raises(InvalidCursor):
        decode_cursor(raw, {"vehicle_id"})


def test_cursor_not_an_object():
    raw = base64.urlsafe_b64encode(json.dumps([1, 2]).encode()).decode()
    with pytest.raises(InvalidCursor):
        decode_cursor(raw, {"vehicle_id"})