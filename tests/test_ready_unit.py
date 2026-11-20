from sqlalchemy.exc import OperationalError

import app.main


def test_ready_returns_503_when_database_fails(client, monkeypatch):
    def failing_check():
        raise OperationalError("SELECT 1", None, Exception("connection refused"))

    monkeypatch.setattr(app.main, "check_database_ready", failing_check)

    response = client.get("/ready")
    assert response.status_code == 503
    assert response.json() == {"status": "database unavailable"}