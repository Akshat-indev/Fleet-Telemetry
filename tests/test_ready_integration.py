"""Integration test: requires a real PostgreSQL reachable via DATABASE_URL.

Skipped when DATABASE_URL is not set. Run with the Compose database up, e.g.:
  DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet pytest
"""


def test_ready_succeeds_against_real_database(client, real_database):
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}