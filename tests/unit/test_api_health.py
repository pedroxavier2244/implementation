from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fastapi")


@pytest.fixture
def client():
    from fastapi.testclient import TestClient

    from api.main import app

    return TestClient(app)


def test_health_always_returns_200(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_returns_503_when_db_fails(client):
    with patch("api.main.get_engine") as mock_engine:
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(side_effect=Exception("db down"))
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine.return_value.connect.return_value = mock_conn

        response = client.get("/ready")
        assert response.status_code == 503
