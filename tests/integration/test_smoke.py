"""
Integration smoke test, requires docker compose services running.
Run with: pytest tests/integration/ -v -m integration
"""

import os

import httpx
import pytest

SMOKE_BASE_URL = os.getenv("SMOKE_API_URL", "http://localhost:8000")


@pytest.mark.integration
def test_health_endpoint():
    response = httpx.get(f"{SMOKE_BASE_URL}/health", timeout=5)
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.integration
def test_ready_endpoint():
    response = httpx.get(f"{SMOKE_BASE_URL}/ready", timeout=10)
    assert response.status_code == 200
    assert response.json()["ready"] is True


@pytest.mark.integration
def test_list_files_empty():
    response = httpx.get(f"{SMOKE_BASE_URL}/v1/files", timeout=5)
    assert response.status_code == 200
    assert response.json()["total"] == 0


@pytest.mark.integration
def test_list_jobs_empty():
    response = httpx.get(f"{SMOKE_BASE_URL}/v1/jobs", timeout=5)
    assert response.status_code == 200
    assert isinstance(response.json(), list)
