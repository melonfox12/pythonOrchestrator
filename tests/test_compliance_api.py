"""Compliance validation on configure."""

import json

import pytest
from fastapi.testclient import TestClient

from orchestrator.main import create_app
from orchestrator.security import sign_request_headers


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("WEB_APP_URL", "http://127.0.0.1:8001")
    monkeypatch.setenv("HMAC_SECRET", "dev-secret-min-8-chars")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("ORCHESTRATOR_DISABLE_LOOPS", "1")
    app = create_app()
    with TestClient(app) as c:
        yield c


def _post(client, path, payload: dict):
    body = json.dumps(payload).encode()
    headers = sign_request_headers("dev-secret-min-8-chars", body)
    headers["Content-Type"] = "application/json"
    return client.post(path, content=body, headers=headers)


def test_configure_rejects_whitespace_only_compliance(client):
    payload = {
        "script": {
            "version": "1",
            "body": "",
            "ai_disclosure": "   ",
            "business_identity": "Biz",
            "callback_number": "+1",
        },
        "calling_hours": {
            "timezone": "UTC",
            "windows": [{"day_of_week": 0, "start": "09:00:00", "end": "17:00:00"}],
        },
        "campaign_concurrent_cap": 1,
    }
    r = _post(client, "/v1/campaigns/c1/configure", payload)
    assert r.status_code == 400