"""
Tiny stand-in web app for local end-to-end testing.

Why this exists:
  Receives signed events from the orchestrator, prints them, dedupes by event_id,
  and exposes helpers to send signed commands back.

Who calls it:
  Run with: uvicorn tools.fake_web_app:app --port 8001
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import time
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from orchestrator.contract import EventAck, parse_event_payload
from orchestrator.security import SignatureError, sign_request_headers, verify_signed_request

ORCH_URL = os.getenv("ORCHESTRATOR_URL", "http://127.0.0.1:8000")
HMAC_SECRET = os.getenv("HMAC_SECRET", "change-me-use-a-long-random-string-in-production")
MAX_AGE = int(os.getenv("SIGNATURE_MAX_AGE_SECS", "300"))

app = FastAPI(title="Fake Web App", version="0.1.0")
_seen_events: set[str] = set()
_event_log: list[dict[str, Any]] = []


@app.post("/v1/events", response_model=EventAck)
async def receive_event(request: Request) -> EventAck:
    """Accept orchestrator events, verify signature, print readable log."""
    body = await request.body()
    try:
        verify_signed_request(HMAC_SECRET, request.headers, body, MAX_AGE)
    except SignatureError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    data = json.loads(body)
    event_id = data.get("event_id", "")
    if event_id in _seen_events:
        return EventAck(ok=True, event_id=event_id)
    _seen_events.add(event_id)

    from orchestrator.contract import OutboundEvent

    event = OutboundEvent.model_validate(data)
    parsed = parse_event_payload(event)
    line = f"[{event.type.value}] campaign={event.campaign_id} call={event.call_id} -> {parsed}"
    print(line)
    _event_log.append(data)
    return EventAck(ok=True, event_id=event_id)


@app.get("/v1/events/log")
async def event_log() -> dict[str, Any]:
    """Return captured events for debugging in tests."""
    return {"count": len(_event_log), "events": _event_log[-50:]}


async def _signed_post(path: str, payload: BaseModel | dict) -> httpx.Response:
    """POST JSON to orchestrator with HMAC headers."""
    if isinstance(payload, BaseModel):
        body = payload.model_dump_json().encode("utf-8")
    else:
        body = json.dumps(payload).encode("utf-8")
    headers = sign_request_headers(HMAC_SECRET, body)
    headers["Content-Type"] = "application/json"
    async with httpx.AsyncClient() as client:
        return await client.post(f"{ORCH_URL.rstrip('/')}{path}", content=body, headers=headers, timeout=30)


async def _signed_put(path: str, payload: BaseModel | dict) -> httpx.Response:
    """PUT JSON to orchestrator with HMAC headers."""
    if isinstance(payload, BaseModel):
        body = payload.model_dump_json().encode("utf-8")
    else:
        body = json.dumps(payload).encode("utf-8")
    headers = sign_request_headers(HMAC_SECRET, body)
    headers["Content-Type"] = "application/json"
    async with httpx.AsyncClient() as client:
        return await client.put(f"{ORCH_URL.rstrip('/')}{path}", content=body, headers=headers, timeout=30)


@app.post("/demo/run-two-campaigns")
async def demo_run_two_campaigns() -> dict[str, Any]:
    """
    Configure two campaigns, enqueue leads, start both, send heartbeat.

    Uses 10-slot global cap assumptions from .env.example.
    """
    from orchestrator.contract import (
        CallingHours,
        CallingWindow,
        CampaignScript,
        ConfigureCampaignRequest,
        HeartbeatRequest,
        LeadInput,
        PutLeadsRequest,
        PutLimitsRequest,
        PutNumbersRequest,
        PoolNumberInput,
    )

    await _signed_put("/v1/limits", PutLimitsRequest(global_concurrent_cap=10))
    await _signed_put(
        "/v1/numbers",
        PutNumbersRequest(
            numbers=[
                PoolNumberInput(number="+18005551001"),
                PoolNumberInput(number="+18005551002"),
            ]
        ),
    )
    await _signed_post("/v1/heartbeat", HeartbeatRequest())

    hours = CallingHours(
        timezone="UTC",
        windows=[CallingWindow(day_of_week=d, start=time(0, 0), end=time(23, 59, 59)) for d in range(7)],
    )
    script = CampaignScript(
        version="demo-1",
        body="Quick demo script.",
        ai_disclosure="This call may use AI.",
        business_identity="Demo Corp",
        callback_number="+18005551212",
    )
    config = ConfigureCampaignRequest(
        script=script,
        calling_hours=hours,
        campaign_concurrent_cap=5,
    )

    for cid in ("campaign_a", "campaign_b"):
        await _signed_post(f"/v1/campaigns/{cid}/configure", config)
        leads = PutLeadsRequest(
            leads=[
                LeadInput(
                    lead_id=f"{cid}-lead-{i}",
                    phone=f"+1555000{i:04d}",
                    business_name=f"Biz {cid} {i}",
                )
                for i in range(3)
            ]
        )
        await _signed_put(f"/v1/campaigns/{cid}/leads", leads)
        await _signed_post(f"/v1/campaigns/{cid}/start", {})

    return {"ok": True, "message": "campaigns started; watch orchestrator logs and /v1/events/log"}


def main() -> None:
    """CLI helper: asyncio entry for demo (optional)."""
    asyncio.run(demo_run_two_campaigns())


if __name__ == "__main__":
    main()