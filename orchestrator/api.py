"""
HTTP command API the web app uses to control the orchestrator.

Why this exists:
  Exposes configure/leads/start/limits/numbers/heartbeat/kill/status over FastAPI.
  Every mutating route verifies HMAC signatures before changing state.

Who calls it:
  main.py mounts this router on the app. tools/fake_web_app.py sends the same requests.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from orchestrator.calls import CallService
from orchestrator.contract import (
    CampaignRunState,
    CampaignScript,
    CallingHours,
    ConfigureCampaignRequest,
    ConfigureCampaignResponse,
    CampaignControlResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    KillResponse,
    OrchestratorStatusResponse,
    PoolNumberStatus,
    PutLeadsRequest,
    PutLeadsResponse,
    PutLimitsRequest,
    PutLimitsResponse,
    PutNumbersRequest,
    PutNumbersResponse,
    CampaignStatusSnapshot,
    SystemHealth,
    validate_campaign_cap_against_global,
    validate_script_compliance,
)
from orchestrator.pool import NumberPoolManager
from orchestrator.security import SignatureError, verify_signed_request
from orchestrator.state import AppState
from orchestrator.store import EventStore

router = APIRouter(prefix="/v1")


class OrchestratorRuntime:
    """
    Bundle of shared objects attached to FastAPI app.state.

    main.py creates one instance during lifespan startup.
    """

    def __init__(
        self,
        settings,
        app_state: AppState,
        store: EventStore,
        pool: NumberPoolManager,
        call_service: CallService,
    ) -> None:
        self.settings = settings
        self.app_state = app_state
        self.store = store
        self.pool = pool
        self.call_service = call_service


def get_runtime(request: Request) -> OrchestratorRuntime:
    """FastAPI dependency returning shared runtime from app.state."""
    runtime: OrchestratorRuntime | None = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise HTTPException(status_code=503, detail="orchestrator not ready")
    return runtime


async def verified_body(request: Request, runtime: OrchestratorRuntime = Depends(get_runtime)) -> bytes:
    """
    Read raw body and verify HMAC before route handlers parse JSON.

    Used as a dependency on all signed command routes.
    """
    body = await request.body()
    try:
        verify_signed_request(
            runtime.settings.hmac_secret,
            request.headers,
            body,
            runtime.settings.signature_max_age_secs,
        )
    except SignatureError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    return body


@router.post("/campaigns/{campaign_id}/configure", response_model=ConfigureCampaignResponse)
async def configure_campaign(
    campaign_id: str,
    body: Annotated[bytes, Depends(verified_body)],
    runtime: OrchestratorRuntime = Depends(get_runtime),
) -> ConfigureCampaignResponse:
    """Store script, hours, and per-campaign cap (compliance enforced)."""
    try:
        payload = ConfigureCampaignRequest.model_validate_json(body)
        validate_script_compliance(payload.script)
        validate_campaign_cap_against_global(
            payload.campaign_concurrent_cap,
            runtime.app_state.global_concurrent_cap,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    runtime.store.upsert_campaign(
        campaign_id,
        payload.script,
        payload.calling_hours,
        payload.campaign_concurrent_cap,
    )
    runtime.app_state.configure_campaign(
        campaign_id,
        payload.script,
        payload.calling_hours,
        payload.campaign_concurrent_cap,
    )
    return ConfigureCampaignResponse(campaign_id=campaign_id, script_version=payload.script.version)


@router.put("/campaigns/{campaign_id}/leads", response_model=PutLeadsResponse)
async def put_leads(
    campaign_id: str,
    body: Annotated[bytes, Depends(verified_body)],
    runtime: OrchestratorRuntime = Depends(get_runtime),
) -> PutLeadsResponse:
    """Queue leads; duplicates skipped by lead_id."""
    payload = PutLeadsRequest.model_validate_json(body)
    record = runtime.app_state.get_or_create_campaign(campaign_id)
    before = set(record.leads_by_id.keys())
    accepted, skipped = runtime.app_state.enqueue_leads(campaign_id, payload.leads)
    new_ids = set(record.leads_by_id.keys()) - before
    if new_ids:
        runtime.store.insert_leads(
            campaign_id,
            [record.leads_by_id[lead_id] for lead_id in new_ids],
        )
    return PutLeadsResponse(campaign_id=campaign_id, accepted=accepted, skipped_duplicate=skipped)


@router.post("/campaigns/{campaign_id}/start", response_model=CampaignControlResponse)
async def start_campaign(
    campaign_id: str,
    _: Annotated[bytes, Depends(verified_body)],
    runtime: OrchestratorRuntime = Depends(get_runtime),
) -> CampaignControlResponse:
    """Mark campaign running if configured."""
    record = runtime.app_state.get_or_create_campaign(campaign_id)
    if record.script is None or record.calling_hours is None:
        raise HTTPException(status_code=400, detail="campaign not configured")
    runtime.app_state.set_campaign_state(campaign_id, CampaignRunState.RUNNING)
    runtime.store.set_campaign_state(campaign_id, CampaignRunState.RUNNING)
    return CampaignControlResponse(campaign_id=campaign_id, state=CampaignRunState.RUNNING)


@router.post("/campaigns/{campaign_id}/pause", response_model=CampaignControlResponse)
async def pause_campaign(
    campaign_id: str,
    _: Annotated[bytes, Depends(verified_body)],
    runtime: OrchestratorRuntime = Depends(get_runtime),
) -> CampaignControlResponse:
    """Pause new dials for this campaign."""
    runtime.app_state.set_campaign_state(campaign_id, CampaignRunState.PAUSED)
    runtime.store.set_campaign_state(campaign_id, CampaignRunState.PAUSED)
    return CampaignControlResponse(campaign_id=campaign_id, state=CampaignRunState.PAUSED)


@router.post("/campaigns/{campaign_id}/stop", response_model=CampaignControlResponse)
async def stop_campaign(
    campaign_id: str,
    _: Annotated[bytes, Depends(verified_body)],
    runtime: OrchestratorRuntime = Depends(get_runtime),
) -> CampaignControlResponse:
    """Stop campaign dialing (in-flight calls continue)."""
    runtime.app_state.set_campaign_state(campaign_id, CampaignRunState.STOPPED)
    runtime.store.set_campaign_state(campaign_id, CampaignRunState.STOPPED)
    return CampaignControlResponse(campaign_id=campaign_id, state=CampaignRunState.STOPPED)


@router.put("/limits", response_model=PutLimitsResponse)
async def put_limits(
    body: Annotated[bytes, Depends(verified_body)],
    runtime: OrchestratorRuntime = Depends(get_runtime),
) -> PutLimitsResponse:
    """Update global concurrent slot cap."""
    payload = PutLimitsRequest.model_validate_json(body)
    for record in runtime.app_state.campaigns.values():
        if record.campaign_concurrent_cap > payload.global_concurrent_cap:
            raise HTTPException(
                status_code=400,
                detail=f"campaign {record.campaign_id} cap exceeds new global cap",
            )
    runtime.app_state.set_global_cap(payload.global_concurrent_cap)
    runtime.store.save_global_cap(payload.global_concurrent_cap)
    return PutLimitsResponse(global_concurrent_cap=payload.global_concurrent_cap)


@router.put("/numbers", response_model=PutNumbersResponse)
async def put_numbers(
    body: Annotated[bytes, Depends(verified_body)],
    runtime: OrchestratorRuntime = Depends(get_runtime),
) -> PutNumbersResponse:
    """Sync caller ID pool."""
    payload = PutNumbersRequest.model_validate_json(body)
    active, retired = runtime.pool.sync_numbers(
        payload.numbers,
        runtime.settings.number_daily_cap_default,
    )
    return PutNumbersResponse(active=active, retired=retired)


@router.post("/heartbeat", response_model=HeartbeatResponse)
async def heartbeat(
    body: Annotated[bytes, Depends(verified_body)],
    runtime: OrchestratorRuntime = Depends(get_runtime),
) -> HeartbeatResponse:
    """Record web app liveness."""
    payload = (
        HeartbeatRequest.model_validate_json(body) if body.strip() else HeartbeatRequest()
    )
    runtime.app_state.record_heartbeat(payload.sent_at)
    return HeartbeatResponse(health=runtime.app_state.health)


@router.post("/kill", response_model=KillResponse)
async def kill(
    _: Annotated[bytes, Depends(verified_body)],
    runtime: OrchestratorRuntime = Depends(get_runtime),
) -> KillResponse:
    """Emergency hangup for all calls."""
    count = runtime.app_state.engage_kill_switch()
    terminated = await runtime.call_service.terminate_all()
    return KillResponse(calls_terminated=max(count, terminated))


@router.get("/status", response_model=OrchestratorStatusResponse)
async def get_status(runtime: OrchestratorRuntime = Depends(get_runtime)) -> OrchestratorStatusResponse:
    """
    Operator snapshot (unsigned GET for simplicity in dev; protect in production).

    Returns slots, campaigns, numbers, and health.
    """
    snap = runtime.app_state.status_snapshot_dict()
    numbers_raw = runtime.pool.status_list()
    numbers = []
    for row in numbers_raw:
        row = dict(row)
        row.pop("selectable_now", None)
        numbers.append(PoolNumberStatus.model_validate(row))
    campaigns = [CampaignStatusSnapshot.model_validate(c) for c in snap["campaigns"]]
    return OrchestratorStatusResponse(
        health=SystemHealth(snap["health"]),
        global_concurrent_cap=snap["global_concurrent_cap"],
        slots_in_use=snap["slots_in_use"],
        slots_available=snap["slots_available"],
        last_heartbeat_at=snap["last_heartbeat_at"],
        degraded_reason=snap.get("degraded_reason"),
        campaigns=campaigns,
        numbers=numbers,
    )