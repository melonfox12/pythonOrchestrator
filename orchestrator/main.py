"""
FastAPI application entrypoint: lifespan, recovery, background loops.

Why this exists:
  Wires config, SQLite, state, pool, backends, and the three asyncio loops
  into one runnable uvicorn target.

Who calls it:
  `uvicorn orchestrator.main:app` (local dev and Docker CMD).
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI

from orchestrator.api import OrchestratorRuntime, router
from orchestrator.backends.fake_agent import FakeAgent
from orchestrator.backends.fake_telephony import FakeTelephony
from orchestrator.calls import CallService
from orchestrator.config import Settings, load_settings
from orchestrator.contract import (
    CallEndReason,
    CallInterruptedPayload,
    CampaignRunState,
    CampaignScript,
    CallingHours,
    EventType,
    TranscriptTurn,
    TurnSpeaker,
    build_event,
)
from orchestrator.dialer import dialer_loop
from orchestrator.outbox import OutboxPublisher, outbox_loop
from orchestrator.pool import NumberPoolManager
from orchestrator.security import redact_secret_for_log
from orchestrator.state import AppState
from orchestrator.store import EventStore
from orchestrator.watchdog import watchdog_loop

logger = logging.getLogger(__name__)


def _configure_logging(level: str) -> None:
    """Apply LOG_LEVEL once at import/startup."""
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def hydrate_from_store(
    store: EventStore,
    app_state: AppState,
    pool: NumberPoolManager,
    settings: Settings,
) -> None:
    """
    Load campaigns, leads, numbers, and global cap from SQLite into memory.

    Called once during lifespan before accepting traffic.
    """
    cap = store.load_global_cap(settings.global_concurrent_cap)
    app_state.set_global_cap(cap)

    for row in store.load_campaigns():
        campaign_id = row["campaign_id"]
        record = app_state.get_or_create_campaign(campaign_id)
        record.campaign_concurrent_cap = row["campaign_concurrent_cap"]
        if row["script"]:
            record.script = CampaignScript.model_validate(row["script"])
        if row["calling_hours"]:
            record.calling_hours = CallingHours.model_validate(row["calling_hours"])
        if campaign_id not in app_state.dialer_round_robin:
            app_state.dialer_round_robin.append(campaign_id)

        app_state.set_campaign_state(campaign_id, CampaignRunState.PAUSED)
        store.set_campaign_state(campaign_id, CampaignRunState.PAUSED)

        leads = store.load_queued_leads(campaign_id)
        if leads:
            app_state.enqueue_leads(campaign_id, leads)

    pool.hydrate_from_store(store.load_pool_numbers())


async def recover_in_progress_calls(
    store: EventStore,
    app_state: AppState,
    outbox: OutboxPublisher,
    telephony: FakeTelephony,
) -> None:
    """
    Mark crash-era calls interrupted, hang up fake legs, emit events, free slots.

    Runs during lifespan startup before loops begin.
    """
    rows = store.fetch_in_progress_calls()
    for row in rows:
        call_id = row["call_id"]
        transcript_raw = store.fetch_transcript_for_call(call_id)
        partial = [
            TranscriptTurn(
                speaker=TurnSpeaker(t["speaker"]),
                text=t["text"],
                ms_offset=t["ms_offset"],
            )
            for t in transcript_raw
        ]
        payload = CallInterruptedPayload(
            end_reason=CallEndReason.INTERRUPTED,
            partial_transcript=partial,
            facts={},
            duration_secs=0,
        )
        outbox.publish(
            build_event(
                EventType.CALL_INTERRUPTED,
                payload,
                campaign_id=row["campaign_id"],
                call_id=call_id,
                lead_id=row["lead_id"],
            )
        )
        store.update_call_status(call_id, "interrupted", CallEndReason.INTERRUPTED.value)
        app_state.release_slot(call_id, row["campaign_id"])
        app_state.finish_call(call_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup: validate config, open DB, hydrate, recover, start loops.
    Shutdown: stop loops, wait for calls, flush outbox, close DB.
    """
    settings = load_settings()
    _configure_logging(settings.log_level)
    logger.info(
        "starting orchestrator on %s:%s (hmac=%s)",
        settings.orchestrator_host,
        settings.orchestrator_port,
        redact_secret_for_log(settings.hmac_secret),
    )

    store = EventStore(settings.sqlite_path_resolved)
    store.start()

    app_state = AppState(settings.global_concurrent_cap)
    pool = NumberPoolManager(
        default_daily_cap=settings.number_daily_cap_default,
        cooldown_secs=settings.number_cooldown_secs,
        dial_spacing_min_secs=settings.dial_spacing_min_secs,
        dial_spacing_max_secs=settings.dial_spacing_max_secs,
        pool_timezone=settings.pool_timezone,
        store=store,
    )
    hydrate_from_store(store, app_state, pool, settings)

    telephony = FakeTelephony(
        settings.fake_no_answer_prob,
        settings.fake_voicemail_prob,
        settings.fake_busy_prob,
        settings.fake_answered_prob,
    )
    agent = FakeAgent(settings.fake_agent_min_duration_secs, settings.fake_agent_max_duration_secs)

    outbox_queue: asyncio.Queue = asyncio.Queue()
    outbox = OutboxPublisher(store, outbox_queue)
    http_client = httpx.AsyncClient()

    call_service = CallService(
        app_state=app_state,
        store=store,
        pool=pool,
        telephony=telephony,
        agent=agent,
        outbox=outbox,
        max_call_secs=settings.max_call_secs,
    )

    disable_loops = os.getenv("ORCHESTRATOR_DISABLE_LOOPS", "").lower() in {
        "1",
        "true",
        "yes",
    }

    if not disable_loops:
        await recover_in_progress_calls(store, app_state, outbox, telephony)
        await outbox.replay_from_store(store.fetch_unacknowledged_events())

    stop_event = asyncio.Event()
    tasks: list[asyncio.Task] = []
    if not disable_loops:
        tasks = [
            asyncio.create_task(dialer_loop(app_state, pool, call_service, stop_event)),
            asyncio.create_task(outbox_loop(outbox, store, http_client, settings, stop_event)),
            asyncio.create_task(
                watchdog_loop(app_state, pool, call_service, outbox, settings, stop_event)
            ),
        ]

    runtime = OrchestratorRuntime(settings, app_state, store, pool, call_service)
    app.state.runtime = runtime

    yield

    stop_event.set()
    if not disable_loops:
        app_state.engage_kill_switch()
        await call_service.terminate_all()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(1.0)
    await http_client.aclose()
    await telephony.shutdown()
    store.stop()


def create_app() -> FastAPI:
    """Build FastAPI app with router and lifespan (used by tests and uvicorn)."""
    application = FastAPI(title="AI Call Orchestrator", version="0.1.0", lifespan=lifespan)
    application.include_router(router)
    return application


app = create_app()