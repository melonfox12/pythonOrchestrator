"""
Watchdog loop: heartbeat, stuck calls, snapshots.

Why this exists:
  Catches silent web app failure, overlong calls, and emits periodic health
  snapshots for operators.

Who calls it:
  main.py starts watchdog_loop() at startup alongside dialer and outbox.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from orchestrator.calls import CallService
from orchestrator.contract import StatusSnapshotPayload, EventType, build_event
from orchestrator.config import Settings
from orchestrator.outbox import OutboxPublisher
from orchestrator.pool import NumberPoolManager
from orchestrator.state import AppState, SystemHealth

logger = logging.getLogger(__name__)


async def watchdog_loop(
    app_state: AppState,
    pool: NumberPoolManager,
    call_service: CallService,
    outbox: OutboxPublisher,
    settings: Settings,
    stop_event: asyncio.Event,
    interval_secs: float = 15.0,
) -> None:
    """
    Every ~15s: heartbeat timeout, max call duration, status.snapshot.

    Stops when stop_event is set.
    """
    while not stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)
            if app_state.last_heartbeat_at is not None:
                age = (now - app_state.last_heartbeat_at).total_seconds()
                if age > settings.heartbeat_timeout_secs:
                    app_state.mark_degraded("heartbeat_timeout")
            elif app_state.health == SystemHealth.OK:
                # No heartbeat received yet since startup
                pass

            for call_id, record in list(app_state.active_calls.items()):
                elapsed = (now - record.started_at).total_seconds()
                if elapsed > settings.max_call_secs:
                    logger.warning("watchdog ending call %s after %ss", call_id, elapsed)
                    await call_service.terminate_one(call_id)

            for call_id, record in list(app_state.active_calls.items()):
                if record.slot_released and call_id not in call_service._tasks:
                    app_state.finish_call(call_id)

            status = app_state.status_snapshot_dict()
            numbers = pool.status_list()
            payload = StatusSnapshotPayload(
                health=SystemHealth(status["health"]),
                global_concurrent_cap=status["global_concurrent_cap"],
                slots_in_use=status["slots_in_use"],
                campaigns=[],  # filled below
                numbers=[],
                degraded_reason=status.get("degraded_reason"),
            )
            from orchestrator.contract import CampaignStatusSnapshot, PoolNumberStatus

            for row in status["campaigns"]:
                payload.campaigns.append(CampaignStatusSnapshot.model_validate(row))
            for row in numbers:
                row.pop("selectable_now", None)
                payload.numbers.append(PoolNumberStatus.model_validate(row))

            outbox.publish(
                build_event(
                    EventType.STATUS_SNAPSHOT,
                    payload,
                    campaign_id=None,
                )
            )
        except Exception:
            logger.exception("watchdog tick failed")

        await asyncio.sleep(interval_secs)