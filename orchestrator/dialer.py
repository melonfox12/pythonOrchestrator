"""
Dialer loop: fairly dequeue leads and start outbound calls.

Why this exists:
  Separates steady-state placement from HTTP handlers. Runs about once per
  second and checks caps, hours, health, pool spacing, and slots.

Who calls it:
  main.py starts dialer_loop() as an asyncio task at startup.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from orchestrator.calls import CallService
from orchestrator.contract import CallingHours, SystemHealth
from orchestrator.pool import NumberPoolManager
from orchestrator.state import AppState

logger = logging.getLogger(__name__)


def is_within_calling_hours(calling_hours: CallingHours, now: datetime | None = None) -> bool:
    """
    Return True if local time falls inside any configured window.

    dialer_loop skips campaigns outside hours without dequeuing leads.
    """
    tz = ZoneInfo(calling_hours.timezone)
    local = datetime.now(tz) if now is None else now.astimezone(tz)
    day = local.weekday()
    current_time = local.time()
    for window in calling_hours.windows:
        if window.day_of_week == day and window.start <= current_time < window.end:
            return True
    return False


async def dialer_loop(
    app_state: AppState,
    pool: NumberPoolManager,
    call_service: CallService,
    stop_event: asyncio.Event,
    interval_secs: float = 1.0,
) -> None:
    """
    Round-robin across running campaigns and start one call when allowed.

    Exits when stop_event is set (main.py shutdown).
    """
    while not stop_event.is_set():
        try:
            if app_state.kill_switch:
                await asyncio.sleep(interval_secs)
                continue

            if app_state.health == SystemHealth.DEGRADED:
                await asyncio.sleep(interval_secs)
                continue

            if not app_state.global_slots_available():
                await asyncio.sleep(interval_secs)
                continue

            campaign_id = app_state.next_campaign_for_dialer()
            if not campaign_id:
                await asyncio.sleep(interval_secs)
                continue

            record = app_state.campaigns[campaign_id]
            if record.script is None or record.calling_hours is None:
                await asyncio.sleep(interval_secs)
                continue

            if not is_within_calling_hours(record.calling_hours):
                await asyncio.sleep(interval_secs)
                continue

            if not app_state.campaign_slots_available(campaign_id):
                await asyncio.sleep(interval_secs)
                continue

            if not pool.can_start_dial():
                await asyncio.sleep(interval_secs)
                continue

            lead_id = app_state.pop_next_lead_id(campaign_id)
            if not lead_id:
                await asyncio.sleep(interval_secs)
                continue

            from_number = pool.select_and_reserve()
            if not from_number:
                app_state.requeue_lead_front(campaign_id, lead_id)
                await asyncio.sleep(interval_secs)
                continue

            call_id = f"call-{uuid.uuid4().hex[:12]}"
            if not app_state.try_acquire_slot(call_id, campaign_id):
                pool.cancel_reservation(from_number)
                app_state.requeue_lead_front(campaign_id, lead_id)
                await asyncio.sleep(interval_secs)
                continue

            pool.record_dial_started()
            call_service.start_outbound_call(
                campaign_id, lead_id, from_number, call_id=call_id
            )
        except Exception:
            logger.exception("dialer tick failed")

        await asyncio.sleep(interval_secs)