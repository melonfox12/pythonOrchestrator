"""
Per-call lifecycle: place, answer, agent, events, cleanup.

Why this exists:
  Each outbound attempt runs as one async task with a single idempotent
  cleanup path so slots and numbers are always released.

Who calls it:
  dialer_loop starts tasks via CallService.start_outbound_call().
  watchdog_loop and POST /v1/kill call terminate_all() or terminate_one().
"""

from __future__ import annotations

import asyncio
import logging
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from orchestrator.backends.base import AgentRunner, TelephonyBackend, TelephonyOutcome
from orchestrator.contract import (
    AnsweredBy,
    CallAnsweredPayload,
    CallEndedPayload,
    CallEndReason,
    CallFactsPayload,
    CallPlacedPayload,
    CallTurnPayload,
    EventType,
    build_event,
)
from orchestrator.outbox import OutboxPublisher
from orchestrator.pool import NumberPoolManager
from orchestrator.state import ActiveCallRecord, AppState
from orchestrator.store import EventStore

logger = logging.getLogger(__name__)

OUTCOME_TO_END_REASON = {
    TelephonyOutcome.NO_ANSWER: CallEndReason.NO_ANSWER,
    TelephonyOutcome.VOICEMAIL: CallEndReason.VOICEMAIL,
    TelephonyOutcome.BUSY: CallEndReason.BUSY,
    TelephonyOutcome.FAILED: CallEndReason.FAILED,
}


@dataclass
class CallService:
    """
    Owns in-flight asyncio tasks and coordinates backends for one call.

    Constructed once in main.py and shared with dialer and watchdog.
    """

    app_state: AppState
    store: EventStore
    pool: NumberPoolManager
    telephony: TelephonyBackend
    agent: AgentRunner
    outbox: OutboxPublisher
    max_call_secs: int
    _tasks: dict[str, asyncio.Task] = field(default_factory=dict)
    _telephony_ids: dict[str, str] = field(default_factory=dict)
    _cleanup_locks: dict[str, asyncio.Lock] = field(default_factory=dict)

    def _call_lock(self, call_id: str) -> asyncio.Lock:
        """Return per-call lock so cleanup runs exactly once logically."""
        if call_id not in self._cleanup_locks:
            self._cleanup_locks[call_id] = asyncio.Lock()
        return self._cleanup_locks[call_id]

    def start_outbound_call(
        self,
        campaign_id: str,
        lead_id: str,
        from_number: str,
        call_id: str | None = None,
    ) -> str:
        """
        Spawn the async lifecycle for one lead (non-blocking).

        dialer_loop calls after slot + number reservation succeed.
        """
        call_id = call_id or f"call-{uuid.uuid4().hex[:12]}"
        task = asyncio.create_task(
            self._run_call(call_id, campaign_id, lead_id, from_number),
            name=f"call-{call_id}",
        )
        self._tasks[call_id] = task
        return call_id

    async def terminate_all(self) -> int:
        """
        Hang up every live call (kill switch).

        Returns how many tasks were signalled.
        """
        call_ids = list(self._tasks.keys())
        for call_id in call_ids:
            await self.terminate_one(call_id)
        return len(call_ids)

    async def terminate_one(self, call_id: str) -> None:
        """Force hangup + agent cancel for a single call (watchdog timeout)."""
        telephony_id = self._telephony_ids.get(call_id)
        if telephony_id is not None:
            await self.telephony.hangup(telephony_id)
        await self.agent.cancel(call_id)

    async def _run_call(
        self,
        call_id: str,
        campaign_id: str,
        lead_id: str,
        from_number: str,
    ) -> None:
        """
        Main call flow from place through agent to call.ended.

        All paths funnel through _cleanup in the finally block.
        """
        slot_held = True
        started_at = datetime.now(timezone.utc)
        end_reason = CallEndReason.FAILED
        duration_secs = 0
        telephony_id: str | None = None
        lead = self.app_state.campaigns[campaign_id].leads_by_id[lead_id]
        campaign = self.app_state.campaigns[campaign_id]
        script = campaign.script
        interrupted = False

        try:
            if script is None:
                end_reason = CallEndReason.FAILED
                return

            self.store.insert_call(
                call_id,
                campaign_id,
                lead_id,
                from_number,
                lead.phone,
                "placing",
            )
            self.app_state.register_active_call(
                ActiveCallRecord(
                    call_id=call_id,
                    campaign_id=campaign_id,
                    lead_id=lead_id,
                    from_number=from_number,
                    to_number=lead.phone,
                    started_at=started_at,
                )
            )

            place = await self.telephony.place_call(call_id, from_number, lead.phone)
            telephony_id = place.telephony_call_id
            self._telephony_ids[call_id] = telephony_id

            self.outbox.publish(
                build_event(
                    EventType.CALL_PLACED,
                    CallPlacedPayload(
                        from_number=from_number,
                        to_number=lead.phone,
                        business_name=lead.business_name,
                    ),
                    campaign_id=campaign_id,
                    call_id=call_id,
                    lead_id=lead_id,
                )
            )

            if self.app_state.kill_switch:
                end_reason = CallEndReason.INTERRUPTED
                interrupted = True
                return

            status = await self.telephony.wait_for_outcome(telephony_id)
            answered_by = status.answered_by or AnsweredBy.UNKNOWN

            self.outbox.publish(
                build_event(
                    EventType.CALL_ANSWERED,
                    CallAnsweredPayload(answered_by=answered_by),
                    campaign_id=campaign_id,
                    call_id=call_id,
                    lead_id=lead_id,
                )
            )

            if status.outcome != TelephonyOutcome.ANSWERED_HUMAN:
                end_reason = OUTCOME_TO_END_REASON.get(status.outcome, CallEndReason.FAILED)
                return

            if self.app_state.kill_switch:
                end_reason = CallEndReason.INTERRUPTED
                interrupted = True
                return

            self.store.update_call_status(call_id, "active")

            lead_context: dict[str, Any] = {
                "business_name": lead.business_name,
                "attributes": lead.attributes,
            }
            async for turn in self.agent.run_conversation(call_id, script, lead_context):
                if self.app_state.kill_switch:
                    end_reason = CallEndReason.INTERRUPTED
                    interrupted = True
                    break
                self.store.append_transcript_turn(
                    call_id, turn.speaker.value, turn.text, turn.ms_offset
                )
                self.outbox.publish(
                    build_event(
                        EventType.CALL_TURN,
                        CallTurnPayload(
                            speaker=turn.speaker,
                            text=turn.text,
                            ms_offset=turn.ms_offset,
                        ),
                        campaign_id=campaign_id,
                        call_id=call_id,
                        lead_id=lead_id,
                    )
                )

            if not interrupted:
                facts = await self.agent.extract_facts(call_id)
                self.outbox.publish(
                    build_event(
                        EventType.CALL_FACTS,
                        CallFactsPayload(facts=facts),
                        campaign_id=campaign_id,
                        call_id=call_id,
                        lead_id=lead_id,
                    )
                )
                end_reason = CallEndReason.COMPLETED
        except asyncio.CancelledError:
            end_reason = CallEndReason.INTERRUPTED
            interrupted = True
            raise
        except Exception:
            logger.exception("call %s failed", call_id)
            end_reason = CallEndReason.FAILED
        finally:
            ended_at = datetime.now(timezone.utc)
            duration_secs = max(0, int((ended_at - started_at).total_seconds()))
            billed = math.ceil(duration_secs / 60) if duration_secs > 0 else 0
            if end_reason == CallEndReason.COMPLETED and billed == 0:
                billed = 1

            self.outbox.publish(
                build_event(
                    EventType.CALL_ENDED,
                    CallEndedPayload(
                        end_reason=end_reason,
                        duration_secs=duration_secs,
                        billed_minutes=billed,
                    ),
                    campaign_id=campaign_id,
                    call_id=call_id,
                    lead_id=lead_id,
                )
            )
            self.store.update_call_status(call_id, "ended", end_reason.value)
            self.store.mark_lead_done(campaign_id, lead_id)
            await self._cleanup(call_id, campaign_id, from_number, slot_held)
            if telephony_id:
                await self.telephony.hangup(telephony_id)
            self._tasks.pop(call_id, None)
            self._telephony_ids.pop(call_id, None)
            self._cleanup_locks.pop(call_id, None)

    async def _cleanup(
        self,
        call_id: str,
        campaign_id: str,
        from_number: str,
        slot_held: bool,
    ) -> None:
        """
        Idempotent release of slot, number, and active-call registry.

        Must run on every exit path including errors and kill.
        """
        async with self._call_lock(call_id):
            if slot_held:
                self.app_state.release_slot(call_id, campaign_id)
            self.pool.release_number(from_number)
            self.app_state.finish_call(call_id)