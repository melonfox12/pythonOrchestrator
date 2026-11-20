"""
Simulated telephony: ring delay and probabilistic outcomes.

Why this exists:
  Lets us exercise full call lifecycle without a carrier account.

Who calls it:
  main.py registers FakeTelephony when TELEPHONY_BACKEND=fake.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from dataclasses import dataclass

from orchestrator.backends.base import (
    TelephonyBackend,
    TelephonyOutcome,
    TelephonyPlaceResult,
    TelephonyStatus,
)
from orchestrator.contract import AnsweredBy


@dataclass
class _FakeCallState:
    """In-memory leg state for one simulated call."""

    telephony_call_id: str
    call_id: str
    from_number: str
    to_number: str
    cancelled: bool = False
    outcome: TelephonyOutcome | None = None
    answered_by: AnsweredBy | None = None
    ring_ms: int = 0


class FakeTelephony(TelephonyBackend):
    """
    Random-delay ring simulator with configurable outcome probabilities.

    Probabilities come from Settings (FAKE_*_PROB env vars).
    """

    def __init__(
        self,
        no_answer_prob: float,
        voicemail_prob: float,
        busy_prob: float,
        answered_prob: float,
        ring_min_ms: int = 2000,
        ring_max_ms: int = 12000,
    ) -> None:
        """Store probs and ring timing; probs must sum to 1.0 (validated in config)."""
        self._probs = [
            (TelephonyOutcome.NO_ANSWER, no_answer_prob),
            (TelephonyOutcome.VOICEMAIL, voicemail_prob),
            (TelephonyOutcome.BUSY, busy_prob),
            (TelephonyOutcome.ANSWERED_HUMAN, answered_prob),
        ]
        self._ring_min_ms = ring_min_ms
        self._ring_max_ms = ring_max_ms
        self._calls: dict[str, _FakeCallState] = {}
        self._tasks: dict[str, asyncio.Task] = {}

    def _pick_outcome(self) -> TelephonyOutcome:
        """Weighted random choice of call outcome."""
        roll = random.random()
        cumulative = 0.0
        for outcome, prob in self._probs:
            cumulative += prob
            if roll <= cumulative:
                return outcome
        return TelephonyOutcome.FAILED

    async def place_call(
        self,
        call_id: str,
        from_number: str,
        to_number: str,
    ) -> TelephonyPlaceResult:
        """
        Register a new fake leg and start background ringing simulation.

        Returns immediately like a real async dial API.
        """
        telephony_call_id = f"fake-{uuid.uuid4().hex[:12]}"
        state = _FakeCallState(
            telephony_call_id=telephony_call_id,
            call_id=call_id,
            from_number=from_number,
            to_number=to_number,
        )
        self._calls[telephony_call_id] = state
        self._tasks[telephony_call_id] = asyncio.create_task(
            self._simulate_ring(telephony_call_id)
        )
        return TelephonyPlaceResult(telephony_call_id=telephony_call_id)

    async def _simulate_ring(self, telephony_call_id: str) -> None:
        """
        Wait random ring time, then assign outcome unless hung up.

        Internal task started by place_call.
        """
        state = self._calls[telephony_call_id]
        ring_ms = random.randint(self._ring_min_ms, self._ring_max_ms)
        state.ring_ms = ring_ms
        await asyncio.sleep(ring_ms / 1000.0)
        if state.cancelled:
            state.outcome = TelephonyOutcome.FAILED
            return
        outcome = self._pick_outcome()
        state.outcome = outcome
        if outcome == TelephonyOutcome.ANSWERED_HUMAN:
            state.answered_by = AnsweredBy.HUMAN
        elif outcome == TelephonyOutcome.VOICEMAIL:
            state.answered_by = AnsweredBy.VOICEMAIL
        elif outcome == TelephonyOutcome.NO_ANSWER:
            state.answered_by = AnsweredBy.UNKNOWN
        else:
            state.answered_by = AnsweredBy.UNKNOWN

    async def wait_for_outcome(self, telephony_call_id: str) -> TelephonyStatus:
        """
        Wait until the simulation task finishes and return status.

        calls.py polls this once per call after place_call.
        """
        task = self._tasks.get(telephony_call_id)
        state = self._calls.get(telephony_call_id)
        if state is None:
            return TelephonyStatus(
                telephony_call_id=telephony_call_id,
                outcome=TelephonyOutcome.FAILED,
            )
        if task is not None and not task.done():
            await task
        return TelephonyStatus(
            telephony_call_id=telephony_call_id,
            outcome=state.outcome,
            answered_by=state.answered_by,
            ring_ms=state.ring_ms,
        )

    async def hangup(self, telephony_call_id: str) -> None:
        """
        Cancel ringing or mark an in-flight leg ended.

        Used by kill switch and recovery.
        """
        state = self._calls.get(telephony_call_id)
        if state is None:
            return
        state.cancelled = True
        task = self._tasks.get(telephony_call_id)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if state.outcome is None:
            state.outcome = TelephonyOutcome.FAILED

    async def shutdown(self) -> None:
        """Cancel all simulation tasks on process exit."""
        for telephony_id in list(self._tasks.keys()):
            await self.hangup(telephony_id)