"""
Abstract interfaces for telephony and conversation agents.

Why this exists:
  Phase 1 uses fake implementations; later Telnyx/Pipecat/LLM adapters implement
  the same methods so calls.py and dialer do not change.

Who calls it:
  main.py picks concrete classes from config and injects them into calls.py.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum
from typing import Any

from orchestrator.contract import AnsweredBy, CampaignScript, TurnSpeaker


class TelephonyOutcome(str, Enum):
    """High-level result of placing one outbound leg."""

    ANSWERED_HUMAN = "answered_human"
    VOICEMAIL = "voicemail"
    NO_ANSWER = "no_answer"
    BUSY = "busy"
    FAILED = "failed"


@dataclass
class TelephonyPlaceResult:
    """
    Immediate result of place_call (external call id + started).

    calls.py stores telephony_call_id for hangup during kill.
    """

    telephony_call_id: str
    accepted: bool = True


@dataclass
class TelephonyStatus:
    """
    Snapshot of a live call leg for watchdog and cleanup.

    FakeTelephony updates this in memory; real backends query the carrier.
    """

    telephony_call_id: str
    outcome: TelephonyOutcome | None = None
    answered_by: AnsweredBy | None = None
    ring_ms: int = 0


@dataclass
class AgentTurn:
    """
    One line of conversation streamed to the web app as call.turn.

    ms_offset is measured from agent start (after human answer).
    """

    speaker: TurnSpeaker
    text: str
    ms_offset: int


class TelephonyBackend(ABC):
    """
    Places, monitors, and tears down phone calls.

    FakeTelephony and a future TelnyxBackend both inherit this.
    """

    @abstractmethod
    async def place_call(
        self,
        call_id: str,
        from_number: str,
        to_number: str,
    ) -> TelephonyPlaceResult:
        """
        Start an outbound dial.

        calls.py invokes this after acquiring a slot and number.
        """

    @abstractmethod
    async def wait_for_outcome(
        self,
        telephony_call_id: str,
    ) -> TelephonyStatus:
        """
        Block until ring completes or timeout (async simulation or webhook wait).

        calls.py uses this before starting an agent on human answer.
        """

    @abstractmethod
    async def hangup(self, telephony_call_id: str) -> None:
        """
        End the carrier leg.

        kill switch and max-duration watchdog call this.
        """

    @abstractmethod
    async def shutdown(self) -> None:
        """Release connections on process exit (main.py lifespan)."""


class AgentRunner(ABC):
    """
    Runs one AI conversation for an answered call.

    FakeAgent and future Pipecat/LLM runners implement this.
    """

    @abstractmethod
    async def run_conversation(
        self,
        call_id: str,
        script: CampaignScript,
        lead_context: dict[str, Any],
    ) -> AsyncIterator[AgentTurn]:
        """
        Yield transcript turns in real time.

        calls.py forwards each turn as a call.turn event.
        """

    @abstractmethod
    async def extract_facts(self, call_id: str) -> dict[str, Any]:
        """
        Return structured facts after the conversation ends.

        calls.py emits call.facts before call.ended on success paths.
        """

    @abstractmethod
    async def cancel(self, call_id: str) -> None:
        """
        Stop a running agent (kill or hangup).

        calls.py cleanup invokes this when the callee drops early.
        """