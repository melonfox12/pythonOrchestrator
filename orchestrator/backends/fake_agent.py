"""
Simulated conversation: timed transcript turns and canned facts.

Why this exists:
  Exercises agent streaming and fact extraction without STT/LLM/TTS.

Who calls it:
  main.py registers FakeAgent when AGENT_BACKEND=fake.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

from orchestrator.backends.base import AgentRunner, AgentTurn
from orchestrator.contract import CampaignScript, TurnSpeaker


class FakeAgent(AgentRunner):
    """
    Plays back a short scripted dialogue with realistic pauses.

    Duration is random between min/max seconds from Settings.
    """

    def __init__(self, min_duration_secs: int, max_duration_secs: int) -> None:
        """Remember conversation length bounds."""
        self._min = min_duration_secs
        self._max = max_duration_secs
        self._cancelled: set[str] = set()
        self._transcripts: dict[str, list[AgentTurn]] = {}

    async def run_conversation(
        self,
        call_id: str,
        script: CampaignScript,
        lead_context: dict[str, Any],
    ):
        """
        Yield agent and human lines with delays (async generator).

        calls.py async-iterates and emits call.turn for each item.
        """
        business = lead_context.get("business_name", "your business")
        duration = random.randint(self._min, self._max)
        script_lines = [
            (
                TurnSpeaker.AGENT,
                f"{script.ai_disclosure} {script.business_identity} calling about {business}.",
            ),
            (TurnSpeaker.HUMAN, "Oh, hi. What is this regarding?"),
            (
                TurnSpeaker.AGENT,
                f"We're following up on outreach. Callback number is {script.callback_number}.",
            ),
            (TurnSpeaker.HUMAN, "Okay, I have a minute."),
            (TurnSpeaker.AGENT, script.body[:120] if script.body else "Thanks for your time today."),
            (TurnSpeaker.HUMAN, "That sounds fine."),
        ]
        turns: list[AgentTurn] = []
        elapsed = 0
        step_ms = max(1, (duration * 1000) // max(len(script_lines), 1))

        for speaker, text in script_lines:
            if call_id in self._cancelled:
                break
            turn = AgentTurn(speaker=speaker, text=text, ms_offset=elapsed)
            turns.append(turn)
            yield turn
            elapsed += step_ms
            await asyncio.sleep(step_ms / 1000.0)

        self._transcripts[call_id] = turns

    async def extract_facts(self, call_id: str) -> dict[str, Any]:
        """
        Return deterministic fake CRM fields for the web app.

        calls.py sends these as call.facts payload.
        """
        turns = self._transcripts.get(call_id, [])
        return {
            "reached_decision_maker": any(t.speaker == TurnSpeaker.HUMAN for t in turns),
            "callback_number_shared": True,
            "interest_level": random.choice(["low", "medium", "high"]),
            "turn_count": len(turns),
        }

    async def cancel(self, call_id: str) -> None:
        """Mark conversation stopped (kill or early hangup)."""
        self._cancelled.add(call_id)