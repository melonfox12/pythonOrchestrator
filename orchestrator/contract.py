"""
Pydantic models for every HTTP command and outbound event.

Why this exists:
  The web app and orchestrator must agree on JSON shapes. This file is the
  single source of truth; api.py, outbox.py, store.py, and tools/fake_web_app.py
  all import from here.

Who calls it:
  api.py validates incoming command bodies.
  outbox.py serializes OutboundEvent instances.
  tests import the same models to build fixtures.
"""

from __future__ import annotations

from datetime import datetime, time, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Shared enums
# ---------------------------------------------------------------------------


class CampaignRunState(str, Enum):
    """Whether the dialer is allowed to dequeue leads for this campaign."""

    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"


class NumberStatus(str, Enum):
    """Whether a caller ID may be selected for new dials."""

    ACTIVE = "active"
    RETIRED = "retired"


class AnsweredBy(str, Enum):
    """Who or what picked up the outbound call."""

    HUMAN = "human"
    VOICEMAIL = "voicemail"
    UNKNOWN = "unknown"


class CallEndReason(str, Enum):
    """Why a call leg ended; the web app maps these to business outcomes."""

    COMPLETED = "completed"
    NO_ANSWER = "no_answer"
    VOICEMAIL = "voicemail"
    BUSY = "busy"
    HUNG_UP_EARLY = "hung_up_early"
    FAILED = "failed"
    DO_NOT_CALL_REQUESTED = "do_not_call_requested"
    INTERRUPTED = "interrupted"


class TurnSpeaker(str, Enum):
    """Who spoke one line of transcript."""

    AGENT = "agent"
    HUMAN = "human"
    SYSTEM = "system"


class SystemHealth(str, Enum):
    """High-level orchestrator health reported in status.snapshot."""

    OK = "ok"
    DEGRADED = "degraded"


class EventType(str, Enum):
    """All outbound event types the web app must handle."""

    CALL_PLACED = "call.placed"
    CALL_ANSWERED = "call.answered"
    CALL_TURN = "call.turn"
    CALL_FACTS = "call.facts"
    CALL_ENDED = "call.ended"
    CALL_INTERRUPTED = "call.interrupted"
    NUMBER_STATUS = "number.status"
    STATUS_SNAPSHOT = "status.snapshot"


# ---------------------------------------------------------------------------
# Campaign configuration (commands)
# ---------------------------------------------------------------------------


class CampaignScript(BaseModel):
    """
    Script content and compliance fields stored per campaign version.

    api.py rejects configure requests missing any compliance field.
    FakeAgent reads version + body later; the web app owns script authoring.
    """

    version: str = Field(min_length=1, description="Web-app script version string")
    body: str = Field(
        default="",
        description="Prompt / talk track text for the agent (may be empty in tests)",
    )
    ai_disclosure: str = Field(
        min_length=1,
        description="Exact AI disclosure read to the callee (compliance)",
    )
    business_identity: str = Field(
        min_length=1,
        description="Who is calling on behalf of (compliance)",
    )
    callback_number: str = Field(
        min_length=1,
        description="Callback number given to the callee (compliance)",
    )


class CallingWindow(BaseModel):
    """
    One allowed calling interval on a given weekday in the campaign timezone.

    The dialer checks local time against these windows before placing calls.
    """

    day_of_week: int = Field(ge=0, le=6, description="0=Monday through 6=Sunday")
    start: time = Field(description="Local start time (inclusive)")
    end: time = Field(description="Local end time (exclusive)")

    @model_validator(mode="after")
    def start_before_end(self) -> CallingWindow:
        """Reject windows where end is not after start."""
        if self.start >= self.end:
            raise ValueError("calling window start must be before end")
        return self


class CallingHours(BaseModel):
    """Timezone plus weekly windows; used by dialer_loop for each campaign."""

    timezone: str = Field(
        min_length=1,
        description="IANA timezone name, e.g. America/New_York",
    )
    windows: list[CallingWindow] = Field(min_length=1)


class ConfigureCampaignRequest(BaseModel):
    """
    Body for POST /v1/campaigns/{id}/configure.

    Idempotent: re-configure with same version updates in place; new version
    replaces the in-memory script snapshot used for new calls.
    """

    script: CampaignScript
    calling_hours: CallingHours
    campaign_concurrent_cap: int = Field(
        ge=1,
        description="Max simultaneous calls for this campaign (<= global cap)",
    )


class ConfigureCampaignResponse(BaseModel):
    """Simple ack returned after campaign config is persisted."""

    ok: bool = True
    campaign_id: str
    script_version: str


class LeadInput(BaseModel):
    """One lead row sent by the web app; lead_id is the idempotency key."""

    lead_id: str = Field(min_length=1)
    phone: str = Field(min_length=1, description="E.164 or normalized dial string")
    business_name: str = Field(min_length=1)
    attributes: dict[str, Any] = Field(default_factory=dict)


class PutLeadsRequest(BaseModel):
    """Body for PUT /v1/campaigns/{id}/leads."""

    leads: list[LeadInput] = Field(min_length=1)


class PutLeadsResponse(BaseModel):
    """Reports how many leads were accepted vs skipped as duplicates."""

    ok: bool = True
    campaign_id: str
    accepted: int
    skipped_duplicate: int


class CampaignControlResponse(BaseModel):
    """Returned by start / pause / stop on a campaign."""

    ok: bool = True
    campaign_id: str
    state: CampaignRunState


class PutLimitsRequest(BaseModel):
    """Body for PUT /v1/limits — updates global slot cap."""

    global_concurrent_cap: int = Field(ge=1)


class PutLimitsResponse(BaseModel):
    """Echoes the new global cap after validation against running campaigns."""

    ok: bool = True
    global_concurrent_cap: int


class PoolNumberInput(BaseModel):
    """One caller ID entry in PUT /v1/numbers."""

    number: str = Field(min_length=1, description="E.164 caller ID")
    status: NumberStatus = NumberStatus.ACTIVE
    daily_cap: int | None = Field(
        default=None,
        ge=1,
        description="Overrides pool default from config when set",
    )


class PutNumbersRequest(BaseModel):
    """Replace or upsert the orchestrator's view of the number pool."""

    numbers: list[PoolNumberInput] = Field(min_length=1)


class PutNumbersResponse(BaseModel):
    """Summary after syncing numbers to store + in-memory pool."""

    ok: bool = True
    active: int
    retired: int


class HeartbeatRequest(BaseModel):
    """
    Body for POST /v1/heartbeat.

    Optional client timestamp for debugging; orchestrator records arrival time.
    """

    sent_at: datetime | None = None


class HeartbeatResponse(BaseModel):
    """Confirms heartbeat; includes current health for the web app UI."""

    ok: bool = True
    health: SystemHealth


class KillResponse(BaseModel):
    """Returned after kill switch drains or hangups in-flight calls."""

    ok: bool = True
    calls_terminated: int


# ---------------------------------------------------------------------------
# GET /v1/status
# ---------------------------------------------------------------------------


class CampaignStatusSnapshot(BaseModel):
    """Per-campaign row inside GET /v1/status and status.snapshot events."""

    campaign_id: str
    state: CampaignRunState
    queue_depth: int = Field(ge=0)
    in_progress: int = Field(ge=0, description="Calls currently using a slot")
    campaign_concurrent_cap: int = Field(ge=1)


class PoolNumberStatus(BaseModel):
    """Runtime view of one number for operators."""

    number: str
    status: NumberStatus
    calls_today: int = Field(ge=0)
    daily_cap: int = Field(ge=1)
    in_use: bool
    cooldown_until: datetime | None = None


class OrchestratorStatusResponse(BaseModel):
    """
    Body for GET /v1/status.

    Built by api.py from shared state.py + pool.py counters.
    """

    health: SystemHealth
    global_concurrent_cap: int
    slots_in_use: int
    slots_available: int
    last_heartbeat_at: datetime | None
    degraded_reason: str | None = None
    campaigns: list[CampaignStatusSnapshot]
    numbers: list[PoolNumberStatus]


# ---------------------------------------------------------------------------
# Outbound events (orchestrator -> web app)
# ---------------------------------------------------------------------------


class CallPlacedPayload(BaseModel):
    """Payload for call.placed."""

    from_number: str
    to_number: str
    business_name: str


class CallAnsweredPayload(BaseModel):
    """Payload for call.answered."""

    answered_by: AnsweredBy


class CallTurnPayload(BaseModel):
    """Payload for call.turn — one transcript line."""

    speaker: TurnSpeaker
    text: str
    ms_offset: int = Field(ge=0, description="Milliseconds from answer/start of agent")


class CallFactsPayload(BaseModel):
    """Payload for call.facts — structured answers extracted post-conversation."""

    facts: dict[str, Any]


class CallEndedPayload(BaseModel):
    """Payload for call.ended."""

    end_reason: CallEndReason
    duration_secs: int = Field(ge=0)
    billed_minutes: int = Field(ge=0)


class TranscriptTurn(BaseModel):
    """Stored turn used inside interrupted events."""

    speaker: TurnSpeaker
    text: str
    ms_offset: int = Field(ge=0)


class CallInterruptedPayload(BaseModel):
    """Payload for call.interrupted after crash recovery or kill mid-call."""

    end_reason: CallEndReason = CallEndReason.INTERRUPTED
    partial_transcript: list[TranscriptTurn] = Field(default_factory=list)
    facts: dict[str, Any] = Field(default_factory=dict)
    duration_secs: int = Field(default=0, ge=0)


class NumberStatusPayload(BaseModel):
    """Payload for number.status when usage or cooldown changes."""

    number: str
    status: NumberStatus
    calls_today: int = Field(ge=0)
    daily_cap: int = Field(ge=1)
    in_use: bool
    cooldown_until: datetime | None = None


class StatusSnapshotPayload(BaseModel):
    """Payload for periodic status.snapshot from watchdog_loop."""

    health: SystemHealth
    global_concurrent_cap: int
    slots_in_use: int
    campaigns: list[CampaignStatusSnapshot]
    numbers: list[PoolNumberStatus]
    degraded_reason: str | None = None


class OutboundEvent(BaseModel):
    """
    Envelope POSTed to WEB_APP_URL for every fact the orchestrator reports.

    outbox.py persists then queues these; fields at the top level are fixed so
    the web app can route without unpacking payload first.
    """

    event_id: str = Field(description="Unique id for deduplication on the web app")
    type: EventType
    campaign_id: str | None = Field(
        default=None,
        description="None for purely global events like status.snapshot",
    )
    call_id: str | None = None
    lead_id: str | None = None
    timestamp: datetime
    payload: dict[str, Any] = Field(description="Type-specific body; parse with helpers below")

    @field_validator("event_id")
    @classmethod
    def event_id_not_empty(cls, value: str) -> str:
        """Require a non-blank event_id for dedupe."""
        if not value.strip():
            raise ValueError("event_id must not be empty")
        return value


class EventAck(BaseModel):
    """
    Expected JSON body from the web app when an event is accepted.

    outbox.py removes the outbox row only after this ack (HTTP 2xx + ack true).
    """

    ok: bool = True
    event_id: str


def build_event(
    event_type: EventType,
    payload: BaseModel,
    *,
    campaign_id: str | None = None,
    call_id: str | None = None,
    lead_id: str | None = None,
    event_id: str | None = None,
    timestamp: datetime | None = None,
) -> OutboundEvent:
    """
    Create an OutboundEvent with payload serialized from a typed Pydantic model.

    Called by calls.py, pool.py, watchdog.py, and recovery code so payload
    shapes stay consistent with CONTRACT.md.
    """
    from uuid import uuid4

    return OutboundEvent(
        event_id=event_id or str(uuid4()),
        type=event_type,
        campaign_id=campaign_id,
        call_id=call_id,
        lead_id=lead_id,
        timestamp=timestamp or datetime.now(timezone.utc),
        payload=payload.model_dump(mode="json"),
    )


def parse_event_payload(event: OutboundEvent) -> BaseModel:
    """
    Parse event.payload into the correct Pydantic model for event.type.

    Used by tools/fake_web_app.py and tests to pretty-print events safely.
    """
    mapping: dict[EventType, type[BaseModel]] = {
        EventType.CALL_PLACED: CallPlacedPayload,
        EventType.CALL_ANSWERED: CallAnsweredPayload,
        EventType.CALL_TURN: CallTurnPayload,
        EventType.CALL_FACTS: CallFactsPayload,
        EventType.CALL_ENDED: CallEndedPayload,
        EventType.CALL_INTERRUPTED: CallInterruptedPayload,
        EventType.NUMBER_STATUS: NumberStatusPayload,
        EventType.STATUS_SNAPSHOT: StatusSnapshotPayload,
    }
    model = mapping[event.type]
    return model.model_validate(event.payload)


def validate_campaign_cap_against_global(
    campaign_cap: int,
    global_cap: int,
) -> None:
    """
    Raise ValueError if a campaign cap exceeds the global slot limit.

    api.py calls this on configure and when PUT /v1/limits shrinks the global cap.
    """
    if campaign_cap > global_cap:
        raise ValueError(
            f"campaign_concurrent_cap ({campaign_cap}) cannot exceed "
            f"global_concurrent_cap ({global_cap})"
        )


def validate_script_compliance(script: CampaignScript) -> None:
    """
    Explicit compliance check for configure handler (fields also enforced by Pydantic).

    api.py may call this for clearer 400 messages before persistence.
    """
    missing: list[str] = []
    if not script.ai_disclosure.strip():
        missing.append("ai_disclosure")
    if not script.business_identity.strip():
        missing.append("business_identity")
    if not script.callback_number.strip():
        missing.append("callback_number")
    if missing:
        raise ValueError(f"script missing required compliance fields: {', '.join(missing)}")