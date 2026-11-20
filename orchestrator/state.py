"""
In-memory shared state for campaigns, slots, queues, and active calls.

Why this exists:
  Background loops (dialer, watchdog) and API handlers run concurrently in one
  process. This module holds the fast path data structures guarded by locks.
  SQLite (store.py) is updated for durability but must not block dialing.

Who calls it:
  api.py mutates campaigns and limits.
  dialer.py reads queues and acquires slots.
  calls.py tracks active calls and releases slots on cleanup.
  watchdog.py reads health and heartbeat.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from orchestrator.contract import (
    CampaignRunState,
    CampaignScript,
    CallingHours,
    LeadInput,
    SystemHealth,
)


@dataclass
class CampaignRecord:
    """
    Runtime view of one campaign kept in AppState.campaigns.

    store.py persists the same facts; AppState is rehydrated at startup.
    """

    campaign_id: str
    script: CampaignScript | None = None
    calling_hours: CallingHours | None = None
    campaign_concurrent_cap: int = 1
    state: CampaignRunState = CampaignRunState.STOPPED
    lead_queue: deque[str] = field(default_factory=deque)
    leads_by_id: dict[str, LeadInput] = field(default_factory=dict)
    in_progress: int = 0


@dataclass
class ActiveCallRecord:
    """
    One call currently holding a concurrency slot.

    calls.py creates and removes these; watchdog uses started_at for max duration.
    """

    call_id: str
    campaign_id: str
    lead_id: str
    from_number: str
    to_number: str
    started_at: datetime
    slot_released: bool = False


class AppState:
    """
    Process-wide mutable state with a reentrant lock.

    main.py constructs one instance at startup and passes it to loops and api.
    """

    def __init__(self, global_concurrent_cap: int) -> None:
        """Initialize empty state; cap may be changed later via PUT /v1/limits."""
        self._lock = threading.RLock()
        self.global_concurrent_cap = global_concurrent_cap
        self.slots_in_use = 0
        self.campaigns: dict[str, CampaignRecord] = {}
        self.active_calls: dict[str, ActiveCallRecord] = {}
        self.health: SystemHealth = SystemHealth.OK
        self.degraded_reason: str | None = None
        self.last_heartbeat_at: datetime | None = None
        self.kill_switch: bool = False
        self.dialer_round_robin: list[str] = []
        self._released_slots: set[str] = set()

    def set_global_cap(self, cap: int) -> None:
        """
        Update the global slot limit (PUT /v1/limits).

        Called from api.py after validating against campaign caps.
        """
        with self._lock:
            self.global_concurrent_cap = cap

    def record_heartbeat(self, at: datetime | None = None) -> None:
        """
        Mark the web app as alive (POST /v1/heartbeat).

        watchdog_loop clears degraded health when heartbeats resume.
        """
        with self._lock:
            self.last_heartbeat_at = at or datetime.now(timezone.utc)
            if self.health == SystemHealth.DEGRADED and self.degraded_reason == "heartbeat_timeout":
                self.health = SystemHealth.OK
                self.degraded_reason = None

    def mark_degraded(self, reason: str) -> None:
        """
        Stop starting new calls while keeping health visible (watchdog).

        dialer_loop checks health before placing calls.
        """
        with self._lock:
            self.health = SystemHealth.DEGRADED
            self.degraded_reason = reason

    def engage_kill_switch(self) -> int:
        """
        Halt all dialing and return count of calls that were live.

        api.py POST /v1/kill sets kill_switch; dialer and calls respect it.
        """
        with self._lock:
            self.kill_switch = True
            return len(self.active_calls)

    def get_or_create_campaign(self, campaign_id: str) -> CampaignRecord:
        """
        Return existing campaign or insert a stopped placeholder.

        api.py uses this before configure/leads/start handlers.
        """
        with self._lock:
            if campaign_id not in self.campaigns:
                self.campaigns[campaign_id] = CampaignRecord(campaign_id=campaign_id)
            return self.campaigns[campaign_id]

    def configure_campaign(
        self,
        campaign_id: str,
        script: CampaignScript,
        calling_hours: CallingHours,
        campaign_concurrent_cap: int,
    ) -> CampaignRecord:
        """
        Apply configure command to memory (after store write).

        Does not change run state; campaigns stay paused/stopped until start.
        """
        with self._lock:
            record = self.get_or_create_campaign(campaign_id)
            record.script = script
            record.calling_hours = calling_hours
            record.campaign_concurrent_cap = campaign_concurrent_cap
            if campaign_id not in self.dialer_round_robin:
                self.dialer_round_robin.append(campaign_id)
            return record

    def enqueue_leads(self, campaign_id: str, leads: list[LeadInput]) -> tuple[int, int]:
        """
        Add leads to the campaign queue; skip duplicate lead_ids.

        Returns (accepted, skipped_duplicate). store.py persists accepted rows.
        """
        with self._lock:
            record = self.get_or_create_campaign(campaign_id)
            accepted = 0
            skipped = 0
            for lead in leads:
                if lead.lead_id in record.leads_by_id:
                    skipped += 1
                    continue
                record.leads_by_id[lead.lead_id] = lead
                record.lead_queue.append(lead.lead_id)
                accepted += 1
            return accepted, skipped

    def set_campaign_state(self, campaign_id: str, state: CampaignRunState) -> CampaignRecord:
        """Update running/paused/stopped (start/pause/stop commands)."""
        with self._lock:
            record = self.get_or_create_campaign(campaign_id)
            record.state = state
            return record

    def campaign_slots_available(self, campaign_id: str) -> bool:
        """
        True if this campaign may take another slot under its cap.

        dialer_loop checks this alongside global slots.
        """
        with self._lock:
            record = self.campaigns.get(campaign_id)
            if record is None:
                return False
            return record.in_progress < record.campaign_concurrent_cap

    def global_slots_available(self) -> bool:
        """True if the global cap allows another concurrent call."""
        with self._lock:
            return self.slots_in_use < self.global_concurrent_cap

    def try_acquire_slot(self, call_id: str, campaign_id: str) -> bool:
        """
        Atomically take one global and one campaign slot for a new call.

        dialer_loop calls this immediately before placing a call.
        Returns False if any limit or kill/degraded rules block dialing.
        """
        with self._lock:
            if self.kill_switch:
                return False
            if self.health == SystemHealth.DEGRADED:
                return False
            record = self.campaigns.get(campaign_id)
            if record is None or record.state != CampaignRunState.RUNNING:
                return False
            if self.slots_in_use >= self.global_concurrent_cap:
                return False
            if record.in_progress >= record.campaign_concurrent_cap:
                return False
            self.slots_in_use += 1
            record.in_progress += 1
            self._released_slots.discard(call_id)
            return True

    def register_active_call(self, call: ActiveCallRecord) -> None:
        """Track a call after slot acquisition and telephony place."""
        with self._lock:
            self.active_calls[call.call_id] = call

    def release_slot(self, call_id: str, campaign_id: str) -> None:
        """
        Free global and campaign slots for a call (idempotent).

        calls.py cleanup must call this on every exit path; safe to call twice.
        """
        with self._lock:
            if call_id in self._released_slots:
                return
            self._released_slots.add(call_id)

            if self.slots_in_use > 0:
                self.slots_in_use -= 1
            record = self.campaigns.get(campaign_id)
            if record is not None and record.in_progress > 0:
                record.in_progress -= 1

            call = self.active_calls.get(call_id)
            if call is not None:
                call.slot_released = True

    def finish_call(self, call_id: str) -> None:
        """Remove call from active_calls after cleanup (slot already released)."""
        with self._lock:
            self.active_calls.pop(call_id, None)

    def requeue_lead_front(self, campaign_id: str, lead_id: str) -> None:
        """
        Put a lead back at the head of the queue when dialing could not start.

        dialer_loop calls this if pool or slot acquisition fails after dequeue.
        """
        with self._lock:
            record = self.campaigns.get(campaign_id)
            if record is None:
                return
            record.lead_queue.appendleft(lead_id)

    def pop_next_lead_id(self, campaign_id: str) -> str | None:
        """
        Take the next queued lead for a campaign (FIFO).

        dialer_loop calls after all gates pass; returns None if queue empty.
        """
        with self._lock:
            record = self.campaigns.get(campaign_id)
            if record is None or not record.lead_queue:
                return None
            return record.lead_queue.popleft()

    def next_campaign_for_dialer(self) -> str | None:
        """
        Round-robin pick among RUNNING campaigns for fairness.

        dialer_loop advances an internal index each tick.
        """
        with self._lock:
            if not self.dialer_round_robin:
                return None
            running = [
                cid
                for cid in self.dialer_round_robin
                if self.campaigns.get(cid)
                and self.campaigns[cid].state == CampaignRunState.RUNNING
            ]
            if not running:
                return None
            if not hasattr(self, "_rr_index"):
                self._rr_index = 0
            cid = running[self._rr_index % len(running)]
            self._rr_index = (self._rr_index + 1) % len(running)
            return cid

    def status_snapshot_dict(self) -> dict[str, Any]:
        """
        Build a dict matching OrchestratorStatusResponse fields.

        api.py GET /v1/status and watchdog status.snapshot both use this.
        """
        with self._lock:
            campaigns = []
            for record in self.campaigns.values():
                campaigns.append(
                    {
                        "campaign_id": record.campaign_id,
                        "state": record.state.value,
                        "queue_depth": len(record.lead_queue),
                        "in_progress": record.in_progress,
                        "campaign_concurrent_cap": record.campaign_concurrent_cap,
                    }
                )
            return {
                "health": self.health.value,
                "global_concurrent_cap": self.global_concurrent_cap,
                "slots_in_use": self.slots_in_use,
                "slots_available": max(
                    0, self.global_concurrent_cap - self.slots_in_use
                ),
                "last_heartbeat_at": self.last_heartbeat_at,
                "degraded_reason": self.degraded_reason,
                "campaigns": campaigns,
            }