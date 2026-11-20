"""
Caller ID number pool: selection, reservation, cooldown, and dial spacing.

Why this exists:
  Outbound reputation depends on rotating numbers and not over-dialing each
  caller ID. The dialer asks this module for a number before placing a call.

Who calls it:
  dialer_loop checks can_start_dial() then select_and_reserve().
  calls.py release_number() when a call ends.
  api.py PUT /v1/numbers refreshes config via sync_numbers().
  watchdog includes pool status in snapshots.
"""

from __future__ import annotations

import random
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from orchestrator.contract import NumberStatus, PoolNumberInput
from orchestrator.store import EventStore


@dataclass
class PoolNumber:
    """
    Runtime state for one caller ID.

    store.py persists counters; PoolManager mirrors them in memory for speed.
    """

    number: str
    status: NumberStatus = NumberStatus.ACTIVE
    daily_cap: int = 75
    calls_today: int = 0
    last_used_at: datetime | None = None
    cooldown_until: datetime | None = None
    in_use: bool = False
    usage_date: date | None = None


@dataclass
class NumberPoolManager:
    """
    Thread-safe manager for all pool numbers and pool-wide dial spacing.

    main.py constructs one instance and shares it across dialer and API.
    """

    default_daily_cap: int
    cooldown_secs: int
    dial_spacing_min_secs: float
    dial_spacing_max_secs: float
    pool_timezone: str
    store: EventStore | None = None
    numbers: dict[str, PoolNumber] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _last_dial_started_at: datetime | None = None
    _next_allowed_dial_at: datetime | None = None

    def _tz(self) -> ZoneInfo:
        """Resolve IANA timezone for daily cap reset."""
        return ZoneInfo(self.pool_timezone)

    def _today_local(self) -> date:
        """Current calendar date in the pool timezone."""
        return datetime.now(self._tz()).date()

    def _maybe_roll_daily(self, number: PoolNumber) -> None:
        """
        Reset calls_today at local midnight for this number.

        Called before selection and when syncing from disk.
        """
        today = self._today_local()
        if number.usage_date != today:
            number.calls_today = 0
            number.usage_date = today
            self._persist_number(number)

    def sync_numbers(self, inputs: list[PoolNumberInput], default_daily_cap: int) -> tuple[int, int]:
        """
        Upsert numbers from PUT /v1/numbers (memory + store).

        Returns (active_count, retired_count).
        """
        with self._lock:
            for item in inputs:
                cap = item.daily_cap if item.daily_cap is not None else default_daily_cap
                existing = self.numbers.get(item.number)
                if existing is None:
                    self.numbers[item.number] = PoolNumber(
                        number=item.number,
                        status=item.status,
                        daily_cap=cap,
                        usage_date=self._today_local(),
                    )
                else:
                    existing.status = item.status
                    existing.daily_cap = cap
                self._maybe_roll_daily(self.numbers[item.number])

            if self.store is not None:
                self.store.upsert_pool_numbers(inputs, default_daily_cap)

            active = sum(1 for n in self.numbers.values() if n.status == NumberStatus.ACTIVE)
            retired = sum(1 for n in self.numbers.values() if n.status == NumberStatus.RETIRED)
            return active, retired

    def hydrate_from_store(self, rows: list[dict]) -> None:
        """
        Load pool rows at startup from EventStore.load_pool_numbers().

        main.py calls this before accepting traffic.
        """
        with self._lock:
            for row in rows:
                usage_date = (
                    date.fromisoformat(row["usage_date"])
                    if row.get("usage_date")
                    else self._today_local()
                )
                self.numbers[row["number"]] = PoolNumber(
                    number=row["number"],
                    status=NumberStatus(row["status"]),
                    daily_cap=row["daily_cap"],
                    calls_today=row["calls_today"],
                    last_used_at=(
                        datetime.fromisoformat(row["last_used_at"])
                        if row.get("last_used_at")
                        else None
                    ),
                    cooldown_until=(
                        datetime.fromisoformat(row["cooldown_until"])
                        if row.get("cooldown_until")
                        else None
                    ),
                    in_use=row["in_use"],
                    usage_date=usage_date,
                )
                self._maybe_roll_daily(self.numbers[row["number"]])

    def _persist_number(self, number: PoolNumber) -> None:
        """Write number counters to SQLite when store is attached."""
        if self.store is None:
            return
        self.store.update_number_row(
            number.number,
            {
                "calls_today": number.calls_today,
                "last_used_at": number.last_used_at.isoformat() if number.last_used_at else None,
                "cooldown_until": number.cooldown_until.isoformat() if number.cooldown_until else None,
                "in_use": 1 if number.in_use else 0,
                "usage_date": (number.usage_date or self._today_local()).isoformat(),
                "status": number.status.value,
                "daily_cap": number.daily_cap,
            },
        )

    def _is_selectable(self, number: PoolNumber, now: datetime) -> bool:
        """True if this number could be chosen right now."""
        self._maybe_roll_daily(number)
        if number.status != NumberStatus.ACTIVE:
            return False
        if number.in_use:
            return False
        if number.calls_today >= number.daily_cap:
            return False
        if number.cooldown_until is not None and now < number.cooldown_until:
            return False
        return True

    def select_and_reserve(self, now: datetime | None = None) -> str | None:
        """
        Pick the best active number and mark it in_use immediately.

        dialer_loop calls after can_start_dial(). Preference: fewest calls_today,
        then oldest last_used_at (None treated as oldest).

        Returns E.164 string or None if no number is available.
        """
        now = now or datetime.now(timezone.utc)
        with self._lock:
            candidates = [n for n in self.numbers.values() if self._is_selectable(n, now)]
            if not candidates:
                return None

            candidates.sort(
                key=lambda n: (
                    n.calls_today,
                    n.last_used_at or datetime.min.replace(tzinfo=timezone.utc),
                )
            )
            chosen = candidates[0]
            chosen.in_use = True
            self._persist_number(chosen)
            return chosen.number

    def cancel_reservation(self, number: str) -> None:
        """
        Clear in_use after a failed dial before the call actually started.

        dialer_loop uses this when slot acquisition or place_call fails.
        Does not increment calls_today or start cooldown.
        """
        with self._lock:
            pool_number = self.numbers.get(number)
            if pool_number is None:
                return
            pool_number.in_use = False
            self._persist_number(pool_number)

    def release_number(self, number: str, now: datetime | None = None) -> None:
        """
        Free a number after a call ends (idempotent if already released).

        Increments calls_today, sets last_used_at, starts cooldown timer.
        calls.py always calls this from cleanup.
        """
        now = now or datetime.now(timezone.utc)
        with self._lock:
            pool_number = self.numbers.get(number)
            if pool_number is None:
                return
            if not pool_number.in_use and pool_number.last_used_at is not None:
                # Already released for this dial cycle
                return

            pool_number.in_use = False
            self._maybe_roll_daily(pool_number)
            pool_number.calls_today += 1
            pool_number.last_used_at = now
            pool_number.cooldown_until = now + timedelta(seconds=self.cooldown_secs)
            self._persist_number(pool_number)

    def record_dial_started(self, now: datetime | None = None) -> None:
        """
        Update pool-wide spacing clock after a dial actually starts.

        Sets the next allowed dial time with random jitter between min/max spacing.
        """
        now = now or datetime.now(timezone.utc)
        jitter = random.uniform(self.dial_spacing_min_secs, self.dial_spacing_max_secs)
        with self._lock:
            self._last_dial_started_at = now
            self._next_allowed_dial_at = now + timedelta(seconds=jitter)

    def can_start_dial(self, now: datetime | None = None) -> bool:
        """
        True if pool-wide spacing allows another dial to begin.

        dialer_loop checks this before select_and_reserve().
        """
        now = now or datetime.now(timezone.utc)
        with self._lock:
            if self._next_allowed_dial_at is None:
                return True
            return now >= self._next_allowed_dial_at

    def status_list(self) -> list[dict]:
        """
        Build rows for GET /v1/status and status.snapshot.

        api.py and watchdog read this; values match PoolNumberStatus in contract.
        """
        now = datetime.now(timezone.utc)
        with self._lock:
            rows = []
            for number in self.numbers.values():
                self._maybe_roll_daily(number)
                rows.append(
                    {
                        "number": number.number,
                        "status": number.status,
                        "calls_today": number.calls_today,
                        "daily_cap": number.daily_cap,
                        "in_use": number.in_use,
                        "cooldown_until": number.cooldown_until,
                        "selectable_now": self._is_selectable(number, now),
                    }
                )
            return rows