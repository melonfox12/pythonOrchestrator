"""
SQLite persistence with a background writer thread.

Why this exists:
  Survive restarts: campaigns, queued leads, calls, transcript, outbox, and
  number counters must be on disk before we risk losing events. Disk writes must
  not block call handling, so mutations enqueue to a writer thread.

Who calls it:
  main.py opens the store at startup and hydrates state.py.
  api.py enqueues campaign/lead/limit updates.
  calls.py and outbox.py enqueue call rows and outbox events (write-first rule).
"""

from __future__ import annotations

import json
import sqlite3
import threading
import queue
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from orchestrator.contract import (
    CampaignRunState,
    CampaignScript,
    CallingHours,
    EventType,
    LeadInput,
    OutboundEvent,
    PoolNumberInput,
)


@dataclass(frozen=True)
class WriteJob:
    """
    One unit of work for the background writer thread.

    func receives a sqlite3.Connection that is committed after success.
    """

    label: str
    func: Callable[[sqlite3.Connection], None]


class EventStore:
    """
    Owns the SQLite file, schema, and async writer queue.

    Construct once per process; call start() before enqueueing and stop() at shutdown.
    """

    def __init__(self, db_path: Path) -> None:
        """Remember path; connection opens in start()."""
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._write_queue: queue.Queue[WriteJob | None] = queue.Queue()
        self._writer: threading.Thread | None = None
        self._running = False

    def start(self) -> None:
        """
        Create parent dirs, open DB, migrate schema, start writer thread.

        main.py lifespan calls this before loading state into memory.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema(self._conn)
        self._running = True
        self._writer = threading.Thread(target=self._writer_loop, name="store-writer", daemon=True)
        self._writer.start()

    def stop(self, timeout: float = 10.0) -> None:
        """
        Drain the write queue and close SQLite.

        main.py shutdown calls this after loops stop.
        """
        self._running = False
        self._write_queue.put(None)
        if self._writer is not None:
            self._writer.join(timeout=timeout)
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _writer_loop(self) -> None:
        """Background thread: run queued write jobs with one connection."""
        assert self._conn is not None
        while True:
            job = self._write_queue.get()
            if job is None:
                break
            try:
                job.func(self._conn)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            finally:
                self._write_queue.task_done()

    def enqueue(self, label: str, func: Callable[[sqlite3.Connection], None]) -> None:
        """
        Schedule a write without blocking the caller (rule: persist before outbox).

        api.py and calls.py use this for all mutations.
        """
        self._write_queue.put(WriteJob(label=label, func=func))

    def enqueue_sync(self, label: str, func: Callable[[sqlite3.Connection], None]) -> None:
        """
        Run a write on the calling thread (startup recovery, tests).

        Use sparingly; prefer enqueue() on the hot path.
        """
        assert self._conn is not None
        func(self._conn)
        self._conn.commit()

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        """Create tables if missing."""
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS campaigns (
                campaign_id TEXT PRIMARY KEY,
                script_json TEXT,
                calling_hours_json TEXT,
                campaign_concurrent_cap INTEGER NOT NULL DEFAULT 1,
                state TEXT NOT NULL DEFAULT 'stopped'
            );

            CREATE TABLE IF NOT EXISTS leads (
                campaign_id TEXT NOT NULL,
                lead_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                business_name TEXT NOT NULL,
                attributes_json TEXT NOT NULL DEFAULT '{}',
                queue_state TEXT NOT NULL DEFAULT 'queued',
                PRIMARY KEY (campaign_id, lead_id)
            );

            CREATE TABLE IF NOT EXISTS calls (
                call_id TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                lead_id TEXT NOT NULL,
                from_number TEXT,
                to_number TEXT,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                end_reason TEXT
            );

            CREATE TABLE IF NOT EXISTS transcript_turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_id TEXT NOT NULL,
                speaker TEXT NOT NULL,
                text TEXT NOT NULL,
                ms_offset INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS outbox (
                event_id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                campaign_id TEXT,
                call_id TEXT,
                lead_id TEXT,
                timestamp TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pool_numbers (
                number TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                daily_cap INTEGER NOT NULL,
                calls_today INTEGER NOT NULL DEFAULT 0,
                last_used_at TEXT,
                cooldown_until TEXT,
                in_use INTEGER NOT NULL DEFAULT 0,
                usage_date TEXT
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

    # --- Read paths (sync on shared connection; WAL allows concurrent readers) ---

    def load_global_cap(self, default: int) -> int:
        """Read persisted global cap; meta key global_concurrent_cap."""
        assert self._conn is not None
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = 'global_concurrent_cap'"
        ).fetchone()
        if row is None:
            return default
        return int(row[0])

    def save_global_cap(self, cap: int) -> None:
        """Persist global cap (enqueued)."""
        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO meta(key, value) VALUES ('global_concurrent_cap', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(cap),),
            )

        self.enqueue("save_global_cap", _write)

    def upsert_campaign(
        self,
        campaign_id: str,
        script: CampaignScript,
        calling_hours: CallingHours,
        campaign_concurrent_cap: int,
        state: CampaignRunState | None = None,
    ) -> None:
        """Persist campaign config from configure command."""
        script_json = script.model_dump_json()
        hours_json = calling_hours.model_dump_json()

        def _write(conn: sqlite3.Connection) -> None:
            existing = conn.execute(
                "SELECT state FROM campaigns WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            new_state = (state or CampaignRunState.STOPPED).value
            if existing is not None and state is None:
                new_state = existing[0]
            conn.execute(
                """
                INSERT INTO campaigns(
                    campaign_id, script_json, calling_hours_json,
                    campaign_concurrent_cap, state
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(campaign_id) DO UPDATE SET
                    script_json = excluded.script_json,
                    calling_hours_json = excluded.calling_hours_json,
                    campaign_concurrent_cap = excluded.campaign_concurrent_cap,
                    state = COALESCE(excluded.state, campaigns.state)
                """,
                (campaign_id, script_json, hours_json, campaign_concurrent_cap, new_state),
            )

        self.enqueue("upsert_campaign", _write)

    def set_campaign_state(self, campaign_id: str, state: CampaignRunState) -> None:
        """Persist running/paused/stopped."""
        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE campaigns SET state = ? WHERE campaign_id = ?",
                (state.value, campaign_id),
            )

        self.enqueue("set_campaign_state", _write)

    def insert_leads(self, campaign_id: str, leads: list[LeadInput]) -> None:
        """Insert leads that were accepted in memory (skip duplicates at SQL level)."""
        def _write(conn: sqlite3.Connection) -> None:
            for lead in leads:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO leads(
                        campaign_id, lead_id, phone, business_name, attributes_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        campaign_id,
                        lead.lead_id,
                        lead.phone,
                        lead.business_name,
                        json.dumps(lead.attributes),
                    ),
                )

        self.enqueue("insert_leads", _write)

    def upsert_pool_numbers(self, numbers: list[PoolNumberInput], default_daily_cap: int) -> None:
        """Sync number pool from PUT /v1/numbers."""
        def _write(conn: sqlite3.Connection) -> None:
            for item in numbers:
                cap = item.daily_cap if item.daily_cap is not None else default_daily_cap
                conn.execute(
                    """
                    INSERT INTO pool_numbers(number, status, daily_cap)
                    VALUES (?, ?, ?)
                    ON CONFLICT(number) DO UPDATE SET
                        status = excluded.status,
                        daily_cap = excluded.daily_cap
                    """,
                    (item.number, item.status.value, cap),
                )

        self.enqueue("upsert_pool_numbers", _write)

    def insert_call(
        self,
        call_id: str,
        campaign_id: str,
        lead_id: str,
        from_number: str,
        to_number: str,
        status: str,
    ) -> None:
        """Record a new call row when dialing starts."""
        started = datetime.now(timezone.utc).isoformat()

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO calls(
                    call_id, campaign_id, lead_id, from_number, to_number, status, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (call_id, campaign_id, lead_id, from_number, to_number, status, started),
            )

        self.enqueue("insert_call", _write)

    def update_call_status(
        self,
        call_id: str,
        status: str,
        end_reason: str | None = None,
    ) -> None:
        """Update call status on end or interrupt."""
        ended = datetime.now(timezone.utc).isoformat()

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE calls SET status = ?, ended_at = ?, end_reason = ?
                WHERE call_id = ?
                """,
                (status, ended, end_reason, call_id),
            )

        self.enqueue("update_call_status", _write)

    def append_transcript_turn(
        self,
        call_id: str,
        speaker: str,
        text: str,
        ms_offset: int,
    ) -> None:
        """Persist one transcript line for recovery."""
        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO transcript_turns(call_id, speaker, text, ms_offset)
                VALUES (?, ?, ?, ?)
                """,
                (call_id, speaker, text, ms_offset),
            )

        self.enqueue("append_transcript_turn", _write)

    def insert_outbox_event(self, event: OutboundEvent) -> None:
        """
        Write event to outbox BEFORE it is queued for HTTP (at-least-once delivery).

        outbox_loop loads rows where acknowledged = 0.
        """
        created = datetime.now(timezone.utc).isoformat()

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT OR REPLACE INTO outbox(
                    event_id, type, campaign_id, call_id, lead_id,
                    timestamp, payload_json, acknowledged, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    event.event_id,
                    event.type.value,
                    event.campaign_id,
                    event.call_id,
                    event.lead_id,
                    event.timestamp.isoformat(),
                    json.dumps(event.payload),
                    created,
                ),
            )

        self.enqueue("insert_outbox_event", _write)

    def insert_outbox_event_sync(self, event: OutboundEvent) -> None:
        """Synchronous variant for write-then-queue ordering in the same thread."""
        created = datetime.now(timezone.utc).isoformat()

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT OR REPLACE INTO outbox(
                    event_id, type, campaign_id, call_id, lead_id,
                    timestamp, payload_json, acknowledged, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    event.event_id,
                    event.type.value,
                    event.campaign_id,
                    event.call_id,
                    event.lead_id,
                    event.timestamp.isoformat(),
                    json.dumps(event.payload),
                    created,
                ),
            )

        self.enqueue_sync("insert_outbox_event_sync", _write)

    def mark_outbox_acknowledged(self, event_id: str) -> None:
        """Remove from retry set after web app ack."""
        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE outbox SET acknowledged = 1 WHERE event_id = ?",
                (event_id,),
            )

        self.enqueue("mark_outbox_acknowledged", _write)

    def fetch_unacknowledged_events(self) -> list[OutboundEvent]:
        """Load all pending outbox events for outbox_loop / startup recovery."""
        assert self._conn is not None
        rows = self._conn.execute(
            """
            SELECT event_id, type, campaign_id, call_id, lead_id, timestamp, payload_json
            FROM outbox WHERE acknowledged = 0 ORDER BY created_at
            """
        ).fetchall()
        events: list[OutboundEvent] = []
        for row in rows:
            events.append(
                OutboundEvent(
                    event_id=row[0],
                    type=EventType(row[1]),
                    campaign_id=row[2],
                    call_id=row[3],
                    lead_id=row[4],
                    timestamp=datetime.fromisoformat(row[5]),
                    payload=json.loads(row[6]),
                )
            )
        return events

    def fetch_in_progress_calls(self) -> list[dict[str, Any]]:
        """Return calls not in a terminal status for crash recovery."""
        assert self._conn is not None
        rows = self._conn.execute(
            """
            SELECT call_id, campaign_id, lead_id, from_number, to_number, started_at, status
            FROM calls WHERE status IN ('placing', 'ringing', 'active')
            """
        ).fetchall()
        return [
            {
                "call_id": r[0],
                "campaign_id": r[1],
                "lead_id": r[2],
                "from_number": r[3],
                "to_number": r[4],
                "started_at": r[5],
                "status": r[6],
            }
            for r in rows
        ]

    def fetch_transcript_for_call(self, call_id: str) -> list[dict[str, Any]]:
        """Load partial transcript for call.interrupted events."""
        assert self._conn is not None
        rows = self._conn.execute(
            """
            SELECT speaker, text, ms_offset FROM transcript_turns
            WHERE call_id = ? ORDER BY ms_offset
            """,
            (call_id,),
        ).fetchall()
        return [
            {"speaker": r[0], "text": r[1], "ms_offset": r[2]}
            for r in rows
        ]

    def load_campaigns(self) -> list[dict[str, Any]]:
        """Return all campaign rows for hydrating AppState."""
        assert self._conn is not None
        rows = self._conn.execute(
            "SELECT campaign_id, script_json, calling_hours_json, campaign_concurrent_cap, state FROM campaigns"
        ).fetchall()
        result = []
        for row in rows:
            result.append(
                {
                    "campaign_id": row[0],
                    "script": json.loads(row[1]) if row[1] else None,
                    "calling_hours": json.loads(row[2]) if row[2] else None,
                    "campaign_concurrent_cap": row[3],
                    "state": row[4],
                }
            )
        return result

    def load_queued_leads(self, campaign_id: str) -> list[LeadInput]:
        """Return leads still queued for one campaign."""
        assert self._conn is not None
        rows = self._conn.execute(
            """
            SELECT lead_id, phone, business_name, attributes_json FROM leads
            WHERE campaign_id = ? AND queue_state = 'queued'
            ORDER BY rowid
            """,
            (campaign_id,),
        ).fetchall()
        return [
            LeadInput(
                lead_id=r[0],
                phone=r[1],
                business_name=r[2],
                attributes=json.loads(r[3]),
            )
            for r in rows
        ]

    def mark_lead_done(self, campaign_id: str, lead_id: str) -> None:
        """Update lead queue_state after a call attempt."""
        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE leads SET queue_state = 'completed'
                WHERE campaign_id = ? AND lead_id = ?
                """,
                (campaign_id, lead_id),
            )

        self.enqueue("mark_lead_done", _write)

    def load_pool_numbers(self) -> list[dict[str, Any]]:
        """Return pool number rows for pool.py hydration."""
        assert self._conn is not None
        rows = self._conn.execute(
            """
            SELECT number, status, daily_cap, calls_today, last_used_at,
                   cooldown_until, in_use, usage_date
            FROM pool_numbers
            """
        ).fetchall()
        return [
            {
                "number": r[0],
                "status": r[1],
                "daily_cap": r[2],
                "calls_today": r[3],
                "last_used_at": r[4],
                "cooldown_until": r[5],
                "in_use": bool(r[6]),
                "usage_date": r[7],
            }
            for r in rows
        ]

    def update_number_row(self, number: str, fields: dict[str, Any]) -> None:
        """Generic pool counter update from pool.py."""
        if not fields:
            return
        columns = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [number]

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                f"UPDATE pool_numbers SET {columns} WHERE number = ?",
                values,
            )

        self.enqueue("update_number_row", _write)