"""
Reliable delivery of outbound events to the web app.

Why this exists:
  The orchestrator must not lose call facts if the web app is briefly down.
  Every event is written to SQLite first, then sent asynchronously with retries.

Who calls it:
  calls.py, pool.py (optional), watchdog.py call OutboxPublisher.publish().
  outbox_loop() runs at startup from main.py and drains the asyncio queue.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import httpx

from orchestrator.contract import EventAck, OutboundEvent
from orchestrator.security import sign_request_headers
from orchestrator.store import EventStore

if TYPE_CHECKING:
    from orchestrator.config import Settings

logger = logging.getLogger(__name__)


class OutboxPublisher:
    """
    Write-first publisher backed by a queue consumed by outbox_loop.

    main.py constructs one instance and shares it across the process.
    """

    def __init__(self, store: EventStore, queue: asyncio.Queue[OutboundEvent]) -> None:
        """Attach store for durable writes and the in-memory delivery queue."""
        self._store = store
        self._queue = queue
        self._seen: set[str] = set()

    def publish(self, event: OutboundEvent) -> None:
        """
        Persist then enqueue (rule: disk before network).

        Safe to call from sync code running under asyncio; uses put_nowait.
        """
        if event.event_id in self._seen:
            return
        self._seen.add(event.event_id)
        self._store.insert_outbox_event_sync(event)
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.error("outbox queue full; event %s stays on disk for retry", event.event_id)

    async def publish_async(self, event: OutboundEvent) -> None:
        """Async variant used during startup replay."""
        if event.event_id in self._seen:
            return
        self._seen.add(event.event_id)
        self._store.insert_outbox_event_sync(event)
        await self._queue.put(event)

    async def replay_from_store(self, events: list[OutboundEvent]) -> None:
        """
        Re-queue unacknowledged rows after crash recovery.

        main.py calls this once during lifespan startup.
        """
        for event in events:
            await self.publish_async(event)


async def outbox_loop(
    publisher: OutboxPublisher,
    store: EventStore,
    client: httpx.AsyncClient,
    settings: Settings,
    stop_event: asyncio.Event,
) -> None:
    """
    Wait on the outbox queue and POST signed events until acknowledged.

    Started as an asyncio task from main.py; exits when stop_event is set and
    the queue is drained.
    """
    url = str(settings.web_app_url).rstrip("/") + "/v1/events"
    queue = publisher._queue

    while True:
        if stop_event.is_set() and queue.empty():
            break
        try:
            event = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        delivered = False
        backoff = settings.outbox_initial_backoff_secs
        for attempt in range(settings.outbox_max_retries):
            if stop_event.is_set() and attempt > 0:
                await queue.put(event)
                break
            body = event.model_dump_json().encode("utf-8")
            headers = sign_request_headers(settings.hmac_secret, body)
            headers["Content-Type"] = "application/json"
            try:
                response = await client.post(url, content=body, headers=headers, timeout=30.0)
                if response.status_code >= 400:
                    raise httpx.HTTPStatusError(
                        "bad status",
                        request=response.request,
                        response=response,
                    )
                data = response.json()
                ack = EventAck.model_validate(data)
                if ack.event_id != event.event_id and ack.ok:
                    logger.warning(
                        "web app ack event_id mismatch: %s vs %s",
                        ack.event_id,
                        event.event_id,
                    )
                store.mark_outbox_acknowledged(event.event_id)
                delivered = True
                break
            except Exception as exc:
                logger.warning(
                    "outbox deliver attempt %s for %s failed: %s",
                    attempt + 1,
                    event.event_id,
                    exc,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, settings.outbox_max_backoff_secs)

        if not delivered and not stop_event.is_set():
            await queue.put(event)
            await asyncio.sleep(backoff)