# How the AI call orchestrator works

This guide explains the design in plain language. Everything telephony- and model-related is **fake** in phase 1 so you can learn the control plane first.

## Big picture

You run two processes locally:

1. **Orchestrator** (`uvicorn orchestrator.main:app`) — places calls, tracks slots, sends events.
2. **Fake web app** (`uvicorn tools.fake_web_app:app --port 8001`) — receives events and sends commands.

The real product will replace the fake web app with your UI and the fake backends with Telnyx/Pipecat/LLM. The **contract** (`CONTRACT.md`) stays the same.

## Concurrency: slots

A **slot** is one permission to have a call in progress.

- **Global cap** — across all campaigns (e.g. 10).
- **Campaign cap** — per campaign (e.g. 5), never above global.

When the dialer places a call, it **takes** a slot. When the call ends (any reason), `calls.py` **releases** the slot once (safe to call twice).

## Number pool

Caller IDs are shared resources:

- Each number has a **daily cap** and **cooldown** after use.
- The pool enforces **spacing** between dial starts (20–30s + jitter).
- On pick: prefer fewest uses today, then least recently used.

## Three background loops

| Loop | Interval | Job |
|------|----------|-----|
| **Dialer** | ~1s | Round-robin running campaigns, check hours/caps/pool, start calls |
| **Outbox** | event-driven | POST signed JSON to web app until ack; retry with backoff |
| **Watchdog** | ~15s | Heartbeat timeout → degraded, max call length, `status.snapshot` |

## One call’s journey

1. Dialer dequeues a lead, reserves a number, acquires a slot.
2. `FakeTelephony` simulates ring + outcome.
3. Events: `call.placed`, `call.answered`.
4. If a human answers, `FakeAgent` streams `call.turn` events, then `call.facts`.
5. `call.ended` with reason and duration.
6. Cleanup releases slot and number; lead marked done in SQLite.

Every exit path (error, kill, timeout) goes through the same **cleanup**.

## Commands vs events

- **Commands** (web → orchestrator): configure, leads, start/pause/stop, limits, numbers, heartbeat, kill, status.
- **Events** (orchestrator → web): call lifecycle, number status, periodic snapshots.

All bodies are **HMAC-signed** with `HMAC_SECRET`.

## Persistence and recovery

SQLite stores config, queued leads, calls, transcript, outbox, and number counters.

**Rule:** write to disk **before** queueing an event for HTTP.

On restart:

- Campaigns load as **paused** (no surprise dialing).
- In-flight calls become `call.interrupted`.
- Unacknowledged outbox rows are sent again.

## Health and kill

- No heartbeat for `HEARTBEAT_TIMEOUT_SECS` → **degraded** (no new dials).
- `POST /v1/kill` → hang up all calls, stop dialing immediately.

## Where to read code

| Topic | File |
|-------|------|
| API shapes | `orchestrator/contract.py`, `CONTRACT.md` |
| Signing | `orchestrator/security.py` |
| Memory state | `orchestrator/state.py` |
| Disk | `orchestrator/store.py` |
| Numbers | `orchestrator/pool.py` |
| Call flow | `orchestrator/calls.py` |
| Loops | `dialer.py`, `outbox.py`, `watchdog.py` |
| Wiring | `orchestrator/main.py` |

## Try the demo

Terminal 1:

```bash
set WEB_APP_URL=http://127.0.0.1:8001
set HMAC_SECRET=dev-secret-min-8-chars
uvicorn orchestrator.main:app --port 8000
```

Terminal 2:

```bash
set HMAC_SECRET=dev-secret-min-8-chars
uvicorn tools.fake_web_app:app --port 8001
```

Terminal 3:

```bash
curl -X POST http://127.0.0.1:8001/demo/run-two-campaigns
```

Watch terminal 2 for printed events.