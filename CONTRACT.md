# API contract (web app ↔ orchestrator)

This document describes every **command** (web app → orchestrator) and **event** (orchestrator → web app). The canonical machine-readable definitions live in `orchestrator/contract.py`.

## Authentication

All **commands** must include:

| Header | Value |
|--------|--------|
| `X-Timestamp` | Unix time in **seconds** (UTC) |
| `X-Signature` | Hex-encoded **HMAC-SHA256** of `f"{timestamp}.{raw_body}"` using `HMAC_SECRET` |

Requests with a timestamp older than **5 minutes** are rejected. The same scheme is used when the orchestrator **POSTs events** to `WEB_APP_URL`.

## Conventions

- Paths are prefixed with `/v1`.
- JSON field names are **snake_case**.
- Timestamps in JSON are **ISO-8601 UTC** with a `Z` or offset.
- `campaign_id` in the URL is an opaque string chosen by the web app.
- Commands should be **idempotent** where noted; the orchestrator uses ids like `lead_id` and `event_id` for deduplication.

---

## Commands (web app → orchestrator)

### `POST /v1/campaigns/{campaign_id}/configure`

Upsert campaign settings and script. **Idempotent** per `campaign_id`; a new `script.version` replaces the script used for **new** calls.

**Compliance:** `script` **must** include non-empty `ai_disclosure`, `business_identity`, and `callback_number`. Configure is rejected if any are missing.

**Request body:**

```json
{
  "script": {
    "version": "2025-11-20.3",
    "body": "You are calling about ...",
    "ai_disclosure": "This call may use an AI assistant.",
    "business_identity": "Merchfly Outreach Team",
    "callback_number": "+18005551212"
  },
  "calling_hours": {
    "timezone": "America/New_York",
    "windows": [
      { "day_of_week": 0, "start": "09:00:00", "end": "17:00:00" },
      { "day_of_week": 1, "start": "09:00:00", "end": "17:00:00" }
    ]
  },
  "campaign_concurrent_cap": 5
}
```

| Field | Notes |
|-------|--------|
| `day_of_week` | 0 = Monday, 6 = Sunday |
| `campaign_concurrent_cap` | Must be ≤ global cap (`PUT /v1/limits`) |

**Response `200`:**

```json
{
  "ok": true,
  "campaign_id": "camp_abc",
  "script_version": "2025-11-20.3"
}
```

---

### `PUT /v1/campaigns/{campaign_id}/leads`

Queue a batch of leads. **Idempotent** on `lead_id`: re-sending the same `lead_id` for the same campaign does **not** create a duplicate.

**Request body:**

```json
{
  "leads": [
    {
      "lead_id": "lead_001",
      "phone": "+15551234567",
      "business_name": "Acme Plumbing",
      "attributes": { "vertical": "home_services" }
    }
  ]
}
```

**Response `200`:**

```json
{
  "ok": true,
  "campaign_id": "camp_abc",
  "accepted": 1,
  "skipped_duplicate": 0
}
```

---

### `POST /v1/campaigns/{campaign_id}/start`

Set campaign state to **running** (dialer may place calls when hours, slots, and numbers allow). Campaign must already be configured.

**Response `200`:**

```json
{
  "ok": true,
  "campaign_id": "camp_abc",
  "state": "running"
}
```

### `POST /v1/campaigns/{campaign_id}/pause`

Set state to **paused** (no new dials; in-flight calls continue).

**Response:** same shape as start, with `"state": "paused"`.

### `POST /v1/campaigns/{campaign_id}/stop`

Set state to **stopped** (no new dials; in-flight calls continue unless killed globally).

**Response:** same shape as start, with `"state": "stopped"`.

---

### `PUT /v1/limits`

Update the **global** concurrent call cap (slots shared across campaigns).

**Request body:**

```json
{
  "global_concurrent_cap": 10
}
```

**Response `200`:**

```json
{
  "ok": true,
  "global_concurrent_cap": 10
}
```

---

### `PUT /v1/numbers`

Upsert the **caller ID pool** (does not reset today's usage counters for unchanged numbers).

**Request body:**

```json
{
  "numbers": [
    {
      "number": "+18005550101",
      "status": "active",
      "daily_cap": 75
    },
    {
      "number": "+18005550102",
      "status": "retired"
    }
  ]
}
```

| `status` | Meaning |
|----------|---------|
| `active` | May be selected for outbound dials |
| `retired` | Never selected; existing counters kept for audit |

**Response `200`:**

```json
{
  "ok": true,
  "active": 1,
  "retired": 1
}
```

---

### `POST /v1/heartbeat`

Web app liveness signal. If heartbeats stop for `HEARTBEAT_TIMEOUT_SECS`, the orchestrator **stops starting new calls**, lets live calls finish, and sets health to **degraded**.

**Request body (optional):**

```json
{
  "sent_at": "2025-11-20T19:30:00Z"
}
```

**Response `200`:**

```json
{
  "ok": true,
  "health": "ok"
}
```

---

### `POST /v1/kill`

**Emergency stop:** hang up all live calls and halt all dialing immediately.

**Response `200`:**

```json
{
  "ok": true,
  "calls_terminated": 3
}
```

---

### `GET /v1/status`

Snapshot for operators (same facts as `status.snapshot`, but synchronous).

**Response `200`:**

```json
{
  "health": "ok",
  "global_concurrent_cap": 10,
  "slots_in_use": 2,
  "slots_available": 8,
  "last_heartbeat_at": "2025-11-20T19:29:55Z",
  "degraded_reason": null,
  "campaigns": [
    {
      "campaign_id": "camp_abc",
      "state": "running",
      "queue_depth": 42,
      "in_progress": 2,
      "campaign_concurrent_cap": 5
    }
  ],
  "numbers": [
    {
      "number": "+18005550101",
      "status": "active",
      "calls_today": 12,
      "daily_cap": 75,
      "in_use": true,
      "cooldown_until": null
    }
  ]
}
```

---

## Events (orchestrator → web app)

Delivery: **HTTP POST** to `WEB_APP_URL` (path is defined by your web app; the fake tool uses `/v1/events`). Body is JSON. Same HMAC headers as commands.

The web app should respond with **`200`** and:

```json
{
  "ok": true,
  "event_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

The outbox retries with exponential backoff until acked.

### Common envelope

Every event:

| Field | Type | Notes |
|-------|------|--------|
| `event_id` | string | Unique; use for deduplication |
| `type` | string | See table below |
| `campaign_id` | string \| null | `null` for global `status.snapshot` |
| `call_id` | string \| null | Set for call-scoped events |
| `lead_id` | string \| null | Set when a lead is known |
| `timestamp` | string (ISO-8601) | UTC |
| `payload` | object | Type-specific |

### Event types

| `type` | When | `payload` highlights |
|--------|------|----------------------|
| `call.placed` | Outbound dial started | `from_number`, `to_number`, `business_name` |
| `call.answered` | Callee side answered | `answered_by`: `human` \| `voicemail` \| `unknown` |
| `call.turn` | Transcript chunk | `speaker`, `text`, `ms_offset` |
| `call.facts` | Structured extraction | `facts` object |
| `call.ended` | Call finished | `end_reason`, `duration_secs`, `billed_minutes` |
| `call.interrupted` | Crash/kill recovery | `partial_transcript`, `facts`, `end_reason` |
| `number.status` | Pool usage change | `calls_today`, `cooldown_until`, `in_use`, … |
| `status.snapshot` | Watchdog (~15s) | Same summary as `GET /v1/status` |

### `call.ended` — `end_reason` values

| Value | Meaning |
|-------|---------|
| `completed` | Conversation finished normally |
| `no_answer` | No pickup |
| `voicemail` | Voicemail reached / left |
| `busy` | Busy signal |
| `hung_up_early` | Callee hung up before normal end |
| `failed` | Technical / telephony failure |
| `do_not_call_requested` | Callee asked not to be called |
| `interrupted` | Kill, timeout, or crash |

### Example: `call.answered`

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "type": "call.answered",
  "campaign_id": "camp_abc",
  "call_id": "call_xyz",
  "lead_id": "lead_001",
  "timestamp": "2025-11-20T19:31:02Z",
  "payload": {
    "answered_by": "human"
  }
}
```

### Example: `call.turn`

```json
{
  "event_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "type": "call.turn",
  "campaign_id": "camp_abc",
  "call_id": "call_xyz",
  "lead_id": "lead_001",
  "timestamp": "2025-11-20T19:31:15Z",
  "payload": {
    "speaker": "agent",
    "text": "Hi, this is Merchfly calling about your account.",
    "ms_offset": 3200
  }
}
```

### Example: `call.ended`

```json
{
  "event_id": "c3d4e5f6-a7b8-9012-cdef-123456789012",
  "type": "call.ended",
  "campaign_id": "camp_abc",
  "call_id": "call_xyz",
  "lead_id": "lead_001",
  "timestamp": "2025-11-20T19:32:40Z",
  "payload": {
    "end_reason": "completed",
    "duration_secs": 87,
    "billed_minutes": 2
  }
}
```

### Example: `status.snapshot`

```json
{
  "event_id": "d4e5f6a7-b8c9-0123-def0-234567890123",
  "type": "status.snapshot",
  "campaign_id": null,
  "call_id": null,
  "lead_id": null,
  "timestamp": "2025-11-20T19:30:00Z",
  "payload": {
    "health": "ok",
    "global_concurrent_cap": 10,
    "slots_in_use": 2,
    "campaigns": [],
    "numbers": [],
    "degraded_reason": null
  }
}
```

---

## Startup behavior (for integrators)

- After restart, the orchestrator comes up **paused** at the campaign level unless your recovery logic restores states from SQLite (see implementation in `main.py`).
- In-progress calls at crash time become `call.interrupted` events; unacked outbox entries are resent.

## Code reference

Import models and helpers from `orchestrator.contract`:

- Request/response: `ConfigureCampaignRequest`, `PutLeadsRequest`, …
- Events: `OutboundEvent`, `build_event()`, `parse_event_payload()`
- Validation: `validate_script_compliance()`, `validate_campaign_cap_against_global()`