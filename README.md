# AI Call Orchestrator

Python service that **places outbound calls**, manages **concurrency slots**, rotates **caller ID numbers**, and runs **one AI agent per answered call**. It reports **facts** to your web app; the web app decides business outcomes (qualified, disqualified, etc.).

**Phase 1 (current build):** all external systems are **fake**—telephony, agents, and a toy web app—so we can implement and test control logic without Telnyx, Pipecat, STT, LLM, or TTS. Real backends will plug in behind the same interfaces later.

## Mental model

| Piece | Role |
|--------|------|
| **Web app** | Sends signed commands (configure, leads, start/pause/stop). Receives signed events (call lifecycle, facts). |
| **Orchestrator** | Long-running process: dialer, slot caps, number pool, per-call agent, reliable event outbox. |
| **Campaign** | Isolated queue + config + cap; many campaigns can run at once under a global slot limit. |

## Stack

- Python 3.11+
- FastAPI + uvicorn
- pydantic, httpx
- SQLite (`sqlite3` stdlib)
- pytest

## Project layout (target)

```
orchestrator/          # Main service
  config.py            # Environment settings (step 1 ✓)
  contract.py          # Command/event models (step 2)
  ...
tools/
  fake_web_app.py      # Local event receiver + command sender (step 8)
tests/
CONTRACT.md
docs/HOW_IT_WORKS.md
```

See the task spec for the full file list.

## Quick start (after the app is wired in step 7+)

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
copy .env.example .env          # edit HMAC_SECRET and URLs
mkdir data
uvicorn orchestrator.main:app --reload
```

Fake web app (separate terminal, step 8):

```bash
uvicorn tools.fake_web_app:app --port 8001
```

## Configuration

Copy `.env.example` to `.env`. Required variables:

| Variable | Purpose |
|----------|---------|
| `WEB_APP_URL` | Where the orchestrator POSTs signed events |
| `HMAC_SECRET` | Shared HMAC key (never commit real values) |

Optional variables cover caps, heartbeat timeout, SQLite path, number-pool defaults, fake simulation probabilities, and logging. See `.env.example` for defaults.

Validate config without starting the server (step 1):

```bash
set WEB_APP_URL=http://127.0.0.1:8001
set HMAC_SECRET=dev-secret-min-8-chars
python -c "from orchestrator.config import load_settings; print(load_settings())"
```

## Docker

```bash
docker build -t call-orchestrator .
docker run --env-file .env -p 8000:8000 -v orchestrator-data:/app/data call-orchestrator
```

The image expects `orchestrator.main:app` (added in build step 7).

## Tests

```bash
pytest
```

Tests are added incrementally (pool, outbox, security, recovery, etc.) per the build plan.

## Build plan

All steps below are implemented in this repo (phase 1 / fake backends).

1. Skeleton, config, README, `.env.example`, Dockerfile  
2. `CONTRACT.md` + `contract.py`  
3. `security.py`, `store.py`, `state.py`  
4. `pool.py` + tests  
5. Fake backends  
6. `calls.py`, dialer, outbox, watchdog  
7. `api.py`, `main.py` (startup/shutdown/recovery)  
8. `tools/fake_web_app.py` + `/demo/run-two-campaigns`  
9. `docs/HOW_IT_WORKS.md`  

Tests: `pytest` (set `ORCHESTRATOR_DISABLE_LOOPS=1` for fast API tests without background loops).  

## Security

- Commands to the orchestrator are verified with **HMAC-SHA256** (`X-Timestamp` + `X-Signature`).
- Events to the web app use the same scheme.
- Do not log secrets; use only `.env.example` in git.

## License

Private / internal unless you add a license file.