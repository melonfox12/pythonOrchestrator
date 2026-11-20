"""Quick smoke tests until dedicated modules land in later steps."""

import tempfile
from pathlib import Path

import pytest

from orchestrator.contract import (
    CampaignScript,
    CallPlacedPayload,
    EventType,
    build_event,
    validate_script_compliance,
)
from orchestrator.security import sign_request_headers, verify_signed_request
from orchestrator.state import AppState, CampaignRunState
from orchestrator.store import EventStore


def test_hmac_roundtrip():
    body = b'{"ok": true}'
    secret = "dev-secret-min-8-chars"
    headers = sign_request_headers(secret, body)
    verify_signed_request(secret, headers, body)


def test_build_event():
    e = build_event(
        EventType.CALL_PLACED,
        CallPlacedPayload(from_number="+1", to_number="+2", business_name="X"),
        campaign_id="c1",
    )
    assert e.type == EventType.CALL_PLACED


def test_compliance_empty_disclosure():
    with pytest.raises(ValueError):
        validate_script_compliance(
            CampaignScript(
                version="v",
                body="",
                ai_disclosure="   ",
                business_identity="b",
                callback_number="c",
            )
        )


def test_slot_release_idempotent():
    st = AppState(5)
    st.set_campaign_state("camp", CampaignRunState.RUNNING)
    assert st.try_acquire_slot("call-1", "camp")
    st.release_slot("call-1", "camp")
    st.release_slot("call-1", "camp")
    assert st.slots_in_use == 0


def test_store_start_stop():
    p = Path(tempfile.mkdtemp()) / "t.db"
    store = EventStore(p)
    store.start()
    store.save_global_cap(10)
    store.stop()