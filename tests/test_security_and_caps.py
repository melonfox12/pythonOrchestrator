"""Security, caps, scheduling, heartbeat behavior."""

import time

import pytest

from orchestrator.contract import CampaignScript, CallingHours
from orchestrator.contract import CallingWindow as CW
from orchestrator.security import SignatureError, sign_request_headers, verify_signed_request
from orchestrator.state import AppState, CampaignRunState
from datetime import time as dt_time


def test_signature_expired():
    body = b"{}"
    secret = "dev-secret-min-8-chars"
    old_ts = int(time.time()) - 400
    headers = sign_request_headers(secret, body, timestamp=old_ts)
    with pytest.raises(SignatureError, match="older than"):
        verify_signed_request(secret, headers, body, max_age_secs=300)


def test_global_vs_campaign_caps():
    st = AppState(global_concurrent_cap=2)
    st.configure_campaign(
        "a",
        CampaignScript(
            version="1",
            body="",
            ai_disclosure="d",
            business_identity="b",
            callback_number="c",
        ),
        CallingHours(timezone="UTC", windows=[CW(day_of_week=0, start=dt_time(9, 0), end=dt_time(17, 0))]),
        campaign_concurrent_cap=2,
    )
    st.set_campaign_state("a", CampaignRunState.RUNNING)
    assert st.try_acquire_slot("c1", "a")
    assert st.try_acquire_slot("c2", "a")
    assert not st.try_acquire_slot("c3", "a")


def test_fair_round_robin():
    st = AppState(10)
    hours = CallingHours(
        timezone="UTC",
        windows=[CW(day_of_week=i, start=dt_time(0, 0), end=dt_time(23, 59)) for i in range(7)],
    )
    script = CampaignScript(
        version="1",
        body="",
        ai_disclosure="d",
        business_identity="b",
        callback_number="c",
    )
    for cid in ("one", "two"):
        st.configure_campaign(cid, script, hours, 5)
        st.set_campaign_state(cid, CampaignRunState.RUNNING)
    picks = [st.next_campaign_for_dialer() for _ in range(4)]
    assert picks[0] == "one" and picks[1] == "two" and picks[2] == "one"


def test_heartbeat_degraded_blocks_slots():
    st = AppState(5)
    st.mark_degraded("heartbeat_timeout")
    st.set_campaign_state("c", CampaignRunState.RUNNING)
    assert not st.try_acquire_slot("x", "c")
    st.record_heartbeat()
    assert st.health.value == "ok"