"""Number pool selection, cooldown, daily cap, spacing, and release rules."""

from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.contract import NumberStatus, PoolNumberInput
from orchestrator.pool import NumberPoolManager, PoolNumber


@pytest.fixture
def pool() -> NumberPoolManager:
    """Small pool with zero cooldown/spacing for most tests."""
    mgr = NumberPoolManager(
        default_daily_cap=75,
        cooldown_secs=600,
        dial_spacing_min_secs=0,
        dial_spacing_max_secs=0,
        pool_timezone="UTC",
        store=None,
    )
    mgr.sync_numbers(
        [
            PoolNumberInput(number="+1001", status=NumberStatus.ACTIVE, daily_cap=3),
            PoolNumberInput(number="+1002", status=NumberStatus.ACTIVE, daily_cap=3),
            PoolNumberInput(number="+1999", status=NumberStatus.RETIRED),
        ],
        default_daily_cap=75,
    )
    return mgr


def test_prefers_fewest_uses_today(pool: NumberPoolManager):
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    pool.numbers["+1001"].calls_today = 2
    pool.numbers["+1002"].calls_today = 1
    picked = pool.select_and_reserve(now)
    assert picked == "+1002"


def test_daily_cap_blocks_selection(pool: NumberPoolManager):
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    pool.numbers["+1001"].calls_today = 3
    pool.numbers["+1002"].calls_today = 3
    assert pool.select_and_reserve(now) is None


def test_cooldown_blocks_selection(pool: NumberPoolManager):
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    pool.numbers["+1001"].cooldown_until = now + timedelta(seconds=300)
    pool.numbers["+1002"].cooldown_until = now + timedelta(seconds=300)
    assert pool.select_and_reserve(now) is None


def test_release_increments_and_starts_cooldown(pool: NumberPoolManager):
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert pool.select_and_reserve(now) == "+1001"
    pool.release_number("+1001", now)
    n = pool.numbers["+1001"]
    assert not n.in_use
    assert n.calls_today == 1
    assert n.cooldown_until == now + timedelta(seconds=600)


def test_release_idempotent(pool: NumberPoolManager):
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    pool.select_and_reserve(now)
    pool.release_number("+1001", now)
    calls_after_first = pool.numbers["+1001"].calls_today
    pool.release_number("+1001", now)
    assert pool.numbers["+1001"].calls_today == calls_after_first


def test_dial_spacing():
    mgr = NumberPoolManager(
        default_daily_cap=75,
        cooldown_secs=0,
        dial_spacing_min_secs=20,
        dial_spacing_max_secs=20,
        pool_timezone="UTC",
        store=None,
    )
    mgr.sync_numbers([PoolNumberInput(number="+1001")], 75)
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert mgr.can_start_dial(now)
    mgr.record_dial_started(now)
    assert not mgr.can_start_dial(now + timedelta(seconds=10))
    assert mgr.can_start_dial(now + timedelta(seconds=20))


def test_daily_reset_on_new_local_day(pool: NumberPoolManager):
    pool.numbers["+1001"].usage_date = None
    pool.numbers["+1001"].calls_today = 99
    pool._maybe_roll_daily(pool.numbers["+1001"])
    assert pool.numbers["+1001"].calls_today == 0


def test_in_use_not_reselected(pool: NumberPoolManager):
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    first = pool.select_and_reserve(now)
    second = pool.select_and_reserve(now)
    assert first == "+1001"
    assert second == "+1002"
    assert pool.select_and_reserve(now) is None