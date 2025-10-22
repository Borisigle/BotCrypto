from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

import pytest

from app.config import GovernanceRules
from app.governance import SignalGovernance
from app.models import MetricsSnapshot, SignalEvent


class DummyNotifier:
    def __init__(self) -> None:
        self.messages: List[str] = []

    def send(self, message: str) -> None:
        self.messages.append(message)


def make_signal(signal_id: int, generated_at: datetime, tier: str = "high") -> SignalEvent:
    return SignalEvent(
        id=signal_id,
        symbol="BTCUSDT",
        status="active",
        generated_at=generated_at,
        cadence_seconds=3600,
        tier=tier,
    )


def make_snapshot(signals: List[SignalEvent]) -> MetricsSnapshot:
    return MetricsSnapshot(ingestions=[], signals=signals, executions=[])


def test_governance_no_drought_retains_baseline_threshold() -> None:
    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    recent_signal = make_signal(1, generated_at=now - timedelta(hours=1))
    snapshot = make_snapshot([recent_signal])

    rules = GovernanceRules()
    notifier = DummyNotifier()
    governance = SignalGovernance(rules=rules, notifier=notifier)  # type: ignore[arg-type]

    status = governance.evaluate(snapshot, now=now)

    assert status.drought_active is False
    assert status.medium_tier_allowed is False
    assert status.adjustments == []
    assert pytest.approx(status.delta_oi_threshold) == rules.delta_oi_baseline
    assert status.rolling_counts["6"] == 1


def test_governance_drought_triggers_relaxation_and_notification() -> None:
    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    old_signal = make_signal(1, generated_at=now - timedelta(hours=40))
    snapshot = make_snapshot([old_signal])

    rules = GovernanceRules()
    notifier = DummyNotifier()
    governance = SignalGovernance(rules=rules, notifier=notifier)  # type: ignore[arg-type]

    status = governance.evaluate(snapshot, now=now)

    assert status.drought_active is True
    assert status.delta_oi_threshold == pytest.approx(rules.delta_oi_relaxed)
    assert status.medium_tier_allowed is True
    assert status.cap_exhausted is False
    assert status.adjustments[0].reason == "drought_relaxed"
    assert notifier.messages, "Expected a governance notification to be sent"
    assert "ΔOI threshold" in notifier.messages[-1]


def test_governance_resets_after_activity_returns() -> None:
    rules = GovernanceRules()
    notifier = DummyNotifier()
    governance = SignalGovernance(rules=rules, notifier=notifier)  # type: ignore[arg-type]

    drought_time = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    drought_signal = make_signal(1, generated_at=drought_time - timedelta(hours=40))
    governance.evaluate(make_snapshot([drought_signal]), now=drought_time)

    recovery_time = drought_time + timedelta(hours=1)
    recent_signal = make_signal(2, generated_at=recovery_time - timedelta(minutes=30))
    status = governance.evaluate(make_snapshot([recent_signal]), now=recovery_time)

    assert status.drought_active is False
    assert status.medium_tier_allowed is False
    assert status.delta_oi_threshold == pytest.approx(rules.delta_oi_baseline)
    assert status.adjustments[0].reason == "reset_baseline"
    assert status.adjustments[1].reason == "drought_relaxed"


def test_governance_cap_blocks_medium_tier_after_usage() -> None:
    rules = GovernanceRules(medium_tier_daily_cap=2)
    notifier = DummyNotifier()
    governance = SignalGovernance(rules=rules, notifier=notifier)  # type: ignore[arg-type]

    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    drought_signal = make_signal(1, generated_at=now - timedelta(hours=40))
    medium_signal = make_signal(2, generated_at=now - timedelta(hours=1), tier="medium")
    first_status = governance.evaluate(make_snapshot([drought_signal, medium_signal]), now=now)

    assert first_status.medium_tier_allowed is True
    assert first_status.cap_exhausted is False

    later = now + timedelta(hours=2)
    drought_signal_later = make_signal(3, generated_at=later - timedelta(hours=40))
    medium_signals = [
        make_signal(4, generated_at=later - timedelta(hours=1), tier="medium"),
        make_signal(5, generated_at=later - timedelta(minutes=30), tier="medium"),
    ]
    status = governance.evaluate(make_snapshot([drought_signal_later, *medium_signals]), now=later)

    assert status.medium_tier_allowed is False
    assert status.cap_exhausted is True
    assert status.medium_tier_remaining == 0
    assert status.adjustments[0].reason == "medium_tier_cap_exhausted"
    assert any("Medium-tier allowed" in message for message in notifier.messages)
