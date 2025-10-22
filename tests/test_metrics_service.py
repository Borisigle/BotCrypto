from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.config import GovernanceRules, Settings, Thresholds
from app.metrics_service import MetricsService
from app.models import (
    ExecutionEvent,
    IngestionEvent,
    MetricsSnapshot,
    SignalConfidence,
    SignalEvent,
    SignalSetup,
    SignalSetupType,
    VolumeProfile,
    VolumeProfileBin,
)


class StubRepository:
    def __init__(self, snapshot: MetricsSnapshot) -> None:
        self._snapshot = snapshot

    def fetch_snapshot(self) -> MetricsSnapshot:
        return self._snapshot


def _build_settings() -> Settings:
    return Settings(
        metrics_snapshot_path=Path("/tmp/snapshot.json"),
        alert_webhook_url=None,
        backtest_log_path=Path("/tmp"),
        thresholds=Thresholds(),
        governance_rules=GovernanceRules(),
        environment="test",
        telegram_bot_token=None,
        telegram_chat_id=None,
    )


def test_signal_summary_reports_setup_breakdown_and_scores() -> None:
    now = datetime(2099, 10, 21, 19, 55, tzinfo=timezone.utc)
    ingestion_event = IngestionEvent(source="binance", received_at=now, latency_seconds=24.0)

    squeeze_setup = SignalSetup(
        type=SignalSetupType.SQUEEZE_REVERSAL,
        confidence=SignalConfidence.HIGH,
        score=0.82,
        metadata={"compression_ratio": 0.64, "basis": 0.0015},
        volume_profile=VolumeProfile(
            bins=[
                VolumeProfileBin(price=27000.0, buy_volume=8.0, sell_volume=5.0, total_volume=13.0)
            ],
            value_area=(27000.0, 27000.0),
        ),
    )

    absorption_setup = SignalSetup(
        type=SignalSetupType.ABSORPTION,
        confidence=SignalConfidence.MEDIUM,
        score=0.71,
        metadata={"orderflow_imbalance": 1.9, "basis": 0.0004},
        volume_profile=VolumeProfile(
            bins=[
                VolumeProfileBin(price=1800.0, buy_volume=4.2, sell_volume=2.5, total_volume=6.7)
            ],
            value_area=(1800.0, 1800.0),
        ),
    )

    signals = [
        SignalEvent(
            id=1,
            symbol="BTCUSDT",
            status="active",
            generated_at=now - timedelta(minutes=10),
            cadence_seconds=600,
            setup=squeeze_setup,
            outcome="win",
            return_pct=0.012,
        ),
        SignalEvent(
            id=2,
            symbol="ETHUSDT",
            status="active",
            generated_at=now - timedelta(minutes=40),
            cadence_seconds=900,
            setup=absorption_setup,
            outcome="loss",
            return_pct=-0.006,
        ),
    ]

    snapshot = MetricsSnapshot(
        ingestions=[ingestion_event],
        signals=signals,
        executions=[
            ExecutionEvent(signal_id=1, closed_at=now + timedelta(minutes=25), outcome="win", return_pct=0.011)
        ],
    )

    service = MetricsService(repository=StubRepository(snapshot), settings=_build_settings())
    metrics = service.collect()

    assert metrics.signals.total == 2
    assert metrics.signals.by_setup == {"squeeze_reversal": 1, "absorption": 1}
    assert metrics.signals.confidence_breakdown == {"high": 1, "medium": 1}
    assert metrics.signals.average_score == pytest.approx((0.82 + 0.71) / 2, rel=1e-6)
    assert metrics.signals.by_status["active"] == 2
    assert metrics.signals.last_60_minutes == 2
    assert metrics.signals.last_24_hours == 2
