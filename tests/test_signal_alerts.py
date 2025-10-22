from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

import types
import httpx
import pytest

from app.config import GovernanceRules, Settings, Thresholds
from app.models import (
    MetricsSnapshot,
    SignalConfidence,
    SignalEvent,
    SignalSetup,
    SignalSetupType,
    VolumeProfile,
    VolumeProfileBin,
)
from app.signal_alerts import SignalAlertConfig, SignalAlertFormatter, SignalAlertPipeline


class _DummyResponse:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("error", request=None, response=None)


def _build_settings(*, enabled: bool, include_medium: bool) -> Settings:
    return Settings(
        metrics_snapshot_path=types.SimpleNamespace(exists=lambda: True),  # type: ignore[arg-type]
        alert_webhook_url=None,
        backtest_log_path=types.SimpleNamespace(),  # type: ignore[arg-type]
        thresholds=Thresholds(),
        governance_rules=GovernanceRules(),
        environment="test",
        telegram_bot_token="token",
        telegram_chat_id="chat",
        signal_alerts_enabled=enabled,
        signal_alerts_include_medium=include_medium,
        web_base_url="http://localhost:8080",
    )


def _make_signal(
    *,
    signal_id: int,
    symbol: str,
    when: datetime,
    setup: SignalSetup,
) -> SignalEvent:
    return SignalEvent(
        id=signal_id,
        symbol=symbol,
        status="active",
        generated_at=when,
        cadence_seconds=600,
        setup=setup,
    )


def _squeeze_setup(score: float, momentum: float) -> SignalSetup:
    return SignalSetup(
        type=SignalSetupType.SQUEEZE_REVERSAL,
        confidence=SignalConfidence.HIGH if score >= 0.8 else SignalConfidence.MEDIUM,
        score=score,
        metadata={
            "bb_width": 0.009,
            "kc_width": 0.014,
            "compression_ratio": 0.64,
            "momentum": momentum,
            "momentum_shift": 0.41,
            "funding_rate": -0.002,
            "basis": 0.0015,
        },
        volume_profile=VolumeProfile(
            bins=[
                VolumeProfileBin(price=27000.0, buy_volume=8.0, sell_volume=5.0, total_volume=13.0),
                VolumeProfileBin(price=27005.0, buy_volume=6.5, sell_volume=3.0, total_volume=9.5),
            ],
            value_area=(27000.0, 27005.0),
        ),
    )


def _absorption_setup(score: float, delta_volume: float) -> SignalSetup:
    return SignalSetup(
        type=SignalSetupType.ABSORPTION,
        confidence=SignalConfidence.HIGH if score >= 0.8 else SignalConfidence.MEDIUM,
        score=score,
        metadata={
            "orderflow_imbalance": 1.9,
            "delta_volume": delta_volume,
            "basis": 0.0004,
            "liquidation_cluster": 0.55,
        },
        volume_profile=VolumeProfile(
            bins=[
                VolumeProfileBin(price=1800.0, buy_volume=4.2, sell_volume=2.5, total_volume=6.7),
                VolumeProfileBin(price=1805.0, buy_volume=3.8, sell_volume=3.1, total_volume=6.9),
            ],
            value_area=(1800.0, 1805.0),
        ),
    )


def test_pipeline_sends_high_confidence_only_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2099, 10, 21, 19, 55, tzinfo=timezone.utc)
    high = _make_signal(signal_id=1, symbol="BTCUSDT", when=now - timedelta(minutes=2), setup=_squeeze_setup(0.82, 0.32))
    medium = _make_signal(signal_id=2, symbol="ETHUSDT", when=now - timedelta(minutes=1), setup=_absorption_setup(0.71, 1.4))
    snapshot = MetricsSnapshot(ingestions=[], signals=[high, medium], executions=[])

    calls: List[dict] = []

    def fake_post(url: str, json: dict, timeout: float):  # type: ignore[override]
        calls.append({"url": url, "json": json, "timeout": timeout})
        return _DummyResponse(200)

    monkeypatch.setattr(httpx, "post", fake_post)

    settings = _build_settings(enabled=True, include_medium=False)
    pipeline = SignalAlertPipeline(settings)

    delivered = pipeline.process(snapshot)

    assert delivered == [1]
    # Only one Telegram call should have been made
    assert len(calls) == 1
    assert "sendMessage" in calls[0]["url"]
    assert "BTCUSDT" in calls[0]["json"]["text"]


def test_pipeline_includes_medium_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2099, 10, 21, 19, 55, tzinfo=timezone.utc)
    high = _make_signal(signal_id=11, symbol="BTCUSDT", when=now - timedelta(minutes=3), setup=_squeeze_setup(0.85, 0.12))
    medium = _make_signal(signal_id=12, symbol="ETHUSDT", when=now - timedelta(minutes=1), setup=_absorption_setup(0.71, 1.4))
    snapshot = MetricsSnapshot(ingestions=[], signals=[high, medium], executions=[])

    calls: List[dict] = []

    def fake_post(url: str, json: dict, timeout: float):  # type: ignore[override]
        calls.append({"url": url, "json": json, "timeout": timeout})
        return _DummyResponse(200)

    monkeypatch.setattr(httpx, "post", fake_post)

    settings = _build_settings(enabled=True, include_medium=True)
    pipeline = SignalAlertPipeline(settings)

    delivered = pipeline.process(snapshot)

    assert delivered == [11, 12]
    assert len(calls) == 2


def test_pipeline_disabled_no_sends(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2099, 10, 21, 19, 55, tzinfo=timezone.utc)
    high = _make_signal(signal_id=21, symbol="BTCUSDT", when=now - timedelta(minutes=2), setup=_squeeze_setup(0.82, 0.32))
    snapshot = MetricsSnapshot(ingestions=[], signals=[high], executions=[])

    def fake_post(url: str, json: dict, timeout: float):  # type: ignore[override]
        raise AssertionError("HTTP should not be called when disabled")

    monkeypatch.setattr(httpx, "post", fake_post)

    settings = _build_settings(enabled=False, include_medium=False)
    pipeline = SignalAlertPipeline(settings)

    delivered = pipeline.process(snapshot)

    assert delivered == []


def test_retry_on_transient_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2099, 10, 21, 19, 55, tzinfo=timezone.utc)
    high = _make_signal(signal_id=31, symbol="BTCUSDT", when=now - timedelta(minutes=1), setup=_squeeze_setup(0.82, 0.32))
    snapshot = MetricsSnapshot(ingestions=[], signals=[high], executions=[])

    attempts = {"count": 0}

    def flaky_post(url: str, json: dict, timeout: float):  # type: ignore[override]
        attempts["count"] += 1
        # Fail twice, succeed on third attempt
        if attempts["count"] < 3:
            raise httpx.ConnectError("boom", request=None)
        return _DummyResponse(200)

    monkeypatch.setattr(httpx, "post", flaky_post)

    settings = _build_settings(enabled=True, include_medium=False)
    pipeline = SignalAlertPipeline(settings, config=SignalAlertConfig(max_retries=3, enabled=True))

    delivered = pipeline.process(snapshot)

    assert delivered == [31]
    assert attempts["count"] == 3


def test_message_format_includes_key_sections() -> None:
    now = datetime(2099, 10, 21, 19, 55, tzinfo=timezone.utc)
    signal = _make_signal(signal_id=41, symbol="BTCUSDT", when=now, setup=_squeeze_setup(0.82, 0.32))
    formatter = SignalAlertFormatter(base_url="http://example.com")

    text = formatter.format(signal)

    assert "[Signal] BTCUSDT" in text
    assert "Entry:" in text and "Stop:" in text and "Target:" in text
    assert "Rationale:" in text
    assert "View: http://example.com/dashboard?symbol=BTCUSDT" in text
