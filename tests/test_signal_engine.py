from __future__ import annotations

import pytest

from app.models import SignalConfidence, SignalSetupType
from app.signal_engine import SignalEngine, SignalEngineConfig, Trade


def test_signal_engine_detects_squeeze_reversal_with_high_confidence() -> None:
    engine = SignalEngine()
    indicators = {
        "bb_width": 0.009,
        "kc_width": 0.014,
        "momentum": -0.35,
        "momentum_shift": 0.32,
    }
    trades = [
        Trade(price=27001.2, quantity=4.0, side="buy"),
        Trade(price=27003.4, quantity=2.5, side="sell"),
        Trade(price=27005.6, quantity=3.1, side="buy"),
    ]

    setup = engine.evaluate(
        indicators=indicators,
        funding_rate=-0.0025,
        spot_price=27000.0,
        perp_price=27045.0,
        trades=trades,
    )

    assert setup is not None
    assert setup.type is SignalSetupType.SQUEEZE_REVERSAL
    assert setup.confidence is SignalConfidence.HIGH
    assert setup.score >= 0.8
    assert pytest.approx(setup.metadata["compression_ratio"], rel=1e-3) == indicators["bb_width"] / indicators["kc_width"]
    assert setup.volume_profile is not None
    assert len(setup.volume_profile.bins) >= 1


def test_signal_engine_prioritises_absorption_when_conditions_satisfied() -> None:
    engine = SignalEngine()
    indicators = {
        "bb_width": 0.02,  # too wide for squeeze detection
        "kc_width": 0.02,
        "orderflow_imbalance": 2.4,
        "delta_volume": 1.6,
        "liquidation_cluster": 0.72,
    }
    trades = [
        Trade(price=1875.4, quantity=3.0, side="buy"),
        Trade(price=1876.1, quantity=1.4, side="sell"),
        Trade(price=1876.4, quantity=2.2, side="buy"),
        Trade(price=1877.0, quantity=1.8, side="sell"),
    ]

    setup = engine.evaluate(
        indicators=indicators,
        funding_rate=0.0001,
        spot_price=1875.0,
        perp_price=1875.8,
        trades=trades,
    )

    assert setup is not None
    assert setup.type is SignalSetupType.ABSORPTION
    assert setup.confidence is SignalConfidence.HIGH
    assert setup.score >= 0.8
    assert pytest.approx(setup.metadata["orderflow_imbalance"], rel=1e-3) == indicators["orderflow_imbalance"]


def test_volume_profile_groups_trades_into_price_bins() -> None:
    config = SignalEngineConfig(volume_profile_bin_size=1.0, value_area_volume_fraction=0.7)
    engine = SignalEngine(config=config)
    trades = [
        Trade(price=100.2, quantity=1.0, side="buy"),
        Trade(price=100.6, quantity=0.5, side="sell"),
        Trade(price=101.1, quantity=3.0, side="buy"),
        Trade(price=101.8, quantity=1.0, side="sell"),
        Trade(price=102.2, quantity=2.0, side="buy"),
    ]

    profile = engine.build_volume_profile(trades)

    assert [bin.price for bin in profile.bins] == [100.0, 101.0, 102.0]
    totals = [bin.total_volume for bin in profile.bins]
    assert totals == [1.5, 4.0, 2.0]
    assert profile.value_area == (101.0, 102.0)
    assert pytest.approx(sum(totals), rel=1e-9) == 7.5
