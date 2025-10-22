from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.indicator_compute import (
    Candle,
    CvdCalculator,
    DeltaOpenInterestCalculator,
    IndicatorComputationWorker,
    IndicatorDataStore,
    IndicatorInputs,
    OpenInterestSample,
    Trade,
    VolumeProfileCalculator,
)
from app.indicator_repository import IndicatorRepository


def _dt(hour: int, minute: int) -> datetime:
    return datetime(2099, 10, 22, hour, minute, tzinfo=timezone.utc)


def test_cvd_calculator_resets_per_session() -> None:
    trades = [
        Trade(symbol="BTCUSDT", time=_dt(0, 1), side="buy", quantity=10.0),
        Trade(symbol="BTCUSDT", time=_dt(0, 3), side="sell", quantity=4.0),
        Trade(symbol="BTCUSDT", time=_dt(0, 7), side="buy", quantity=3.0),
        Trade(symbol="BTCUSDT", time=_dt(8, 1), side="buy", quantity=5.0),
    ]

    calculator = CvdCalculator(timeframe_minutes=5)
    series = calculator.compute(trades)

    assert set(series.keys()) == {"asia", "london"}

    asia_points = series["asia"]
    assert [point.time for point in asia_points] == [_dt(0, 0), _dt(0, 5)]
    assert [point.value for point in asia_points] == [pytest.approx(6.0), pytest.approx(9.0)]

    london_points = series["london"]
    assert [point.time for point in london_points] == [_dt(8, 0)]
    assert [point.value for point in london_points] == [pytest.approx(5.0)]


def test_delta_open_interest_calculator_windows_and_zscore() -> None:
    start = _dt(0, 0)
    samples = [
        OpenInterestSample(symbol="BTCUSDT", time=start, open_interest=100.0),
        OpenInterestSample(symbol="BTCUSDT", time=start + timedelta(minutes=15), open_interest=105.0),
        OpenInterestSample(symbol="BTCUSDT", time=start + timedelta(minutes=30), open_interest=110.0),
        OpenInterestSample(symbol="BTCUSDT", time=start + timedelta(minutes=45), open_interest=112.0),
        OpenInterestSample(symbol="BTCUSDT", time=start + timedelta(minutes=60), open_interest=120.0),
    ]

    calculator = DeltaOpenInterestCalculator([("30m", 30), ("60m", 60)])
    raw = calculator.compute(samples)

    assert set(raw.keys()) == {"30m", "60m"}

    thirty_series = raw["30m"]["asia"]
    assert len(thirty_series) == 3
    assert thirty_series[0].time == start + timedelta(minutes=30)
    assert thirty_series[0].value == pytest.approx(0.1, rel=1e-6)
    assert thirty_series[1].value == pytest.approx((112.0 - 105.0) / 105.0, rel=1e-6)
    assert thirty_series[2].value == pytest.approx((120.0 - 110.0) / 110.0, rel=1e-6)

    sixty_series = raw["60m"]["asia"]
    assert len(sixty_series) == 1
    assert sixty_series[0].time == start + timedelta(minutes=60)
    assert sixty_series[0].value == pytest.approx((120.0 - 100.0) / 100.0, rel=1e-6)

    z_scores = calculator.compute(samples, normalise=True)
    z_thirty = z_scores["30m"]["asia"]
    assert len(z_thirty) == 3
    assert pytest.approx(0.0, abs=1e-9) == sum(point.value for point in z_thirty)  # mean zero
    assert z_scores["60m"]["asia"][0].value == pytest.approx(0.0)


def test_volume_profile_calculator_value_area_and_nodes() -> None:
    start = _dt(0, 0)
    prices = [100.0, 101.0, 102.0, 103.0, 104.0]
    volumes = [10.0, 15.0, 25.0, 5.0, 12.0]
    candles = [
        Candle(
            symbol="BTCUSDT",
            time=start + timedelta(minutes=idx),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=volume,
        )
        for idx, (price, volume) in enumerate(zip(prices, volumes))
    ]

    calculator = VolumeProfileCalculator(value_area_fraction=0.7)
    profiles = calculator.compute(candles)

    assert set(profiles.keys()) == {"asia"}
    stats = profiles["asia"]
    assert stats.poc == pytest.approx(102.0)
    assert stats.val == pytest.approx(101.0)
    assert stats.vah == pytest.approx(104.0)
    assert stats.value_area_volume_pct == pytest.approx((25.0 + 15.0 + 12.0) / sum(volumes))
    assert stats.vwap == pytest.approx(6828.0 / sum(volumes))
    assert stats.low_volume_nodes == [103.0]
    assert stats.high_volume_nodes == [102.0]
    assert [bin.price for bin in stats.distribution] == prices


def test_indicator_worker_persists_dataset(tmp_path: Path) -> None:
    start = _dt(0, 0)
    trades = [
        Trade(symbol="BTCUSDT", time=start + timedelta(minutes=idx * 2), side="buy", quantity=5.0)
        for idx in range(3)
    ]
    trades.append(Trade(symbol="BTCUSDT", time=start + timedelta(minutes=8), side="sell", quantity=2.0))

    open_interest = [
        OpenInterestSample(symbol="BTCUSDT", time=start + timedelta(minutes=idx * 15), open_interest=value)
        for idx, value in enumerate([100.0, 104.0, 109.0, 118.0])
    ]

    candles = [
        Candle(
            symbol="BTCUSDT",
            time=start + timedelta(minutes=idx),
            open=100.0 + idx,
            high=100.0 + idx,
            low=100.0 + idx,
            close=100.0 + idx,
            volume=volume,
        )
        for idx, volume in enumerate([12.0, 18.0, 20.0, 7.0])
    ]

    inputs = {"BTCUSDT": IndicatorInputs(trades=trades, open_interest=open_interest, candles=candles)}
    path = tmp_path / "indicator_dataset.json"
    store = IndicatorDataStore(path)
    worker = IndicatorComputationWorker(
        inputs,
        cvd_timeframes=("5m",),
        delta_windows=("30m",),
        include_delta_zscore=True,
        volume_profile_timeframe="1m",
        store=store,
    )

    dataset = worker.run()

    assert path.exists()
    payload = json.loads(path.read_text())
    assert payload["cvd"]
    assert payload["delta_oi_pct"]
    assert payload["volume_profile"]

    delta_timeframes = {entry["timeframe"] for entry in payload["delta_oi_pct"]}
    assert "30m" in delta_timeframes
    assert "30m_z" in delta_timeframes

    repo = IndicatorRepository(path)
    cvd_curve = repo.cvd_curve(symbol="BTCUSDT", timeframe="5m", session="asia")
    assert cvd_curve.points

    delta_curve = repo.delta_oi_percent(symbol="BTCUSDT", timeframe="30m", session="asia")
    assert delta_curve.points

    profile = repo.volume_profile(symbol="BTCUSDT", timeframe="1m", session="asia")
    assert profile.poc > 0
