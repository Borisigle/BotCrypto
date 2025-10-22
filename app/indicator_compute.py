from __future__ import annotations

"""Services for computing higher-level indicator aggregates from market data."""

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import fmean, pstdev
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from .indicator_models import (
    CvdCurveResponse,
    DeltaOiCurveResponse,
    IndicatorDataset,
    IndicatorSeriesPoint,
    VolumeProfileDistributionBin,
    VolumeProfileStatsResponse,
)
from .sessions import determine_session

__all__ = [
    "Trade",
    "OpenInterestSample",
    "Candle",
    "IndicatorInputs",
    "CvdCalculator",
    "DeltaOpenInterestCalculator",
    "VolumeProfileCalculator",
    "IndicatorDataStore",
    "IndicatorComputationWorker",
]


@dataclass(frozen=True)
class Trade:
    """Individual trade print required for cumulative volume delta."""

    symbol: str
    time: datetime
    side: str
    quantity: float

    def signed_quantity(self) -> float:
        """Return the signed quantity contribution for the trade."""

        quantity = float(self.quantity)
        side = (self.side or "").strip().lower()
        if side in {"buy", "bid", "buyer", "long"}:
            return abs(quantity)
        if side in {"sell", "ask", "seller", "short"}:
            return -abs(quantity)
        return quantity


@dataclass(frozen=True)
class OpenInterestSample:
    """Snapshot of open interest used for ΔOI% calculations."""

    symbol: str
    time: datetime
    open_interest: float


@dataclass(frozen=True)
class Candle:
    """OHLCV candle used for approximating volume profile statistics."""

    symbol: str
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class IndicatorInputs:
    """Container bundling the raw inputs needed to compute indicators for a symbol."""

    trades: Sequence[Trade] = ()
    open_interest: Sequence[OpenInterestSample] = ()
    candles: Sequence[Candle] = ()


@dataclass
class VolumeProfileComputation:
    """Intermediate representation of volume profile statistics for a session."""

    vah: float
    val: float
    poc: float
    vwap: float
    value_area_volume_pct: float
    low_volume_nodes: List[float]
    high_volume_nodes: List[float]
    distribution: List[VolumeProfileDistributionBin]
    generated_at: datetime


def _floor_to_timeframe(timestamp: datetime, minutes: int) -> datetime:
    minutes = max(int(minutes), 1)
    floored = timestamp.replace(second=0, microsecond=0)
    remainder = floored.minute % minutes
    if remainder:
        floored -= timedelta(minutes=remainder)
    return floored


def _normalise_timeframe(value: str | int) -> Tuple[str, int]:
    if isinstance(value, int):
        minutes = int(value)
        if minutes <= 0:
            raise ValueError("Timeframe minutes must be positive")
        return f"{minutes}m", minutes

    token = value.strip().lower()
    if token.endswith("m"):
        try:
            minutes = int(token[:-1])
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Invalid minute timeframe: {value}") from exc
    elif token.endswith("h"):
        try:
            minutes = int(token[:-1]) * 60
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Invalid hour timeframe: {value}") from exc
    else:
        raise ValueError(f"Unsupported timeframe format: {value}")

    if minutes <= 0:
        raise ValueError("Timeframe minutes must be positive")
    return token, minutes


class CvdCalculator:
    """Aggregates trade prints into cumulative volume delta curves."""

    def __init__(self, timeframe_minutes: int) -> None:
        if timeframe_minutes <= 0:
            raise ValueError("Timeframe minutes must be positive")
        self._timeframe_minutes = int(timeframe_minutes)

    def compute(self, trades: Sequence[Trade]) -> Dict[str, List[IndicatorSeriesPoint]]:
        buckets: Dict[str, MutableMapping[datetime, float]] = defaultdict(lambda: defaultdict(float))
        for trade in sorted(trades, key=lambda item: item.time):
            signed = trade.signed_quantity()
            if signed == 0.0:
                continue
            bucket_time = _floor_to_timeframe(trade.time, self._timeframe_minutes)
            session = determine_session(trade.time)
            buckets[session][bucket_time] += signed

        series: Dict[str, List[IndicatorSeriesPoint]] = {}
        for session, values in buckets.items():
            cumulative = 0.0
            points: List[IndicatorSeriesPoint] = []
            for timestamp in sorted(values):
                cumulative += values[timestamp]
                points.append(IndicatorSeriesPoint(time=timestamp, value=round(cumulative, 6)))
            if points:
                series[session] = points
        return series


def _find_base_sample(
    history: Sequence[OpenInterestSample], target_time: datetime
) -> Optional[OpenInterestSample]:
    for sample in reversed(history):
        if sample.time <= target_time:
            return sample
    return None


class DeltaOpenInterestCalculator:
    """Computes ΔOI% time series across rolling windows."""

    def __init__(self, windows: Sequence[Tuple[str, int]]) -> None:
        if not windows:
            raise ValueError("At least one window must be provided")
        seen: Dict[str, int] = {}
        ordered: List[Tuple[str, int]] = []
        for label, minutes in windows:
            if minutes <= 0:
                raise ValueError("Window minutes must be positive")
            if label not in seen:
                seen[label] = minutes
                ordered.append((label, minutes))
        self._windows = tuple(ordered)

    def compute(
        self,
        samples: Sequence[OpenInterestSample],
        *,
        normalise: bool = False,
    ) -> Dict[str, Dict[str, List[IndicatorSeriesPoint]]]:
        history_by_session: Dict[str, List[OpenInterestSample]] = defaultdict(list)
        results: Dict[str, Dict[str, List[IndicatorSeriesPoint]]] = {
            label: defaultdict(list) for label, _ in self._windows
        }
        value_tracker: Dict[str, Dict[str, List[float]]] = {
            label: defaultdict(list) for label, _ in self._windows
        }

        for sample in sorted(samples, key=lambda item: item.time):
            session = determine_session(sample.time)
            session_history = history_by_session[session]
            for label, minutes in self._windows:
                target_time = sample.time - timedelta(minutes=minutes)
                base = _find_base_sample(session_history, target_time)
                if base is None:
                    continue
                if base.open_interest == 0:
                    continue
                delta = (sample.open_interest - base.open_interest) / base.open_interest
                results[label][session].append(IndicatorSeriesPoint(time=sample.time, value=round(delta, 6)))
                value_tracker[label][session].append(delta)
            session_history.append(sample)

        if normalise:
            for label, session_map in results.items():
                for session, points in session_map.items():
                    raw_values = value_tracker[label][session]
                    if not points:
                        continue
                    if len(raw_values) < 2:
                        session_map[session] = [
                            IndicatorSeriesPoint(time=points[0].time, value=0.0)
                        ]
                        continue
                    mean = fmean(raw_values)
                    std = pstdev(raw_values)
                    if std == 0:
                        session_map[session] = [
                            IndicatorSeriesPoint(time=point.time, value=0.0)
                            for point in points
                        ]
                        continue
                    normalised = []
                    for point, value in zip(points, raw_values):
                        z_score = (value - mean) / std
                        normalised.append(IndicatorSeriesPoint(time=point.time, value=round(z_score, 6)))
                    session_map[session] = normalised

        return results


class VolumeProfileCalculator:
    """Approximates session-based volume profiles from candle data."""

    def __init__(self, value_area_fraction: float = 0.7) -> None:
        if not 0.0 < value_area_fraction <= 1.0:
            raise ValueError("Value area fraction must be within (0, 1]")
        self._value_area_fraction = value_area_fraction

    def compute(self, candles: Sequence[Candle]) -> Dict[str, VolumeProfileComputation]:
        grouped: Dict[str, List[Candle]] = defaultdict(list)
        for candle in sorted(candles, key=lambda item: item.time):
            session = determine_session(candle.time)
            grouped[session].append(candle)

        stats: Dict[str, VolumeProfileComputation] = {}
        for session, session_candles in grouped.items():
            stats[session] = self._compute_session(session_candles)
        return stats

    def _compute_session(self, candles: Sequence[Candle]) -> VolumeProfileComputation:
        price_volume: Dict[float, float] = defaultdict(float)
        total_volume = 0.0
        weighted_price = 0.0

        for candle in candles:
            volume = max(float(candle.volume), 0.0)
            price = float(round(candle.close, 2))
            price_volume[price] += volume
            total_volume += volume
            weighted_price += price * volume

        if not price_volume:
            last_price = float(round(candles[-1].close, 2))
            distribution = [VolumeProfileDistributionBin(price=last_price, volume=0.0)]
            return VolumeProfileComputation(
                vah=last_price,
                val=last_price,
                poc=last_price,
                vwap=last_price,
                value_area_volume_pct=0.0,
                low_volume_nodes=[],
                high_volume_nodes=[],
                distribution=distribution,
                generated_at=candles[-1].time,
            )

        sorted_prices = sorted(price_volume.items())
        distribution = [
            VolumeProfileDistributionBin(price=price, volume=round(volume, 6))
            for price, volume in sorted_prices
        ]
        volumes = [volume for _, volume in sorted_prices]
        max_volume = max(volumes)
        poc_candidates = [price for price, volume in sorted_prices if volume == max_volume]
        poc = min(poc_candidates)

        sorted_by_volume = sorted(price_volume.items(), key=lambda item: (item[1], item[0]), reverse=True)
        target_volume = total_volume * self._value_area_fraction
        accumulated = 0.0
        value_area_prices: List[float] = []
        for price, volume in sorted_by_volume:
            value_area_prices.append(price)
            accumulated += volume
            if total_volume == 0 or accumulated >= target_volume:
                break
        if not value_area_prices:
            value_area_prices.append(poc)
        vah = max(value_area_prices)
        val = min(value_area_prices)
        value_area_volume_pct = (accumulated / total_volume) if total_volume > 0 else 0.0

        low_volume_nodes: List[float] = []
        high_volume_nodes: List[float] = []
        for idx, (price, volume) in enumerate(sorted_prices):
            left = volumes[idx - 1] if idx > 0 else None
            right = volumes[idx + 1] if idx < len(volumes) - 1 else None
            if max_volume > 0:
                if volume <= max_volume * 0.35 and (left is None or volume < left) and (
                    right is None or volume < right
                ):
                    low_volume_nodes.append(price)
                if volume >= max_volume * 0.9:
                    high_volume_nodes.append(price)

        low_volume_nodes = list(dict.fromkeys(low_volume_nodes))
        high_volume_nodes = list(dict.fromkeys(high_volume_nodes))

        vwap = (weighted_price / total_volume) if total_volume > 0 else float(candles[-1].close)

        return VolumeProfileComputation(
            vah=round(vah, 6),
            val=round(val, 6),
            poc=round(float(poc), 6),
            vwap=round(vwap, 6),
            value_area_volume_pct=round(value_area_volume_pct, 6),
            low_volume_nodes=[round(value, 6) for value in low_volume_nodes],
            high_volume_nodes=[round(value, 6) for value in high_volume_nodes],
            distribution=distribution,
            generated_at=candles[-1].time,
        )


class IndicatorDataStore:
    """Persists computed indicator datasets to disk."""

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def persist(self, dataset: IndicatorDataset) -> None:
        payload = dataset.model_dump(mode="json")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)


class IndicatorComputationWorker:
    """Coordinates indicator calculations and persistence for multiple symbols."""

    def __init__(
        self,
        data: Mapping[str, IndicatorInputs],
        *,
        cvd_timeframes: Sequence[str | int] = ("5m", "15m"),
        delta_windows: Sequence[str | int] = ("30m", "60m"),
        include_delta_zscore: bool = False,
        volume_profile_timeframe: str = "session",
        value_area_fraction: float = 0.7,
        store: IndicatorDataStore | None = None,
    ) -> None:
        self._data = {symbol.upper(): inputs for symbol, inputs in data.items()}
        self._cvd_timeframes = tuple(_normalise_timeframe(tf) for tf in cvd_timeframes)
        self._delta_windows = tuple(_normalise_timeframe(window) for window in delta_windows)
        self._include_delta_zscore = include_delta_zscore
        self._volume_profile_timeframe = volume_profile_timeframe
        self._value_area_fraction = value_area_fraction
        self._store = store

    def compute_dataset(self) -> IndicatorDataset:
        cvd_entries: List[CvdCurveResponse] = []
        delta_entries: List[DeltaOiCurveResponse] = []
        profile_entries: List[VolumeProfileStatsResponse] = []

        for symbol, inputs in self._data.items():
            cvd_entries.extend(self._build_cvd(symbol, inputs.trades))
            delta_entries.extend(self._build_delta(symbol, inputs.open_interest))
            profile_entries.extend(self._build_volume_profiles(symbol, inputs.candles))

        return IndicatorDataset(
            cvd=cvd_entries,
            delta_oi_pct=delta_entries,
            volume_profile=profile_entries,
        )

    def run(self) -> IndicatorDataset:
        dataset = self.compute_dataset()
        if self._store is not None:
            self._store.persist(dataset)
        return dataset

    def _build_cvd(self, symbol: str, trades: Sequence[Trade]) -> List[CvdCurveResponse]:
        filtered = [trade for trade in trades if trade.symbol.upper() == symbol.upper()]
        if not filtered or not self._cvd_timeframes:
            return []

        responses: List[CvdCurveResponse] = []
        for timeframe_label, minutes in self._cvd_timeframes:
            calculator = CvdCalculator(minutes)
            series_by_session = calculator.compute(filtered)
            for session, points in series_by_session.items():
                if not points:
                    continue
                generated_at = points[-1].time
                responses.append(
                    CvdCurveResponse(
                        symbol=symbol,
                        timeframe=timeframe_label,
                        session=session,
                        generated_at=generated_at,
                        points=points,
                    )
                )
        return responses

    def _build_delta(
        self, symbol: str, samples: Sequence[OpenInterestSample]
    ) -> List[DeltaOiCurveResponse]:
        filtered = [sample for sample in samples if sample.symbol.upper() == symbol.upper()]
        if not filtered or not self._delta_windows:
            return []

        calculator = DeltaOpenInterestCalculator(self._delta_windows)
        raw_series = calculator.compute(filtered, normalise=False)
        responses: List[DeltaOiCurveResponse] = []
        for timeframe_label, session_points in raw_series.items():
            for session, points in session_points.items():
                if not points:
                    continue
                responses.append(
                    DeltaOiCurveResponse(
                        symbol=symbol,
                        timeframe=timeframe_label,
                        session=session,
                        generated_at=points[-1].time,
                        points=points,
                    )
                )

        if self._include_delta_zscore:
            normalised_series = calculator.compute(filtered, normalise=True)
            for timeframe_label, session_points in normalised_series.items():
                z_label = f"{timeframe_label}_z"
                for session, points in session_points.items():
                    if not points:
                        continue
                    responses.append(
                        DeltaOiCurveResponse(
                            symbol=symbol,
                            timeframe=z_label,
                            session=session,
                            generated_at=points[-1].time,
                            points=points,
                        )
                    )

        return responses

    def _build_volume_profiles(
        self, symbol: str, candles: Sequence[Candle]
    ) -> List[VolumeProfileStatsResponse]:
        filtered = [candle for candle in candles if candle.symbol.upper() == symbol.upper()]
        if not filtered:
            return []

        calculator = VolumeProfileCalculator(self._value_area_fraction)
        profiles = calculator.compute(filtered)
        responses: List[VolumeProfileStatsResponse] = []
        for session, stats in profiles.items():
            responses.append(
                VolumeProfileStatsResponse(
                    symbol=symbol,
                    timeframe=self._volume_profile_timeframe,
                    session=session,
                    generated_at=stats.generated_at,
                    vah=stats.vah,
                    val=stats.val,
                    poc=stats.poc,
                    vwap=stats.vwap,
                    value_area_volume_pct=stats.value_area_volume_pct,
                    low_volume_nodes=stats.low_volume_nodes,
                    high_volume_nodes=stats.high_volume_nodes,
                    distribution=stats.distribution,
                )
            )
        return responses
