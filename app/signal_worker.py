from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from .market_models import MarketDataset, MarketInstrument, PricePoint, SeriesPoint, VolumeLevels
from .models import SignalConfidence, SignalEvent, SignalSetup, SignalSetupType, VolumeProfile
from .sessions import determine_session
from .signal_scoring import SignalContext, SignalScoringEngine, SignalScoringResult
from .signal_storage import InMemorySignalStorage, SignalRecord, SignalStorage


def _ema(values: Sequence[float], period: int) -> Optional[float]:
    if not values:
        return None
    period = max(period, 1)
    seed = min(len(values), period)
    if seed == 0:
        return None
    seed_avg = sum(values[:seed]) / seed
    multiplier = 2.0 / (period + 1)
    ema = seed_avg
    for value in values[seed:]:
        ema = (value - ema) * multiplier + ema
    return ema


def _adx(values: Sequence[float], period: int = 14) -> Optional[float]:
    if len(values) < 2:
        return None
    changes = [values[idx] - values[idx - 1] for idx in range(1, len(values))]
    up_moves = [max(change, 0.0) for change in changes]
    down_moves = [max(-change, 0.0) for change in changes]
    tr = [abs(change) for change in changes]

    avg_up = _ema(up_moves, period) or 0.0
    avg_down = _ema(down_moves, period) or 0.0
    avg_tr = _ema(tr, period) or 0.0
    if avg_tr <= 0:
        return 0.0
    plus_di = (avg_up / avg_tr) * 100.0
    minus_di = (avg_down / avg_tr) * 100.0
    denominator = max(plus_di + minus_di, 1e-6)
    adx = abs(plus_di - minus_di) / denominator * 100.0
    return max(0.0, min(adx, 100.0))


def _vwap(price_points: Sequence[PricePoint]) -> Optional[float]:
    if not price_points:
        return None
    total_price = sum(point.close for point in price_points)
    return total_price / len(price_points)


def _series_slope(series: Sequence[SeriesPoint]) -> Optional[float]:
    if len(series) < 2:
        return None
    return series[-1].value - series[-2].value


def _value_area(levels: Optional[VolumeLevels]) -> Optional[Tuple[float, float]]:
    if levels is None:
        return None
    if levels.val is None or levels.vah is None:
        return None
    low = float(min(levels.val, levels.vah))
    high = float(max(levels.val, levels.vah))
    return low, high


@dataclass
class WorkerConfig:
    min_score: float = 3.5
    require_value_area: bool = False


class SignalScoringWorker:
    """Periodic worker that evaluates market data and emits scored signals."""

    def __init__(
        self,
        *,
        scoring_engine: Optional[SignalScoringEngine] = None,
        storage: Optional[SignalStorage] = None,
        config: Optional[WorkerConfig] = None,
    ) -> None:
        self._engine = scoring_engine or SignalScoringEngine()
        self._storage = storage or InMemorySignalStorage()
        self._config = config or WorkerConfig()
        self._last_by_symbol: Dict[str, datetime] = {}

    @property
    def storage(self) -> SignalStorage:
        return self._storage

    @property
    def engine(self) -> SignalScoringEngine:
        return self._engine

    def run(self, dataset: MarketDataset) -> List[SignalEvent]:
        generated: List[SignalEvent] = []
        for instrument in dataset.markets:
            context = self.build_context(instrument)
            if context is None:
                continue
            if self._config.require_value_area and context.value_area is None:
                continue
            result = self._engine.score(context)
            if result.score < self._config.min_score:
                continue
            event = self._build_event(result, instrument)
            record = SignalRecord(event=event, metadata=result.metadata, created_at=result.timestamp)
            self._storage.store(record)
            generated.append(event)
            symbol_key = instrument.symbol.upper()
            self._last_by_symbol[symbol_key] = event.generated_at
        return generated

    def build_context(self, instrument: MarketInstrument) -> Optional[SignalContext]:
        if not instrument.price:
            return None
        price_points = sorted(instrument.price, key=lambda point: point.time)
        price_values = [point.close for point in price_points]
        timestamp = price_points[-1].time
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        delta_series = sorted(instrument.delta_oi_pct, key=lambda point: point.time) if instrument.delta_oi_pct else []
        cvd_series = sorted(instrument.cvd, key=lambda point: point.time) if instrument.cvd else []

        ema50 = _ema(price_values, 50)
        adx14 = _adx(price_values, 14)
        vwap = _vwap(price_points)
        delta_oi = delta_series[-1].value if delta_series else None
        cvd_value = cvd_series[-1].value if cvd_series else None
        cvd_slope = _series_slope(cvd_series)
        session = determine_session(timestamp)
        value_area = _value_area(instrument.volume_levels)
        lvns = (
            tuple(level for level in instrument.volume_levels.lvns if level is not None)
            if instrument.volume_levels and instrument.volume_levels.lvns
            else tuple()
        )
        direction = "long"
        if delta_oi is not None and delta_oi < 0:
            direction = "short"
        elif ema50 is not None and price_points[-1].close < ema50:
            direction = "short"

        return SignalContext(
            symbol=instrument.symbol,
            price=price_points[-1].close,
            timestamp=timestamp,
            ema_50=ema50,
            adx_14=adx14,
            vwap=vwap,
            delta_oi_pct=delta_oi,
            cvd=cvd_value,
            cvd_slope=cvd_slope,
            session=session,
            value_area=value_area,
            lvns=lvns,
            direction=direction,
        )

    def _build_event(self, result: SignalScoringResult, instrument: MarketInstrument) -> SignalEvent:
        symbol = instrument.symbol
        symbol_key = symbol.upper()
        previous_time = self._last_by_symbol.get(symbol_key)
        cadence: Optional[float] = None
        if previous_time is not None:
            cadence = max((result.timestamp - previous_time).total_seconds(), 0.0)

        setup_metadata = {
            key: float(value)
            for key, value in result.metadata.items()
            if isinstance(value, (int, float)) and value is not None
        }
        value_area = result.metadata.get("value_area_low"), result.metadata.get("value_area_high")
        if value_area[0] is not None and value_area[1] is not None:
            volume_profile = VolumeProfile(bins=[], value_area=(float(value_area[0]), float(value_area[1])))
        else:
            volume_profile = None

        setup = SignalSetup(
            type=SignalSetupType.MOMENTUM_CONTINUATION,
            confidence=self._confidence_for_tier(result.tier),
            score=round(result.score, 4),
            metadata=setup_metadata,
            volume_profile=volume_profile,
        )

        event = SignalEvent(
            id=self._storage.next_id(),
            symbol=symbol,
            generated_at=result.timestamp,
            cadence_seconds=cadence,
            tier=result.tier,
            setup=setup,
        )
        return event

    @staticmethod
    def _confidence_for_tier(tier: str) -> SignalConfidence:
        tier_lower = tier.lower()
        if tier_lower == "high":
            return SignalConfidence.HIGH
        if tier_lower == "medium":
            return SignalConfidence.MEDIUM
        return SignalConfidence.LOW
