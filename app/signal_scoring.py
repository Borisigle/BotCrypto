from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Sequence, Tuple


@dataclass(frozen=True)
class ScoreBreakdown:
    """Represents the weighted contributions that make up a signal score."""

    trend: float
    delta_oi: float
    cvd: float
    session: float
    extras: Dict[str, float] = field(default_factory=dict)

    @property
    def total(self) -> float:
        return round(self.trend + self.delta_oi + self.cvd + self.session, 4)


@dataclass(frozen=True)
class SignalContext:
    """Snapshot of indicator state used when evaluating a signal candidate."""

    symbol: str
    price: float
    timestamp: datetime
    ema_50: Optional[float]
    adx_14: Optional[float]
    vwap: Optional[float]
    delta_oi_pct: Optional[float]
    cvd: Optional[float]
    cvd_slope: Optional[float]
    session: Optional[str]
    value_area: Optional[Tuple[float, float]] = None
    lvns: Sequence[float] = field(default_factory=tuple)
    direction: Optional[str] = None


@dataclass(frozen=True)
class SignalScoringResult:
    """Computed signal details derived from a :class:`SignalContext`."""

    symbol: str
    timestamp: datetime
    direction: str
    breakdown: ScoreBreakdown
    tier: str
    entry_range: Tuple[float, float]
    stop_loss: float
    target: float
    metadata: Dict[str, float | None]

    @property
    def score(self) -> float:
        return self.breakdown.total


class SignalScoringEngine:
    """Scores signal opportunities on a bounded 0-7 scale.

    The engine applies weighted contributions based on:
    - broader trend alignment (EMA50 / ADX / VWAP / level confluence)
    - delta open interest magnitude
    - cumulative volume delta confirmation
    - session context
    """

    _TREND_WEIGHT = 3.0
    _DELTA_WEIGHT = 2.0
    _CVD_WEIGHT = 1.5
    _SESSION_WEIGHT = 0.5

    def __init__(
        self,
        *,
        cvd_reference: float = 2000.0,
        cvd_slope_reference: float = 50.0,
        delta_reference: float = 0.75,
    ) -> None:
        self._cvd_reference = max(cvd_reference, 1.0)
        self._cvd_slope_reference = max(cvd_slope_reference, 1.0)
        self._delta_reference = max(delta_reference, 0.05)

    def score(self, context: SignalContext) -> SignalScoringResult:
        direction = (context.direction or self._infer_direction(context)).lower()
        breakdown, extras = self._breakdown(context, direction)
        entry_range, stop_loss, target = self._risk_parameters(context, direction)
        tier = self._tier_for_score(breakdown.total)
        metadata = self._build_metadata(context, breakdown, direction, entry_range, stop_loss, target, extras)
        return SignalScoringResult(
            symbol=context.symbol,
            timestamp=context.timestamp,
            direction=direction,
            breakdown=breakdown,
            tier=tier,
            entry_range=entry_range,
            stop_loss=stop_loss,
            target=target,
            metadata=metadata,
        )

    def _infer_direction(self, context: SignalContext) -> str:
        delta = context.delta_oi_pct or 0.0
        if abs(delta) <= 1e-9 and context.ema_50 is not None:
            diff = context.price - context.ema_50
            if abs(diff) <= 1e-9:
                return "long"
            return "long" if diff >= 0 else "short"
        return "long" if delta >= 0 else "short"

    def _breakdown(self, context: SignalContext, direction: str) -> Tuple[ScoreBreakdown, Dict[str, float]]:
        extras: Dict[str, float] = {}
        trend_score, trend_details = self._trend_score(context, direction)
        extras.update(trend_details)
        delta_score, delta_details = self._delta_score(context)
        extras.update(delta_details)
        cvd_score, cvd_details = self._cvd_score(context, direction)
        extras.update(cvd_details)
        session_score, session_details = self._session_score(context.session)
        extras.update(session_details)

        breakdown = ScoreBreakdown(
            trend=trend_score,
            delta_oi=delta_score,
            cvd=cvd_score,
            session=session_score,
            extras=extras,
        )
        return breakdown, extras

    def _trend_score(self, context: SignalContext, direction: str) -> Tuple[float, Dict[str, float]]:
        price = context.price
        score = 0.0
        details: Dict[str, float] = {}

        # EMA alignment component
        ema_contrib = 0.0
        if context.ema_50 is not None and price > 0:
            diff = price - context.ema_50
            aligned = diff >= 0 if direction == "long" else diff <= 0
            diff_ratio = abs(diff) / price
            if aligned:
                ema_contrib = 1.1 + min(diff_ratio / 0.01, 1.0) * 0.4
            elif diff_ratio <= 0.0035:
                ema_contrib = 0.6 * (1.0 - diff_ratio / 0.0035)
            score += ema_contrib
        details["trend_ema_component"] = round(ema_contrib, 4)

        # ADX strength component
        adx_contrib = 0.0
        if context.adx_14 is not None:
            adx = max(context.adx_14, 0.0)
            if adx >= 40:
                adx_contrib = 1.0
            elif adx >= 35:
                adx_contrib = 0.9
            elif adx >= 30:
                adx_contrib = 0.8
            elif adx >= 25:
                adx_contrib = 0.65
            elif adx >= 20:
                adx_contrib = 0.45
            elif adx >= 15:
                adx_contrib = 0.25
            score += adx_contrib
        details["trend_adx_component"] = round(adx_contrib, 4)

        # VWAP alignment component
        vwap_contrib = 0.0
        if context.vwap is not None and price > 0:
            diff = price - context.vwap
            aligned = diff >= 0 if direction == "long" else diff <= 0
            diff_ratio = abs(diff) / price
            if aligned:
                vwap_contrib = 0.35 if diff_ratio <= 0.004 else 0.5
            elif diff_ratio <= 0.005:
                vwap_contrib = 0.2 * (1.0 - diff_ratio / 0.005)
            score += vwap_contrib
        details["trend_vwap_component"] = round(vwap_contrib, 4)

        # LVN / value area confluence
        confluence = 0.0
        va_low: Optional[float] = None
        va_high: Optional[float] = None
        if context.value_area is not None:
            va_low, va_high = sorted(context.value_area)
            if va_low <= price <= va_high:
                confluence += 0.25
            else:
                band_distance = min(abs(price - va_low), abs(price - va_high))
                if price > 0 and band_distance / price <= 0.004:
                    confluence += 0.15
        if context.lvns:
            nearest = min(abs(price - level) for level in context.lvns)
            if price > 0 and nearest / price <= 0.0025:
                confluence += 0.25
        score += confluence
        details["trend_confluence_bonus"] = round(confluence, 4)

        final_score = min(score, self._TREND_WEIGHT)
        details["trend_total_unclamped"] = round(score, 4)
        details["trend_total"] = round(final_score, 4)
        if score > self._TREND_WEIGHT:
            details["trend_clamped"] = round(score - self._TREND_WEIGHT, 4)
        return final_score, details

    def _delta_score(self, context: SignalContext) -> Tuple[float, Dict[str, float]]:
        delta = context.delta_oi_pct
        details: Dict[str, float] = {"delta_oi_pct": float(delta) if delta is not None else 0.0}
        if delta is None:
            return 0.0, details

        magnitude = abs(delta)
        details["delta_oi_abs"] = round(magnitude, 4)
        if magnitude >= 1.2:
            score = 2.0
        elif magnitude >= 1.0:
            score = 1.85
        elif magnitude >= 0.75:
            score = 1.6
        elif magnitude >= 0.5:
            score = 1.2
        elif magnitude >= 0.35:
            score = 0.8
        elif magnitude >= 0.2:
            score = 0.45
        else:
            baseline = min(self._delta_reference, 0.35)
            if baseline <= 0:
                score = 0.0
            else:
                score = min((magnitude / baseline) * 0.35, 0.35)
        score = min(score, self._DELTA_WEIGHT)
        details["delta_component"] = round(score, 4)
        return score, details

    def _cvd_score(self, context: SignalContext, direction: str) -> Tuple[float, Dict[str, float]]:
        cvd_value = context.cvd
        slope = context.cvd_slope
        details: Dict[str, float] = {
            "cvd_value": float(cvd_value) if cvd_value is not None else 0.0,
            "cvd_slope": float(slope) if slope is not None else 0.0,
        }
        if cvd_value is None and slope is None:
            return 0.0, details

        direction_multiplier = 1.0 if direction == "long" else -1.0
        aligned_value = (cvd_value or 0.0) * direction_multiplier
        aligned_slope = (slope or 0.0) * direction_multiplier

        value_points = 0.0
        if aligned_value > 0:
            ratio = min(aligned_value / self._cvd_reference, 1.0)
            value_points = 0.6 * ratio

        slope_points = 0.0
        if aligned_slope > 0:
            slope_points = min(aligned_slope / self._cvd_slope_reference, 1.0) * 0.9

        score = min(value_points + slope_points, self._CVD_WEIGHT)
        details["cvd_component"] = round(score, 4)
        details["cvd_value_points"] = round(value_points, 4)
        details["cvd_slope_points"] = round(slope_points, 4)
        return score, details

    def _session_score(self, session: Optional[str]) -> Tuple[float, Dict[str, float]]:
        mapping = {
            "new_york": 0.5,
            "new york": 0.5,
            "ny": 0.5,
            "london": 0.45,
            "europe": 0.45,
            "asia": 0.35,
            "asia_open": 0.35,
            "asia open": 0.35,
        }
        raw = mapping.get((session or "").lower(), 0.25 if session else 0.2)
        score = min(raw, self._SESSION_WEIGHT)
        return score, {"session_component": round(score, 4)}

    def _risk_parameters(
        self,
        context: SignalContext,
        direction: str,
    ) -> Tuple[Tuple[float, float], float, float]:
        price = context.price
        if price <= 0:
            return (price, price), price, price

        if context.value_area is not None:
            va_low, va_high = sorted(context.value_area)
        else:
            band = price * 0.004
            va_low, va_high = price - band, price + band

        if direction == "long":
            support_levels = [level for level in context.lvns if level <= price]
            anchor = max(support_levels) if support_levels else va_low
            entry_low = max(va_low, anchor)
            entry_high = min(va_high, price * 1.006)
            if entry_low > entry_high:
                entry_low, entry_high = entry_high, entry_low
            range_height = max(entry_high - entry_low, price * 0.0025)
            stop_loss = entry_low - max(range_height * 0.5, price * 0.003)
            target = entry_high + max(range_height, price * 0.004)
        else:
            resistance_levels = [level for level in context.lvns if level >= price]
            anchor = min(resistance_levels) if resistance_levels else va_high
            entry_high = min(va_high, anchor)
            entry_low = max(va_low, price * 0.994)
            if entry_low > entry_high:
                entry_low, entry_high = entry_high, entry_low
            range_height = max(entry_high - entry_low, price * 0.0025)
            stop_loss = entry_high + max(range_height * 0.5, price * 0.003)
            target = entry_low - max(range_height, price * 0.004)

        entry_range = (round(entry_low, 4), round(entry_high, 4))
        stop_loss = round(stop_loss, 4)
        target = round(target, 4)
        return entry_range, stop_loss, target

    def _tier_for_score(self, score: float) -> str:
        if score >= 5.5:
            return "high"
        if score >= 3.5:
            return "medium"
        return "low"

    def _build_metadata(
        self,
        context: SignalContext,
        breakdown: ScoreBreakdown,
        direction: str,
        entry_range: Tuple[float, float],
        stop_loss: float,
        target: float,
        extras: Dict[str, float],
    ) -> Dict[str, float | None]:
        metadata: Dict[str, float | None] = {
            "price": round(context.price, 4),
            "ema_50": round(context.ema_50, 4) if context.ema_50 is not None else None,
            "adx_14": round(context.adx_14, 4) if context.adx_14 is not None else None,
            "vwap": round(context.vwap, 4) if context.vwap is not None else None,
            "delta_oi_pct": round(context.delta_oi_pct, 4) if context.delta_oi_pct is not None else None,
            "cvd": round(context.cvd, 4) if context.cvd is not None else None,
            "cvd_slope": round(context.cvd_slope, 4) if context.cvd_slope is not None else None,
            "trend_score": round(breakdown.trend, 4),
            "delta_score": round(breakdown.delta_oi, 4),
            "cvd_score": round(breakdown.cvd, 4),
            "session_score": round(breakdown.session, 4),
            "score_total": round(breakdown.total, 4),
            "entry_low": entry_range[0],
            "entry_high": entry_range[1],
            "stop_loss": stop_loss,
            "target": target,
        }
        if context.value_area is not None:
            va_low, va_high = sorted(context.value_area)
            metadata["value_area_low"] = round(va_low, 4)
            metadata["value_area_high"] = round(va_high, 4)
        if context.lvns:
            metadata["nearest_lvn"] = round(min(context.lvns, key=lambda level: abs(level - context.price)), 4)
        metadata["direction"] = 1.0 if direction == "long" else -1.0
        for key, value in extras.items():
            if key in metadata:
                continue
            metadata[key] = round(value, 4)
        return metadata
