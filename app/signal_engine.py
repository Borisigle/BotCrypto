from __future__ import annotations

from dataclasses import dataclass
from math import floor
from typing import Dict, Iterable, List, Literal, Optional, Tuple

from .models import (
    SignalConfidence,
    SignalSetup,
    SignalSetupType,
    VolumeProfile,
    VolumeProfileBin,
)


@dataclass
class Trade:
    """Lightweight representation of a trade used for volume aggregation."""

    price: float
    quantity: float
    side: Literal["buy", "sell"]

    def __post_init__(self) -> None:  # pragma: no cover - simple validation
        if self.price <= 0:
            raise ValueError("Trade price must be positive")
        if self.quantity <= 0:
            raise ValueError("Trade quantity must be positive")
        if self.side not in ("buy", "sell"):
            raise ValueError("Trade side must be 'buy' or 'sell'")


@dataclass(frozen=True)
class SignalEngineConfig:
    """Configuration tuning thresholds for setup detection and profiling."""

    volume_profile_bin_size: float = 5.0
    value_area_volume_fraction: float = 0.7
    squeeze_bb_width_max: float = 0.012
    squeeze_volatility_ratio_max: float = 0.85
    squeeze_momentum_trigger: float = 0.25
    squeeze_momentum_shift_trigger: float = 0.2
    squeeze_negative_funding_threshold: float = -0.0015
    squeeze_basis_min: float = 0.0008
    absorption_imbalance_threshold: float = 1.5
    absorption_delta_volume_threshold: float = 1.1
    absorption_basis_tolerance: float = 0.0015
    absorption_liquidation_cluster_threshold: float = 0.5


class SignalEngine:
    """Classifies strategy setups and refines scoring based on market context."""

    def __init__(self, config: Optional[SignalEngineConfig] = None) -> None:
        self._config = config or SignalEngineConfig()

    def evaluate(
        self,
        *,
        indicators: Dict[str, float],
        funding_rate: float,
        spot_price: float,
        perp_price: float,
        trades: Iterable[Trade],
    ) -> Optional[SignalSetup]:
        """Evaluate the dominant setup produced by the current market state."""

        volume_profile = self._build_volume_profile(trades)
        basis = self._basis(spot_price, perp_price)

        candidates: List[SignalSetup] = []

        squeeze = self._evaluate_squeeze_reversal(
            indicators=indicators,
            funding_rate=funding_rate,
            basis=basis,
            volume_profile=volume_profile,
        )
        if squeeze is not None:
            candidates.append(squeeze)

        absorption = self._evaluate_absorption(
            indicators=indicators,
            basis=basis,
            volume_profile=volume_profile,
        )
        if absorption is not None:
            candidates.append(absorption)

        if not candidates:
            return None

        return max(candidates, key=lambda setup: setup.score)

    def build_volume_profile(self, trades: Iterable[Trade]) -> VolumeProfile:
        """Expose volume profile calculation for external callers/tests."""

        return self._build_volume_profile(trades)

    def _build_volume_profile(self, trades: Iterable[Trade]) -> VolumeProfile:
        trade_list = list(trades)
        if not trade_list:
            return VolumeProfile(bins=[], value_area=None)

        bin_size = self._config.volume_profile_bin_size
        if bin_size <= 0:
            raise ValueError("volume_profile_bin_size must be positive")

        buckets: Dict[float, Dict[str, float]] = {}
        for trade in trade_list:
            bucket_price = floor(trade.price / bin_size) * bin_size
            bucket = buckets.setdefault(bucket_price, {"buy": 0.0, "sell": 0.0})
            bucket[trade.side] += trade.quantity

        bins = [
            VolumeProfileBin(
                price=price,
                buy_volume=data["buy"],
                sell_volume=data["sell"],
                total_volume=data["buy"] + data["sell"],
            )
            for price, data in sorted(buckets.items())
        ]

        value_area = self._value_area_range(bins)
        return VolumeProfile(bins=bins, value_area=value_area)

    def _value_area_range(self, bins: List[VolumeProfileBin]) -> Optional[Tuple[float, float]]:
        if not bins:
            return None

        total_volume = sum(bin.total_volume for bin in bins)
        if total_volume <= 0:
            return None

        target = total_volume * self._config.value_area_volume_fraction
        cumulative = 0.0
        selected: List[float] = []
        for item in sorted(bins, key=lambda candidate: candidate.total_volume, reverse=True):
            cumulative += item.total_volume
            selected.append(item.price)
            if cumulative >= target:
                break

        if not selected:
            return None

        return (min(selected), max(selected))

    def _evaluate_squeeze_reversal(
        self,
        *,
        indicators: Dict[str, float],
        funding_rate: float,
        basis: float,
        volume_profile: VolumeProfile,
    ) -> Optional[SignalSetup]:
        bb_width = indicators.get("bb_width")
        kc_width = indicators.get("kc_width")
        if bb_width is None or kc_width is None or kc_width <= 0:
            return None

        compression_ratio = bb_width / kc_width
        if bb_width > self._config.squeeze_bb_width_max:
            return None
        if compression_ratio > self._config.squeeze_volatility_ratio_max:
            return None
        if basis < self._config.squeeze_basis_min:
            return None

        momentum = indicators.get("momentum", 0.0)
        momentum_shift = indicators.get("momentum_shift", 0.0)

        score = 0.55
        if funding_rate <= self._config.squeeze_negative_funding_threshold:
            score += 0.1
        if abs(momentum) >= self._config.squeeze_momentum_trigger:
            score += 0.1
        if momentum_shift >= self._config.squeeze_momentum_shift_trigger:
            score += 0.15
        if compression_ratio <= self._config.squeeze_volatility_ratio_max / 2:
            score += 0.05

        score = min(score, 1.0)
        confidence = self._confidence_for_score(score)

        metadata = {
            "bb_width": bb_width,
            "kc_width": kc_width,
            "compression_ratio": compression_ratio,
            "momentum": momentum,
            "momentum_shift": momentum_shift,
            "funding_rate": funding_rate,
            "basis": basis,
        }

        return SignalSetup(
            type=SignalSetupType.SQUEEZE_REVERSAL,
            confidence=confidence,
            score=round(score, 3),
            metadata=metadata,
            volume_profile=volume_profile,
        )

    def _evaluate_absorption(
        self,
        *,
        indicators: Dict[str, float],
        basis: float,
        volume_profile: VolumeProfile,
    ) -> Optional[SignalSetup]:
        imbalance = indicators.get("orderflow_imbalance")
        delta_volume = indicators.get("delta_volume")
        if imbalance is None or delta_volume is None:
            return None

        if imbalance < self._config.absorption_imbalance_threshold:
            return None
        if delta_volume < self._config.absorption_delta_volume_threshold:
            return None
        if abs(basis) > self._config.absorption_basis_tolerance:
            return None

        liquidation_cluster = indicators.get("liquidation_cluster", 0.0)

        score = 0.58
        imbalance_boost = max(0.0, (imbalance / self._config.absorption_imbalance_threshold) - 1.0)
        imbalance_boost = min(imbalance_boost, 1.5)
        delta_boost = max(0.0, (delta_volume / self._config.absorption_delta_volume_threshold) - 1.0)
        delta_boost = min(delta_boost, 1.5)

        score += imbalance_boost * 0.12
        score += delta_boost * 0.12
        if liquidation_cluster >= self._config.absorption_liquidation_cluster_threshold:
            score += 0.1

        score = min(score, 1.0)
        confidence = self._confidence_for_score(score)

        metadata = {
            "orderflow_imbalance": imbalance,
            "delta_volume": delta_volume,
            "basis": basis,
            "liquidation_cluster": liquidation_cluster,
        }

        return SignalSetup(
            type=SignalSetupType.ABSORPTION,
            confidence=confidence,
            score=round(score, 3),
            metadata=metadata,
            volume_profile=volume_profile,
        )

    @staticmethod
    def _basis(spot_price: float, perp_price: float) -> float:
        if spot_price <= 0:
            return 0.0
        return (perp_price - spot_price) / spot_price

    @staticmethod
    def _confidence_for_score(score: float) -> SignalConfidence:
        if score >= 0.8:
            return SignalConfidence.HIGH
        if score >= 0.65:
            return SignalConfidence.MEDIUM
        return SignalConfidence.LOW
