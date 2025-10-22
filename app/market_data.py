from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Optional

from .market_models import (
    MarketDataset,
    MarketSnapshot,
    SignalFeed,
    SignalFeedFilters,
    SignalFeedItem,
    SignalDebugReport,
)

DEFAULT_MARKET_DATA_PATH = Path(__file__).resolve().parent / "data" / "sample_market_data.json"


class MarketDataError(RuntimeError):
    """Raised when the market data snapshot cannot be loaded."""


class MarketDataRepository:
    """Loads market structure, indicator overlays, and signal feed metadata."""

    def __init__(self, data_path: Optional[Path] = None) -> None:
        self._data_path = data_path or DEFAULT_MARKET_DATA_PATH

    def _load_dataset(self) -> MarketDataset:
        if not self._data_path.exists():
            raise MarketDataError(f"Market data snapshot not found at {self._data_path}")

        try:
            with self._data_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive, difficult to trigger
            raise MarketDataError("Invalid market data JSON") from exc

        return MarketDataset.model_validate(payload)

    def market_snapshot(self, symbols: Optional[Iterable[str]] = None) -> MarketSnapshot:
        dataset = self._load_dataset()
        if symbols:
            requested = {symbol.upper() for symbol in symbols}
            markets = [market for market in dataset.markets if market.symbol.upper() in requested]
        else:
            markets = list(dataset.markets)

        return MarketSnapshot(generated_at=dataset.generated_at, markets=markets)

    def signal_feed(
        self,
        *,
        symbol: Optional[str] = None,
        confidence: Optional[str] = None,
        session: Optional[str] = None,
    ) -> SignalFeed:
        dataset = self._load_dataset()
        items: List[SignalFeedItem] = list(dataset.signals)

        if symbol:
            requested_symbol = symbol.upper()
            items = [item for item in items if item.symbol.upper() == requested_symbol]
        if confidence:
            requested_confidence = confidence.lower()
            items = [
                item
                for item in items
                if (item.confidence or "").lower() == requested_confidence
            ]
        if session:
            requested_session = session.lower()
            items = [
                item
                for item in items
                if (item.session or "").lower() == requested_session
            ]

        symbol_filter = sorted({item.symbol for item in dataset.signals})
        confidence_filter = sorted({item.confidence for item in dataset.signals if item.confidence})
        session_values = set(dataset.sessions)
        session_values.update(item.session for item in dataset.signals if item.session)
        session_filter = sorted(session for session in session_values if session)

        filters = SignalFeedFilters(
            symbols=symbol_filter,
            confidences=confidence_filter,
            sessions=session_filter,
        )

        return SignalFeed(generated_at=dataset.generated_at, signals=items, filters=filters)

    def stream_items(self) -> List[SignalFeedItem]:
        dataset = self._load_dataset()
        return list(dataset.signals)

    def signal_by_id(self, signal_id: int) -> SignalFeedItem:
        dataset = self._load_dataset()
        for item in dataset.signals:
            if item.id == signal_id:
                return item
        raise MarketDataError(f"Signal with id {signal_id} not found")

    def debug_signal(self, signal_id: int) -> SignalDebugReport:
        signal = self.signal_by_id(signal_id)
        # Compute naive contributions for debug/inspection purposes.
        conf = (signal.confidence or "").lower()
        confidence_weight = {"high": 1.0, "medium": 0.65, "low": 0.35}.get(conf, 0.5)
        delta_weight = signal.delta_oi_pct if signal.delta_oi_pct is not None else 0.0
        if delta_weight < 0:
            delta_weight = 0.0
        if delta_weight > 1.0:
            delta_weight = 1.0
        cvd_raw = signal.cvd if signal.cvd is not None else 0.0
        cvd_weight = cvd_raw / 2000.0
        if cvd_weight < 0:
            cvd_weight = 0.0
        if cvd_weight > 1.0:
            cvd_weight = 1.0

        contributions = {
            "confidence_weight": round(confidence_weight, 3),
            "delta_oi_weight": round(delta_weight, 3),
            "cvd_weight": round(cvd_weight, 3),
        }
        total_score = round(
            contributions["confidence_weight"] * 0.4
            + contributions["delta_oi_weight"] * 0.4
            + contributions["cvd_weight"] * 0.2,
            3,
        )
        return SignalDebugReport(
            signal_id=signal.id,
            symbol=signal.symbol,
            confidence=signal.confidence,
            session=signal.session,
            tier=signal.tier,
            contributions=contributions,
            total_score=total_score,
        )

    @property
    def data_path(self) -> Path:
        return self._data_path
