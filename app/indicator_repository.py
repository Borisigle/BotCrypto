from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional, TypeVar

from .indicator_models import (
    CvdCurveResponse,
    DeltaOiCurveResponse,
    IndicatorDataset,
    IndicatorResponseBase,
    VolumeProfileStatsResponse,
)

DEFAULT_INDICATOR_DATA_PATH = Path(__file__).resolve().parent / "data" / "sample_indicator_data.json"

SeriesT = TypeVar("SeriesT", bound=IndicatorResponseBase)


class IndicatorRepositoryError(RuntimeError):
    """Raised when the indicator dataset cannot be loaded or parsed."""


class IndicatorSeriesNotFoundError(IndicatorRepositoryError):
    """Raised when no indicator slice matches the requested filters."""


class IndicatorRepository:
    """Loads pre-computed indicator series from Timescale-derived snapshots."""

    def __init__(self, data_path: Optional[Path] = None) -> None:
        self._data_path = data_path or DEFAULT_INDICATOR_DATA_PATH

    def _load_dataset(self) -> IndicatorDataset:
        if not self._data_path.exists():
            raise IndicatorRepositoryError(f"Indicator snapshot not found at {self._data_path}")

        try:
            with self._data_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive guard
            raise IndicatorRepositoryError("Indicator snapshot contains invalid JSON") from exc

        return IndicatorDataset.model_validate(payload)

    def _select_series(
        self,
        series: Iterable[SeriesT],
        *,
        symbol: str,
        timeframe: str,
        session: Optional[str],
    ) -> SeriesT:
        symbol_key = symbol.upper()
        timeframe_key = timeframe.lower()
        session_key = session.lower() if session else None

        matches = [
            entry
            for entry in series
            if entry.symbol.upper() == symbol_key and entry.timeframe.lower() == timeframe_key
        ]
        if not matches:
            raise IndicatorSeriesNotFoundError(
                f"No indicator series for symbol={symbol_key} timeframe={timeframe_key}"
            )

        if session_key is None:
            preferred = next((entry for entry in matches if entry.session is None), None)
            return preferred or matches[0]

        exact = next(
            (entry for entry in matches if entry.session and entry.session.lower() == session_key),
            None,
        )
        if exact:
            return exact

        fallback = next((entry for entry in matches if entry.session is None), None)
        if fallback:
            return fallback

        raise IndicatorSeriesNotFoundError(
            f"No indicator series for symbol={symbol_key} timeframe={timeframe_key} session={session_key}"
        )

    def cvd_curve(self, *, symbol: str, timeframe: str, session: Optional[str] = None) -> CvdCurveResponse:
        dataset = self._load_dataset()
        return self._select_series(dataset.cvd, symbol=symbol, timeframe=timeframe, session=session)

    def delta_oi_percent(
        self, *, symbol: str, timeframe: str, session: Optional[str] = None
    ) -> DeltaOiCurveResponse:
        dataset = self._load_dataset()
        return self._select_series(dataset.delta_oi_pct, symbol=symbol, timeframe=timeframe, session=session)

    def volume_profile(
        self, *, symbol: str, timeframe: str, session: Optional[str] = None
    ) -> VolumeProfileStatsResponse:
        dataset = self._load_dataset()
        return self._select_series(dataset.volume_profile, symbol=symbol, timeframe=timeframe, session=session)

    @property
    def data_path(self) -> Path:
        return self._data_path
