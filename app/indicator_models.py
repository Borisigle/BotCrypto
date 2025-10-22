from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class IndicatorSeriesPoint(BaseModel):
    """Single time/value observation for an indicator series."""

    time: datetime = Field(..., description="UTC timestamp for the observation.")
    value: float = Field(..., description="Measured value for the indicator at the timestamp.")


class IndicatorResponseBase(BaseModel):
    """Common metadata returned for indicator endpoints."""

    symbol: str = Field(..., description="Instrument symbol (e.g. BTCUSDT).")
    timeframe: str = Field(..., description="Aggregation timeframe (e.g. 5m, 15m, 1h).")
    session: Optional[str] = Field(
        default=None,
        description="Trading session label when the series is session-specific (asia, london, new_york).",
    )
    generated_at: datetime = Field(..., description="Timestamp when the indicator series was produced.")


class CvdCurveResponse(IndicatorResponseBase):
    """Cumulative volume delta readings for a symbol/timeframe slice."""

    points: List[IndicatorSeriesPoint] = Field(
        default_factory=list,
        description="Ordered set of cumulative volume delta points.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "symbol": "BTCUSDT",
                "timeframe": "5m",
                "session": "new_york",
                "generated_at": "2099-10-22T13:00:00Z",
                "points": [
                    {"time": "2099-10-22T12:00:00Z", "value": 1215.0},
                    {"time": "2099-10-22T12:05:00Z", "value": 1284.0},
                    {"time": "2099-10-22T12:10:00Z", "value": 1320.0},
                ],
            }
        }
    )


class DeltaOiCurveResponse(IndicatorResponseBase):
    """Delta open interest percentage readings for a symbol/timeframe slice."""

    points: List[IndicatorSeriesPoint] = Field(
        default_factory=list,
        description="Ordered set of delta open interest percentage points.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "symbol": "BTCUSDT",
                "timeframe": "5m",
                "session": "new_york",
                "generated_at": "2099-10-22T13:00:00Z",
                "points": [
                    {"time": "2099-10-22T12:00:00Z", "value": 0.22},
                    {"time": "2099-10-22T12:05:00Z", "value": 0.27},
                    {"time": "2099-10-22T12:10:00Z", "value": 0.31},
                ],
            }
        }
    )


class VolumeProfileDistributionBin(BaseModel):
    """Volume histogram entry for a price bucket."""

    price: float = Field(..., description="Price level represented by the distribution bucket.")
    volume: float = Field(..., description="Executed or quoted volume at the price level.")


class VolumeProfileStatsResponse(IndicatorResponseBase):
    """Value area and profile metrics for a symbol/timeframe slice."""

    vah: float = Field(..., description="Value area high for the session/timeframe.")
    val: float = Field(..., description="Value area low for the session/timeframe.")
    poc: float = Field(..., description="Point of control price for the volume profile.")
    vwap: float = Field(..., description="Volume weighted average price observed across the window.")
    value_area_volume_pct: float = Field(
        ..., ge=0.0, le=1.0, description="Fraction of total volume captured inside the value area.")
    low_volume_nodes: List[float] = Field(
        default_factory=list,
        description="Identified low volume node price levels.",
    )
    high_volume_nodes: List[float] = Field(
        default_factory=list,
        description="Identified high volume node price levels.",
    )
    distribution: List[VolumeProfileDistributionBin] = Field(
        default_factory=list,
        description="Histogram of volume by price for the requested slice.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "symbol": "BTCUSDT",
                "timeframe": "5m",
                "session": "new_york",
                "generated_at": "2099-10-22T12:55:00Z",
                "vah": 27220.0,
                "val": 26920.0,
                "poc": 27080.0,
                "vwap": 27065.0,
                "value_area_volume_pct": 0.68,
                "low_volume_nodes": [26940.0, 27150.0],
                "high_volume_nodes": [27020.0, 27090.0],
                "distribution": [
                    {"price": 26920.0, "volume": 182.0},
                    {"price": 26980.0, "volume": 215.0},
                    {"price": 27040.0, "volume": 248.0},
                ],
            }
        }
    )


class IndicatorDataset(BaseModel):
    """Shape of the seeded indicator dataset used for tests and local development."""

    cvd: List[CvdCurveResponse] = Field(default_factory=list)
    delta_oi_pct: List[DeltaOiCurveResponse] = Field(default_factory=list)
    volume_profile: List[VolumeProfileStatsResponse] = Field(default_factory=list)
