from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class PricePoint(BaseModel):
    time: datetime
    close: float


class SeriesPoint(BaseModel):
    time: datetime
    value: float


class VolumeLevels(BaseModel):
    vah: float
    val: float
    poc: float
    lvns: List[float] = Field(default_factory=list)


class EntryZone(BaseModel):
    signal_id: int
    start: datetime
    end: datetime
    low: float
    high: float
    tier: Optional[str] = None
    label: Optional[str] = None


class MarketInstrument(BaseModel):
    symbol: str
    price: List[PricePoint]
    cvd: List[SeriesPoint]
    delta_oi_pct: List[SeriesPoint]
    volume_levels: VolumeLevels
    entry_zones: List[EntryZone] = Field(default_factory=list)


class MarketPoint(BaseModel):
    price: PricePoint
    cvd: SeriesPoint
    delta_oi_pct: SeriesPoint


class SignalFeedItem(BaseModel):
    id: int
    symbol: str
    generated_at: datetime
    confidence: Optional[str] = None
    session: Optional[str] = None
    tier: Optional[str] = None
    status: Optional[str] = None
    entry_price: Optional[float] = None
    delta_oi_pct: Optional[float] = Field(
        default=None,
        description="Delta open interest percentage at the time the signal was generated.",
    )
    cvd: Optional[float] = Field(
        default=None,
        description="Cumulative volume delta at the time the signal was generated.",
    )
    notes: Optional[str] = None
    market_point: Optional[MarketPoint] = None
    entry_zone: Optional[EntryZone] = None


class SignalFeedFilters(BaseModel):
    symbols: List[str] = Field(default_factory=list)
    confidences: List[str] = Field(default_factory=list)
    sessions: List[str] = Field(default_factory=list)


class MarketSnapshot(BaseModel):
    generated_at: datetime
    markets: List[MarketInstrument]


class SignalFeed(BaseModel):
    generated_at: datetime
    signals: List[SignalFeedItem]
    filters: SignalFeedFilters


class MarketDataset(BaseModel):
    generated_at: datetime
    markets: List[MarketInstrument]
    signals: List[SignalFeedItem]
    sessions: List[str] = Field(default_factory=list)
