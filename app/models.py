from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class HealthStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"


class IngestionEvent(BaseModel):
    source: str
    received_at: datetime
    latency_seconds: float = Field(ge=0)


class SignalEvent(BaseModel):
    id: int
    symbol: str
    status: str
    generated_at: datetime
    cadence_seconds: Optional[float] = None
    tier: str = Field(
        default="high",
        description="Signal strength tier classification (e.g. high, medium, low).",
    )
    outcome: Optional[str] = None
    return_pct: Optional[float] = Field(default=None, description="Realised return as fraction (0.05 = 5%)")


class ExecutionEvent(BaseModel):
    signal_id: int
    closed_at: datetime
    outcome: str
    return_pct: float


class MetricsSnapshot(BaseModel):
    ingestions: List[IngestionEvent]
    signals: List[SignalEvent]
    executions: List[ExecutionEvent]


class IngestionSourceMetric(BaseModel):
    source: str
    latest_timestamp: datetime
    latency_seconds: float


class IngestionSummary(BaseModel):
    latest_source: Optional[str]
    latest_timestamp: Optional[datetime]
    current_latency_seconds: Optional[float]
    average_latency_seconds: Optional[float]
    max_latency_seconds: Optional[float]
    sources: List[IngestionSourceMetric]
    status: HealthStatus
    time_since_last_event_seconds: Optional[float]


class SignalSummary(BaseModel):
    total: int
    by_status: Dict[str, int]
    last_60_minutes: int
    last_24_hours: int
    cadence_seconds_avg: Optional[float]
    status: HealthStatus


class PerformanceSummary(BaseModel):
    wins: int
    losses: int
    win_rate: float
    avg_return_pct: float
    status: HealthStatus


class AggregatedMetrics(BaseModel):
    generated_at: datetime
    ingestion: IngestionSummary
    signals: SignalSummary
    performance: PerformanceSummary


class AlertEvaluation(BaseModel):
    generated_at: datetime
    triggered: bool
    reasons: List[str]


class AlertDispatchResult(AlertEvaluation):
    delivered: bool
    destination: Optional[str]


class HealthResponse(BaseModel):
    generated_at: datetime
    summary: Dict[str, HealthStatus]
    details: AggregatedMetrics


class GovernanceAdjustment(BaseModel):
    timestamp: datetime
    reason: str
    previous_delta_oi_threshold: float
    new_delta_oi_threshold: float
    medium_tier_allowed: bool
    medium_tier_daily_usage: int
    medium_tier_daily_cap: int
    medium_tier_remaining: int


class GovernanceStatus(BaseModel):
    generated_at: datetime
    trigger_hours: float
    drought_active: bool
    drought_hours: Optional[float]
    delta_oi_threshold: float
    delta_oi_baseline: float
    delta_oi_relaxed: float
    medium_tier_allowed: bool
    cap_exhausted: bool
    medium_tier_daily_usage: int
    medium_tier_daily_cap: int
    medium_tier_remaining: int
    rolling_counts: Dict[str, int]
    low_activity_windows: List[int]
    primary_signal_tiers: List[str]
    adjustments: List[GovernanceAdjustment]


class BacktestOverrides(BaseModel):
    win_return_threshold: Optional[float] = Field(
        default=None,
        description=(
            "Minimum fractional return required to classify a trade as a win. "
            "When omitted, the stored outcome flag is used."
        ),
    )
    loss_return_threshold: Optional[float] = Field(
        default=None,
        description=(
            "Maximum fractional return tolerated before classifying a trade as a loss. "
            "When omitted, the stored outcome flag is used."
        ),
    )
    min_trade_count: Optional[int] = Field(
        default=None,
        ge=0,
        description="Minimum number of trades required for a window to be considered a sufficient sample.",
    )
    min_win_rate: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Target win rate expectation used when evaluating each window.",
    )


class BacktestParameters(BaseModel):
    windows: List[int]
    win_return_threshold: Optional[float]
    loss_return_threshold: Optional[float]
    min_trade_count: int
    min_win_rate: float


class BacktestWindowResult(BaseModel):
    window_days: int
    start: datetime
    end: datetime
    first_trade_at: Optional[datetime]
    last_trade_at: Optional[datetime]
    trade_count: int
    wins: int
    losses: int
    unclassified: int
    hit_rate: float
    expectancy: float
    average_return: float
    cumulative_return: float
    max_drawdown: float
    meets_win_rate_threshold: bool
    sufficient_sample: bool


class BacktestSummary(BaseModel):
    window_days: int
    trade_count: int
    wins: int
    losses: int
    hit_rate: float
    expectancy: float
    cumulative_return: float
    max_drawdown: float
    meets_win_rate_threshold: bool
    sufficient_sample: bool


class BacktestReport(BaseModel):
    generated_at: datetime
    parameters: BacktestParameters
    windows: List[BacktestWindowResult]
    summary: BacktestSummary
