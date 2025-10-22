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
