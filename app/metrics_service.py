from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Dict, Iterable, List, Optional, Tuple

from .config import Settings
from .data_source import FileMetricsRepository
from .models import (
    AggregatedMetrics,
    HealthResponse,
    HealthStatus,
    IngestionSourceMetric,
    IngestionSummary,
    MetricsSnapshot,
    PerformanceSummary,
    SignalSummary,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MetricsService:
    """Aggregates ingestion and performance data into monitoring friendly structures."""

    def __init__(self, repository: FileMetricsRepository, settings: Settings) -> None:
        self._repository = repository
        self._settings = settings

    def _summarise_ingestion(self, snapshot: MetricsSnapshot, now: datetime) -> IngestionSummary:
        events = snapshot.ingestions
        if not events:
            return IngestionSummary(
                latest_source=None,
                latest_timestamp=None,
                current_latency_seconds=None,
                average_latency_seconds=None,
                max_latency_seconds=None,
                sources=[],
                status=HealthStatus.CRITICAL,
                time_since_last_event_seconds=None,
            )

        latest_event = max(events, key=lambda event: event.received_at)
        average_latency = mean(event.latency_seconds for event in events)
        max_latency = max(event.latency_seconds for event in events)
        time_since_last = max((now - latest_event.received_at).total_seconds(), 0.0)

        per_source: Dict[str, IngestionSourceMetric] = {}
        for event in events:
            current = per_source.get(event.source)
            if current is None or event.received_at > current.latest_timestamp:
                per_source[event.source] = IngestionSourceMetric(
                    source=event.source,
                    latest_timestamp=event.received_at,
                    latency_seconds=event.latency_seconds,
                )

        status = self._evaluate_ingestion_status(
            current_latency=latest_event.latency_seconds,
            time_since_last=time_since_last,
            max_latency=max_latency,
        )

        return IngestionSummary(
            latest_source=latest_event.source,
            latest_timestamp=latest_event.received_at,
            current_latency_seconds=latest_event.latency_seconds,
            average_latency_seconds=average_latency,
            max_latency_seconds=max_latency,
            sources=sorted(per_source.values(), key=lambda item: item.source),
            status=status,
            time_since_last_event_seconds=time_since_last,
        )

    def _evaluate_ingestion_status(
        self,
        current_latency: float,
        time_since_last: float,
        max_latency: float,
    ) -> HealthStatus:
        thresholds = self._settings.thresholds
        warning = (
            current_latency > thresholds.max_ingestion_latency_seconds
            or time_since_last > thresholds.max_ingestion_gap_seconds
        )
        critical = (
            current_latency > thresholds.max_ingestion_latency_seconds * 2
            or time_since_last > thresholds.max_ingestion_gap_seconds * 2
        )
        if critical:
            return HealthStatus.CRITICAL
        if warning:
            return HealthStatus.WARNING
        return HealthStatus.OK

    def _summarise_signals(self, snapshot: MetricsSnapshot, now: datetime) -> SignalSummary:
        signals = snapshot.signals
        if not signals:
            return SignalSummary(
                total=0,
                by_status={},
                last_60_minutes=0,
                last_24_hours=0,
                cadence_seconds_avg=None,
                status=HealthStatus.CRITICAL,
            )

        status_counts = Counter(signal.status for signal in signals)
        last_60 = sum(1 for signal in signals if now - signal.generated_at <= timedelta(hours=1))
        last_24 = sum(1 for signal in signals if now - signal.generated_at <= timedelta(hours=24))

        cadence_values: List[float] = [
            signal.cadence_seconds for signal in signals if signal.cadence_seconds is not None
        ]
        if len(signals) > 1 and not cadence_values:
            # derive cadence from inter-arrival times if cadence_seconds missing
            ordered = sorted(signals, key=lambda signal: signal.generated_at)
            diffs = [
                (later.generated_at - earlier.generated_at).total_seconds()
                for earlier, later in zip(ordered[:-1], ordered[1:])
                if later.generated_at > earlier.generated_at
            ]
            cadence_values = diffs
        cadence_avg = mean(cadence_values) if cadence_values else None

        status = self._evaluate_signal_status(last_60, cadence_avg)

        return SignalSummary(
            total=len(signals),
            by_status=dict(status_counts),
            last_60_minutes=last_60,
            last_24_hours=last_24,
            cadence_seconds_avg=cadence_avg,
            status=status,
        )

    def _evaluate_signal_status(
        self,
        last_hour_count: int,
        cadence_avg: Optional[float],
    ) -> HealthStatus:
        thresholds = self._settings.thresholds
        if last_hour_count == 0:
            return HealthStatus.CRITICAL
        if last_hour_count < thresholds.min_signals_per_hour:
            return HealthStatus.WARNING
        if cadence_avg is not None and cadence_avg > thresholds.max_ingestion_gap_seconds:
            return HealthStatus.WARNING
        return HealthStatus.OK

    def _summarise_performance(self, snapshot: MetricsSnapshot) -> PerformanceSummary:
        executions = snapshot.executions
        if not executions:
            return PerformanceSummary(
                wins=0,
                losses=0,
                win_rate=0.0,
                avg_return_pct=0.0,
                status=HealthStatus.WARNING,
            )

        wins = sum(1 for exec_event in executions if exec_event.outcome.lower() == "win")
        losses = sum(1 for exec_event in executions if exec_event.outcome.lower() == "loss")
        total_tracked = wins + losses
        win_rate = wins / total_tracked if total_tracked else 0.0
        avg_return = mean(exec_event.return_pct for exec_event in executions)

        thresholds = self._settings.thresholds
        if total_tracked == 0:
            status = HealthStatus.WARNING
        elif win_rate < thresholds.min_win_rate:
            status = HealthStatus.WARNING
        else:
            status = HealthStatus.OK

        return PerformanceSummary(
            wins=wins,
            losses=losses,
            win_rate=win_rate,
            avg_return_pct=avg_return,
            status=status,
        )

    def collect(self) -> AggregatedMetrics:
        now = _utc_now()
        snapshot = self._repository.fetch_snapshot()
        ingestion = self._summarise_ingestion(snapshot, now=now)
        signals = self._summarise_signals(snapshot, now=now)
        performance = self._summarise_performance(snapshot)

        return AggregatedMetrics(
            generated_at=now,
            ingestion=ingestion,
            signals=signals,
            performance=performance,
        )

    def health(self) -> HealthResponse:
        metrics = self.collect()
        summary = {
            "ingestion": metrics.ingestion.status,
            "signals": metrics.signals.status,
            "performance": metrics.performance.status,
        }
        return HealthResponse(generated_at=metrics.generated_at, summary=summary, details=metrics)
