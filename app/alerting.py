from __future__ import annotations

import logging
from typing import Dict, Optional

import httpx

from .config import Settings
from .models import AggregatedMetrics, AlertDispatchResult, AlertEvaluation, HealthStatus

logger = logging.getLogger(__name__)


class AlertManager:
    """Evaluates aggregated metrics and emits webhook alerts when thresholds are breached."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, metrics: AggregatedMetrics) -> AlertEvaluation:
        thresholds = self._settings.thresholds
        reasons = []

        ingestion = metrics.ingestion
        if ingestion.status == HealthStatus.CRITICAL:
            reasons.append("Ingestion feed is stalled or critically latent")
        elif ingestion.status == HealthStatus.WARNING:
            reasons.append("Ingestion latency exceeds configured threshold")

        if (
            ingestion.time_since_last_event_seconds is not None
            and ingestion.time_since_last_event_seconds > thresholds.max_ingestion_gap_seconds
        ):
            reasons.append(
                f"No ingestion events for {ingestion.time_since_last_event_seconds:.0f}s"
            )

        if metrics.signals.status == HealthStatus.CRITICAL:
            reasons.append("No trading signals produced in the last hour")
        elif metrics.signals.status == HealthStatus.WARNING:
            reasons.append("Signal cadence below configured expectations")

        if metrics.performance.status == HealthStatus.WARNING and metrics.performance.wins + metrics.performance.losses >= 5:
            reasons.append(
                f"Win rate {metrics.performance.win_rate:.2%} below threshold {thresholds.min_win_rate:.0%}"
            )

        triggered = bool(reasons)
        return AlertEvaluation(
            generated_at=metrics.generated_at,
            triggered=triggered,
            reasons=reasons,
        )

    def dispatch(self, metrics: AggregatedMetrics) -> AlertDispatchResult:
        evaluation = self.evaluate(metrics)
        webhook = self._settings.alert_webhook_url

        if not evaluation.triggered or not webhook:
            return AlertDispatchResult(
                generated_at=evaluation.generated_at,
                triggered=evaluation.triggered,
                reasons=evaluation.reasons,
                delivered=False,
                destination=webhook,
            )

        payload = {
            "timestamp": metrics.generated_at.isoformat(),
            "summary": {
                "ingestion": metrics.ingestion.status,
                "signals": metrics.signals.status,
                "performance": metrics.performance.status,
            },
            "reasons": evaluation.reasons,
        }

        try:
            response = httpx.post(webhook, json=payload, timeout=10.0)
            response.raise_for_status()
            delivered = True
        except httpx.HTTPError as exc:
            logger.warning("Failed to dispatch alert webhook: %s", exc)
            delivered = False

        return AlertDispatchResult(
            generated_at=evaluation.generated_at,
            triggered=evaluation.triggered,
            reasons=evaluation.reasons,
            delivered=delivered,
            destination=webhook,
        )
