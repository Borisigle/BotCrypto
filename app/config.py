from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Thresholds:
    """Alerting thresholds for ingestion freshness and signal cadence."""

    max_ingestion_latency_seconds: float = 120.0
    max_ingestion_gap_seconds: float = 600.0
    min_signals_per_hour: int = 6
    min_win_rate: float = 0.35


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the monitoring service."""

    metrics_snapshot_path: Path
    alert_webhook_url: Optional[str]
    backtest_log_path: Path
    thresholds: Thresholds
    environment: str

    @property
    def snapshot_exists(self) -> bool:
        return self.metrics_snapshot_path.exists()


def _resolve_snapshot_path() -> Path:
    base = Path(os.getenv("METRICS_SNAPSHOT_PATH", "")).expanduser()
    if base:
        return base
    return Path(__file__).resolve().parent / "data" / "sample_metrics.json"


def _resolve_backtest_log_path() -> Path:
    base = Path(os.getenv("BACKTEST_LOG_PATH", "")).expanduser()
    if base:
        return base
    return Path(__file__).resolve().parent / "data" / "backtest_logs"


def _resolve_threshold(value: str, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(fallback)


def _resolve_int(value: str, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(fallback)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    thresholds = Thresholds(
        max_ingestion_latency_seconds=_resolve_threshold(
            os.getenv("MAX_INGESTION_LATENCY_SECONDS"), 120.0
        ),
        max_ingestion_gap_seconds=_resolve_threshold(
            os.getenv("MAX_INGESTION_GAP_SECONDS"), 600.0
        ),
        min_signals_per_hour=_resolve_int(
            os.getenv("MIN_SIGNALS_PER_HOUR"), 6
        ),
        min_win_rate=_resolve_threshold(
            os.getenv("MIN_WIN_RATE"), 0.35
        ),
    )

    return Settings(
        metrics_snapshot_path=_resolve_snapshot_path(),
        alert_webhook_url=os.getenv("ALERT_WEBHOOK_URL"),
        backtest_log_path=_resolve_backtest_log_path(),
        thresholds=thresholds,
        environment=os.getenv("APP_ENV", "development"),
    )
