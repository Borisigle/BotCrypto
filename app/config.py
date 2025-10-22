from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional, Tuple


@dataclass(frozen=True)
class Thresholds:
    """Alerting thresholds for ingestion freshness and signal cadence."""

    max_ingestion_latency_seconds: float = 120.0
    max_ingestion_gap_seconds: float = 600.0
    min_signals_per_hour: int = 6
    min_win_rate: float = 0.35


@dataclass(frozen=True)
class GovernanceRules:
    """Governance settings controlling signal cadence adjustments."""

    drought_hours_trigger: float = 36.0
    rolling_windows_hours: Tuple[int, ...] = (6, 12, 24)
    minimum_primary_signals_per_window: int = 1
    delta_oi_baseline: float = 0.65
    delta_oi_relaxed: float = 0.5
    medium_tier_daily_cap: int = 4
    adjustment_history_size: int = 10
    primary_signal_tiers: Tuple[str, ...] = ("high",)


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the monitoring service."""

    metrics_snapshot_path: Path
    indicator_snapshot_path: Path
    alert_webhook_url: Optional[str]
    backtest_log_path: Path
    thresholds: Thresholds
    governance_rules: GovernanceRules
    environment: str
    telegram_bot_token: Optional[str]
    telegram_chat_id: Optional[str]
    # Telegram signal alerting configuration
    signal_alerts_enabled: bool = False
    signal_alerts_include_medium: bool = False
    web_base_url: Optional[str] = None
    redis_url: Optional[str] = None
    indicator_cache_ttl_seconds: int = 30
    timescale_dsn: Optional[str] = None

    @property
    def snapshot_exists(self) -> bool:
        return self.metrics_snapshot_path.exists()

    @property
    def indicator_snapshot_exists(self) -> bool:
        return self.indicator_snapshot_path.exists()


def _resolve_snapshot_path() -> Path:
    base = Path(os.getenv("METRICS_SNAPSHOT_PATH", "")).expanduser()
    if base:
        return base
    return Path(__file__).resolve().parent / "data" / "sample_metrics.json"


def _resolve_indicator_snapshot_path() -> Path:
    base = Path(os.getenv("INDICATOR_SNAPSHOT_PATH", "")).expanduser()
    if base:
        return base
    return Path(__file__).resolve().parent / "data" / "sample_indicator_data.json"


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


def _resolve_bool(value: Optional[str], fallback: bool) -> bool:
    if value is None:
        return fallback
    val = value.strip().lower()
    if val in {"1", "true", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "no", "n", "off"}:
        return False
    return fallback


def _resolve_int_sequence(value: Optional[str], fallback: Tuple[int, ...]) -> Tuple[int, ...]:
    if not value:
        return fallback
    try:
        parts = [int(part.strip()) for part in value.split(",") if part.strip()]
    except ValueError:
        return fallback
    return tuple(parts) if parts else fallback


def _resolve_str_sequence(value: Optional[str], fallback: Tuple[str, ...]) -> Tuple[str, ...]:
    if not value:
        return fallback
    parts = [part.strip() for part in value.split(",") if part.strip()]
    return tuple(parts) if parts else fallback


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

    governance_defaults = GovernanceRules()
    governance = GovernanceRules(
        drought_hours_trigger=_resolve_threshold(
            os.getenv("GOVERNANCE_DROUGHT_HOURS_TRIGGER"),
            governance_defaults.drought_hours_trigger,
        ),
        rolling_windows_hours=_resolve_int_sequence(
            os.getenv("GOVERNANCE_ROLLING_WINDOWS_HOURS"),
            governance_defaults.rolling_windows_hours,
        ),
        minimum_primary_signals_per_window=max(
            0,
            _resolve_int(
                os.getenv("GOVERNANCE_MIN_PRIMARY_SIGNALS"),
                governance_defaults.minimum_primary_signals_per_window,
            ),
        ),
        delta_oi_baseline=_resolve_threshold(
            os.getenv("GOVERNANCE_DELTA_OI_BASELINE"),
            governance_defaults.delta_oi_baseline,
        ),
        delta_oi_relaxed=_resolve_threshold(
            os.getenv("GOVERNANCE_DELTA_OI_RELAXED"),
            governance_defaults.delta_oi_relaxed,
        ),
        medium_tier_daily_cap=max(
            0,
            _resolve_int(
                os.getenv("GOVERNANCE_MEDIUM_TIER_DAILY_CAP"),
                governance_defaults.medium_tier_daily_cap,
            ),
        ),
        adjustment_history_size=max(
            1,
            _resolve_int(
                os.getenv("GOVERNANCE_ADJUSTMENT_HISTORY_SIZE"),
                governance_defaults.adjustment_history_size,
            ),
        ),
        primary_signal_tiers=_resolve_str_sequence(
            os.getenv("GOVERNANCE_PRIMARY_SIGNAL_TIERS"),
            governance_defaults.primary_signal_tiers,
        ),
    )

    return Settings(
        metrics_snapshot_path=_resolve_snapshot_path(),
        indicator_snapshot_path=_resolve_indicator_snapshot_path(),
        alert_webhook_url=os.getenv("ALERT_WEBHOOK_URL"),
        backtest_log_path=_resolve_backtest_log_path(),
        thresholds=thresholds,
        governance_rules=governance,
        environment=os.getenv("APP_ENV", "development"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
        signal_alerts_enabled=_resolve_bool(os.getenv("SIGNAL_ALERTS_ENABLED"), False),
        signal_alerts_include_medium=_resolve_bool(os.getenv("SIGNAL_ALERTS_INCLUDE_MEDIUM"), False),
        web_base_url=os.getenv("WEB_BASE_URL"),
        redis_url=os.getenv("REDIS_URL"),
        indicator_cache_ttl_seconds=max(
            1,
            _resolve_int(os.getenv("INDICATOR_CACHE_TTL_SECONDS"), 30),
        ),
        timescale_dsn=os.getenv("TIMESCALE_DSN"),
    )
