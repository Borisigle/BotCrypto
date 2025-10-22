from __future__ import annotations

import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, List, Optional, Set

import httpx

from .config import GovernanceRules
from .models import GovernanceAdjustment, GovernanceStatus, MetricsSnapshot, SignalEvent

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class TelegramNotifier:
    """Minimal Telegram notifier used for governance adjustments."""

    def __init__(self, bot_token: Optional[str], chat_id: Optional[str]) -> None:
        self._bot_token = bot_token
        self._chat_id = chat_id

    def send(self, message: str) -> None:
        if not self._bot_token or not self._chat_id:
            return

        url = f"https://api.telegram.org/bot{self._bot_token}/sendMessage"
        payload = {"chat_id": self._chat_id, "text": message}
        try:
            httpx.post(url, json=payload, timeout=10.0)
        except httpx.HTTPError as exc:
            logger.warning("Failed to send Telegram notification: %s", exc)


class SignalGovernance:
    """Evaluates signal cadence and applies adaptive governance policies."""

    def __init__(self, rules: GovernanceRules, notifier: Optional[TelegramNotifier] = None) -> None:
        self._rules = rules
        self._notifier = notifier or TelegramNotifier(None, None)
        self._current_delta_oi = rules.delta_oi_baseline
        self._medium_tier_allowed = False
        self._adjustments: Deque[GovernanceAdjustment] = deque(maxlen=rules.adjustment_history_size)

    def evaluate(self, snapshot: MetricsSnapshot, now: Optional[datetime] = None) -> GovernanceStatus:
        now = now or _utc_now()
        now = _as_utc(now)

        primary_tiers: Set[str] = {tier.lower() for tier in self._rules.primary_signal_tiers}
        if not primary_tiers:
            primary_tiers = {"high"}

        def tier_of(signal: SignalEvent) -> str:
            tier = signal.tier or "high"
            return tier.lower()

        high_signals = [signal for signal in snapshot.signals if tier_of(signal) in primary_tiers]

        rolling_counts: Dict[str, int] = {}
        low_activity_windows: List[int] = []
        for window in self._rules.rolling_windows_hours:
            window_start = now - timedelta(hours=window)
            count = sum(1 for signal in high_signals if _as_utc(signal.generated_at) >= window_start)
            rolling_counts[str(window)] = count
            if count < self._rules.minimum_primary_signals_per_window:
                low_activity_windows.append(window)

        if high_signals:
            latest_signal = max(high_signals, key=lambda item: _as_utc(item.generated_at))
            drought_hours_value = max(
                (now - _as_utc(latest_signal.generated_at)).total_seconds() / 3600.0,
                0.0,
            )
        else:
            drought_hours_value = None

        drought_metric = drought_hours_value if drought_hours_value is not None else float("inf")
        drought_active = drought_metric >= self._rules.drought_hours_trigger

        cap_limit = self._rules.medium_tier_daily_cap
        has_cap = cap_limit > 0
        medium_usage_today = sum(
            1
            for signal in snapshot.signals
            if tier_of(signal) == "medium" and _as_utc(signal.generated_at).date() == now.date()
        )
        cap_exhausted = not has_cap or medium_usage_today >= cap_limit
        medium_remaining = max(cap_limit - medium_usage_today, 0) if has_cap else 0

        target_threshold = self._rules.delta_oi_relaxed if drought_active else self._rules.delta_oi_baseline
        medium_tier_allowed = drought_active and has_cap and not cap_exhausted

        reason: Optional[str] = None
        if abs(target_threshold - self._current_delta_oi) > 1e-9:
            reason = "drought_relaxed" if drought_active else "reset_baseline"
        elif medium_tier_allowed != self._medium_tier_allowed:
            if medium_tier_allowed:
                reason = "medium_tier_enabled"
            elif cap_exhausted:
                reason = "medium_tier_cap_exhausted"
            else:
                reason = "medium_tier_disabled"

        if reason is not None:
            self._record_adjustment(
                now=now,
                reason=reason,
                new_threshold=target_threshold,
                medium_allowed=medium_tier_allowed,
                medium_remaining=medium_remaining,
                medium_usage_today=medium_usage_today,
            )
        else:
            # Ensure state reflects the evaluated posture even when no change is recorded.
            self._current_delta_oi = target_threshold
            self._medium_tier_allowed = medium_tier_allowed

        status = GovernanceStatus(
            generated_at=now,
            trigger_hours=self._rules.drought_hours_trigger,
            drought_active=drought_active,
            drought_hours=drought_hours_value,
            delta_oi_threshold=self._current_delta_oi,
            delta_oi_baseline=self._rules.delta_oi_baseline,
            delta_oi_relaxed=self._rules.delta_oi_relaxed,
            medium_tier_allowed=self._medium_tier_allowed,
            cap_exhausted=cap_exhausted,
            medium_tier_daily_usage=medium_usage_today,
            medium_tier_daily_cap=cap_limit,
            medium_tier_remaining=medium_remaining,
            rolling_counts=rolling_counts,
            low_activity_windows=low_activity_windows,
            primary_signal_tiers=list(self._rules.primary_signal_tiers),
            adjustments=list(self._adjustments),
        )
        return status

    def _record_adjustment(
        self,
        *,
        now: datetime,
        reason: str,
        new_threshold: float,
        medium_allowed: bool,
        medium_remaining: int,
        medium_usage_today: int,
    ) -> None:
        previous_threshold = self._current_delta_oi

        adjustment = GovernanceAdjustment(
            timestamp=now,
            reason=reason,
            previous_delta_oi_threshold=previous_threshold,
            new_delta_oi_threshold=new_threshold,
            medium_tier_allowed=medium_allowed,
            medium_tier_daily_usage=medium_usage_today,
            medium_tier_daily_cap=self._rules.medium_tier_daily_cap,
            medium_tier_remaining=medium_remaining,
        )
        self._adjustments.appendleft(adjustment)

        self._current_delta_oi = new_threshold
        self._medium_tier_allowed = medium_allowed

        logger.info(
            "Signal governance adjustment (%s): ΔOI %.3f -> %.3f, medium-tier allowed=%s, usage=%s/%s",
            reason,
            previous_threshold,
            new_threshold,
            str(medium_allowed).lower(),
            medium_usage_today,
            self._rules.medium_tier_daily_cap,
        )

        message = (
            f"[Signal Governance] {reason.replace('_', ' ').title()} @ {now.isoformat()}\n"
            f"ΔOI threshold: {previous_threshold:.3f} → {new_threshold:.3f}\n"
            f"Medium-tier allowed: {'yes' if medium_allowed else 'no'} "
            f"({medium_usage_today}/{self._rules.medium_tier_daily_cap} used)"
        )
        try:
            self._notifier.send(message)
        except Exception:  # pragma: no cover - defensive guard
            logger.debug("Telegram notifier raised an unexpected error", exc_info=True)
