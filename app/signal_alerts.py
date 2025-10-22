from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple

import httpx

from .config import Settings
from .models import MetricsSnapshot, SignalConfidence, SignalEvent, SignalSetupType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SignalAlertConfig:
    enabled: bool = False
    include_medium_confidence: bool = False
    max_retries: int = 3
    request_timeout_seconds: float = 10.0
    web_base_url: Optional[str] = None


class TelegramBot:
    """Thin Telegram bot sender with basic retry and logging."""

    def __init__(
        self,
        *,
        bot_token: Optional[str],
        chat_id: Optional[str],
        timeout: float = 10.0,
        max_retries: int = 3,
    ) -> None:
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._timeout = timeout
        self._max_retries = max(1, max_retries)

    def send(self, message: str) -> bool:
        if not self._bot_token or not self._chat_id:
            logger.debug("Telegram bot token or chat id not configured; skipping send")
            return False

        url = f"https://api.telegram.org/bot{self._bot_token}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": message,
            # Keep plain text to avoid markup rendering issues in tests/integrations.
            # "parse_mode": "MarkdownV2",
        }

        last_error: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = httpx.post(url, json=payload, timeout=self._timeout)
                response.raise_for_status()
                logger.info("Delivered Telegram alert (attempt %s)", attempt)
                return True
            except httpx.HTTPError as exc:  # pragma: no cover - exercised via tests with monkeypatch
                last_error = exc
                logger.warning("Telegram delivery failed on attempt %s: %s", attempt, exc)
        logger.error("Telegram delivery failed after %s attempts: %s", self._max_retries, last_error)
        return False


class SignalAlertFormatter:
    def __init__(self, base_url: Optional[str] = None) -> None:
        self._base_url = base_url or "http://localhost:8080"

    def _infer_direction(self, signal: SignalEvent) -> str:
        setup = signal.setup
        if setup is None:
            return "long"
        if setup.type == SignalSetupType.SQUEEZE_REVERSAL:
            momentum = float(setup.metadata.get("momentum", 0.0)) if setup.metadata else 0.0
            return "long" if momentum >= 0 else "short"
        if setup.type == SignalSetupType.ABSORPTION:
            delta = float(setup.metadata.get("delta_volume", 0.0)) if setup.metadata else 0.0
            return "long" if delta >= 0 else "short"
        return "long"

    def _levels(self, signal: SignalEvent) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        setup = signal.setup
        if setup is None or setup.volume_profile is None or setup.volume_profile.value_area is None:
            return None, None, None

        lower, upper = setup.volume_profile.value_area
        direction = self._infer_direction(signal)
        # Simplistic heuristic: middle of value area as entry, opposite bound as stop, same-side bound as target.
        mid = (lower + upper) / 2.0
        if direction == "long":
            entry, stop, target = mid, lower, upper
        else:
            entry, stop, target = mid, upper, lower
        return entry, stop, target

    def _rationale(self, signal: SignalEvent) -> str:
        setup = signal.setup
        if setup is None:
            return "No setup metadata available"
        m = setup.metadata or {}
        if setup.type == SignalSetupType.SQUEEZE_REVERSAL:
            cr = m.get("compression_ratio")
            mom = m.get("momentum")
            fr = m.get("funding_rate")
            basis = m.get("basis")
            parts = []
            if cr is not None:
                parts.append(f"compression {float(cr):.3f}")
            if mom is not None:
                parts.append(f"momentum {float(mom):.2f}")
            if fr is not None:
                parts.append(f"funding {float(fr):.4f}")
            if basis is not None:
                parts.append(f"basis {float(basis):.4f}")
            return ", ".join(parts) if parts else "squeeze conditions met"
        if setup.type == SignalSetupType.ABSORPTION:
            imb = m.get("orderflow_imbalance")
            dv = m.get("delta_volume")
            basis = m.get("basis")
            parts = []
            if imb is not None:
                parts.append(f"imbalance {float(imb):.2f}")
            if dv is not None:
                parts.append(f"delta {float(dv):.2f}")
            if basis is not None:
                parts.append(f"basis {float(basis):.4f}")
            return ", ".join(parts) if parts else "absorption pattern detected"
        return "setup detected"

    def build_view_url(self, symbol: str) -> str:
        return f"{self._base_url.rstrip('/')}/dashboard?symbol={symbol}"

    def format(self, signal: SignalEvent) -> str:
        setup = signal.setup
        confidence = setup.confidence.value if setup is not None else "n/a"
        setup_name = setup.type.value.replace("_", " ").title() if setup is not None else "Unknown"
        direction = self._infer_direction(signal).upper()
        entry, stop, target = self._levels(signal)
        entry_str = f"{entry:.2f}" if entry is not None else "n/a"
        stop_str = f"{stop:.2f}" if stop is not None else "n/a"
        target_str = f"{target:.2f}" if target is not None else "n/a"
        score_str = f"{setup.score:.2f}" if setup is not None else "n/a"
        rationale = self._rationale(signal)
        url = self.build_view_url(signal.symbol)

        lines = [
            f"[Signal] {signal.symbol} {direction} • {setup_name} ({confidence})",
            f"Entry: {entry_str} | Stop: {stop_str} | Target: {target_str}",
            f"Score: {score_str}",
            f"Rationale: {rationale}",
            f"View: {url}",
        ]
        return "\n".join(lines)


class SignalAlertPipeline:
    """Processes snapshots and emits Telegram alerts for newly generated signals.

    New signals are detected using their generation timestamp. Confidence filters
    control which signals are eligible for alerting.
    """

    def __init__(self, settings: Settings, config: Optional[SignalAlertConfig] = None) -> None:
        cfg = config or SignalAlertConfig(
            enabled=settings.signal_alerts_enabled,
            include_medium_confidence=settings.signal_alerts_include_medium,
            max_retries=3,
            request_timeout_seconds=10.0,
            web_base_url=settings.web_base_url,
        )
        self._config = cfg
        self._bot = TelegramBot(
            bot_token=settings.telegram_bot_token,
            chat_id=settings.telegram_chat_id,
            timeout=cfg.request_timeout_seconds,
            max_retries=cfg.max_retries,
        )
        self._formatter = SignalAlertFormatter(base_url=cfg.web_base_url)
        self._last_seen_at: Optional[datetime] = None
        self._sent_signal_ids: set[int] = set()

    def _is_new(self, signal: SignalEvent) -> bool:
        if self._last_seen_at is None:
            return True
        # Ensure timezone-aware comparison
        sig_time = signal.generated_at
        if sig_time.tzinfo is None:
            sig_time = sig_time.replace(tzinfo=timezone.utc)
        return sig_time > self._last_seen_at

    def _eligible(self, signal: SignalEvent) -> bool:
        if signal.setup is None:
            return False
        if signal.setup.confidence == SignalConfidence.HIGH:
            return True
        if self._config.include_medium_confidence and signal.setup.confidence == SignalConfidence.MEDIUM:
            return True
        return False

    def process(self, snapshot: MetricsSnapshot) -> List[int]:
        if not self._config.enabled:
            logger.debug("Signal alert pipeline disabled; skipping processing")
            return []

        eligible: List[SignalEvent] = [
            s for s in snapshot.signals if self._eligible(s) and self._is_new(s) and s.id not in self._sent_signal_ids
        ]
        eligible.sort(key=lambda s: s.generated_at)

        delivered_ids: List[int] = []
        for signal in eligible:
            message = self._formatter.format(signal)
            try:
                delivered = self._bot.send(message)
            except Exception as exc:  # pragma: no cover - defensive guard
                delivered = False
                logger.debug("Unexpected error when sending Telegram alert", exc_info=True)
                logger.warning("Failed to deliver Telegram alert for signal %s: %s", signal.id, exc)

            if delivered:
                delivered_ids.append(signal.id)
                self._sent_signal_ids.add(signal.id)
                if self._last_seen_at is None or signal.generated_at > self._last_seen_at:
                    self._last_seen_at = signal.generated_at
            else:
                logger.warning("Telegram delivery reported failure for signal %s", signal.id)

        if eligible:
            latest_time = max(s.generated_at for s in eligible)
            if self._last_seen_at is None or latest_time > self._last_seen_at:
                self._last_seen_at = latest_time

        logger.info("Signal alert pipeline processed %s eligible, %s delivered", len(eligible), len(delivered_ids))
        return delivered_ids
