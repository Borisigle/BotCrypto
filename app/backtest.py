from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from statistics import mean
from typing import Iterable, List, Optional, Sequence

from .config import Settings
from .data_source import FileMetricsRepository
from .models import (
    BacktestOverrides,
    BacktestParameters,
    BacktestReport,
    BacktestSummary,
    BacktestWindowResult,
    ExecutionEvent,
)

_LOGGER = logging.getLogger(__name__)

DEFAULT_WINDOWS: Sequence[int] = (30, 60, 90)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class _ResolvedBacktestParameters:
    windows: List[int]
    win_return_threshold: Optional[float]
    loss_return_threshold: Optional[float]
    min_trade_count: int
    min_win_rate: float


class BacktestRunner:
    """Runs performance backtests over historical execution data."""

    def __init__(self, repository: FileMetricsRepository, settings: Settings) -> None:
        self._repository = repository
        self._settings = settings
        self._log_dir: Path = settings.backtest_log_path

    def run(
        self,
        windows: Sequence[int] | None = None,
        overrides: BacktestOverrides | None = None,
        now: Optional[datetime] = None,
    ) -> BacktestReport:
        resolved = self._resolve_parameters(windows, overrides)
        snapshot = self._repository.fetch_snapshot()
        executions = sorted(snapshot.executions, key=lambda event: event.closed_at)

        timestamp = now or _utc_now()
        if executions:
            latest_closed_at = executions[-1].closed_at
            if latest_closed_at > timestamp:
                timestamp = latest_closed_at

        window_results: List[BacktestWindowResult] = []
        for window_days in resolved.windows:
            start = timestamp - timedelta(days=window_days)
            window_trades = [
                trade for trade in executions if start <= trade.closed_at <= timestamp
            ]
            window_results.append(
                self._compute_window(window_days, start, timestamp, window_trades, resolved)
            )

        summary = self._summarise(window_results)
        parameters = BacktestParameters(
            windows=resolved.windows,
            win_return_threshold=resolved.win_return_threshold,
            loss_return_threshold=resolved.loss_return_threshold,
            min_trade_count=resolved.min_trade_count,
            min_win_rate=resolved.min_win_rate,
        )

        report = BacktestReport(
            generated_at=timestamp,
            parameters=parameters,
            windows=window_results,
            summary=summary,
        )

        self._persist_report(report)
        return report

    def to_csv(self, report: BacktestReport) -> str:
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "window_days",
                "start",
                "end",
                "first_trade_at",
                "last_trade_at",
                "trade_count",
                "wins",
                "losses",
                "unclassified",
                "hit_rate",
                "expectancy",
                "average_return",
                "cumulative_return",
                "max_drawdown",
                "meets_win_rate_threshold",
                "sufficient_sample",
            ]
        )

        for window in report.windows:
            writer.writerow(
                [
                    window.window_days,
                    window.start.isoformat(),
                    window.end.isoformat(),
                    window.first_trade_at.isoformat() if window.first_trade_at else "",
                    window.last_trade_at.isoformat() if window.last_trade_at else "",
                    window.trade_count,
                    window.wins,
                    window.losses,
                    window.unclassified,
                    f"{window.hit_rate:.6f}",
                    f"{window.expectancy:.6f}",
                    f"{window.average_return:.6f}",
                    f"{window.cumulative_return:.6f}",
                    f"{window.max_drawdown:.6f}",
                    window.meets_win_rate_threshold,
                    window.sufficient_sample,
                ]
            )

        writer.writerow([])
        writer.writerow(
            [
                "summary_window_days",
                "trade_count",
                "wins",
                "losses",
                "hit_rate",
                "expectancy",
                "cumulative_return",
                "max_drawdown",
                "meets_win_rate_threshold",
                "sufficient_sample",
            ]
        )

        summary = report.summary
        writer.writerow(
            [
                summary.window_days,
                summary.trade_count,
                summary.wins,
                summary.losses,
                f"{summary.hit_rate:.6f}",
                f"{summary.expectancy:.6f}",
                f"{summary.cumulative_return:.6f}",
                f"{summary.max_drawdown:.6f}",
                summary.meets_win_rate_threshold,
                summary.sufficient_sample,
            ]
        )

        return buffer.getvalue()

    def _resolve_parameters(
        self,
        windows: Sequence[int] | None,
        overrides: BacktestOverrides | None,
    ) -> _ResolvedBacktestParameters:
        candidate_windows = windows or DEFAULT_WINDOWS
        resolved_windows = sorted({int(value) for value in candidate_windows if int(value) > 0})
        if not resolved_windows:
            raise ValueError("At least one positive backtest window is required.")

        overrides = overrides or BacktestOverrides()
        min_trade_count = (
            overrides.min_trade_count if overrides.min_trade_count is not None else 5
        )
        min_win_rate = (
            overrides.min_win_rate
            if overrides.min_win_rate is not None
            else self._settings.thresholds.min_win_rate
        )

        return _ResolvedBacktestParameters(
            windows=resolved_windows,
            win_return_threshold=overrides.win_return_threshold,
            loss_return_threshold=overrides.loss_return_threshold,
            min_trade_count=min_trade_count,
            min_win_rate=min_win_rate,
        )

    def _compute_window(
        self,
        window_days: int,
        start: datetime,
        end: datetime,
        trades: Sequence[ExecutionEvent],
        params: _ResolvedBacktestParameters,
    ) -> BacktestWindowResult:
        ordered_trades = sorted(trades, key=lambda trade: trade.closed_at)
        trade_count = len(ordered_trades)
        wins = 0
        losses = 0
        unclassified = 0
        returns: List[float] = []
        win_returns: List[float] = []
        loss_returns: List[float] = []

        for trade in ordered_trades:
            classification = self._classify_trade(trade, params)
            if trade.return_pct is not None:
                returns.append(trade.return_pct)
            if classification == "win":
                wins += 1
                if trade.return_pct is not None:
                    win_returns.append(trade.return_pct)
            elif classification == "loss":
                losses += 1
                if trade.return_pct is not None:
                    loss_returns.append(trade.return_pct)
            else:
                unclassified += 1

        hit_rate = wins / trade_count if trade_count else 0.0
        average_return = mean(returns) if returns else 0.0

        expectancy = 0.0
        if trade_count:
            avg_win = mean(win_returns) if win_returns else 0.0
            avg_loss = abs(mean(loss_returns)) if loss_returns else 0.0
            loss_rate = max(1.0 - hit_rate, 0.0)
            expectancy = (avg_win * hit_rate) - (avg_loss * loss_rate)

        equity_curve = self._equity_curve(returns)
        cumulative_return = equity_curve[-1] - 1.0 if equity_curve else 0.0
        max_drawdown = self._max_drawdown(equity_curve)

        meets_win_rate = hit_rate >= params.min_win_rate if trade_count else False
        sufficient_sample = trade_count >= params.min_trade_count

        first_trade_at = ordered_trades[0].closed_at if ordered_trades else None
        last_trade_at = ordered_trades[-1].closed_at if ordered_trades else None

        return BacktestWindowResult(
            window_days=window_days,
            start=start,
            end=end,
            first_trade_at=first_trade_at,
            last_trade_at=last_trade_at,
            trade_count=trade_count,
            wins=wins,
            losses=losses,
            unclassified=unclassified,
            hit_rate=hit_rate,
            expectancy=expectancy,
            average_return=average_return,
            cumulative_return=cumulative_return,
            max_drawdown=max_drawdown,
            meets_win_rate_threshold=meets_win_rate,
            sufficient_sample=sufficient_sample,
        )

    @staticmethod
    def _equity_curve(returns: Iterable[float]) -> List[float]:
        equity = 1.0
        curve: List[float] = []
        for value in returns:
            equity *= 1.0 + value
            curve.append(equity)
        return curve

    @staticmethod
    def _max_drawdown(curve: Iterable[float]) -> float:
        peak = 0.0
        max_drawdown = 0.0
        for value in curve:
            if value > peak:
                peak = value
            if peak > 0:
                drawdown = (peak - value) / peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
        return max_drawdown

    @staticmethod
    def _classify_trade(
        trade: ExecutionEvent,
        params: _ResolvedBacktestParameters,
    ) -> str:
        if trade.return_pct is not None:
            if (
                params.win_return_threshold is not None
                and trade.return_pct >= params.win_return_threshold
            ):
                return "win"
            if (
                params.loss_return_threshold is not None
                and trade.return_pct <= params.loss_return_threshold
            ):
                return "loss"

        if trade.outcome:
            outcome = trade.outcome.lower()
            if outcome in {"win", "loss"}:
                return outcome

        if trade.return_pct is None:
            return "unknown"
        return "win" if trade.return_pct >= 0 else "loss"

    def _summarise(self, results: Sequence[BacktestWindowResult]) -> BacktestSummary:
        if not results:
            return BacktestSummary(
                window_days=0,
                trade_count=0,
                wins=0,
                losses=0,
                hit_rate=0.0,
                expectancy=0.0,
                cumulative_return=0.0,
                max_drawdown=0.0,
                meets_win_rate_threshold=False,
                sufficient_sample=False,
            )

        anchor = max(results, key=lambda result: result.window_days)
        return BacktestSummary(
            window_days=anchor.window_days,
            trade_count=anchor.trade_count,
            wins=anchor.wins,
            losses=anchor.losses,
            hit_rate=anchor.hit_rate,
            expectancy=anchor.expectancy,
            cumulative_return=anchor.cumulative_return,
            max_drawdown=anchor.max_drawdown,
            meets_win_rate_threshold=anchor.meets_win_rate_threshold,
            sufficient_sample=anchor.sufficient_sample,
        )

    def _persist_report(self, report: BacktestReport) -> None:
        if not self._log_dir:
            return

        try:
            self._log_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _LOGGER.warning(
                "Unable to prepare backtest log directory %s: %s", self._log_dir, exc
            )
            return

        timestamp = report.generated_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        json_path = self._log_dir / f"backtest_{timestamp}.json"
        try:
            json_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
        except OSError as exc:
            _LOGGER.warning("Unable to persist backtest report JSON to %s: %s", json_path, exc)

        try:
            self._append_summary_log(report)
        except OSError as exc:
            _LOGGER.warning("Unable to append backtest summary log: %s", exc)

    def _append_summary_log(self, report: BacktestReport) -> None:
        csv_path = self._log_dir / "backtest_trend.csv"
        file_exists = csv_path.exists()
        summary = report.summary

        with csv_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if not file_exists:
                writer.writerow(
                    [
                        "generated_at",
                        "window_days",
                        "trade_count",
                        "wins",
                        "losses",
                        "hit_rate",
                        "expectancy",
                        "cumulative_return",
                        "max_drawdown",
                        "meets_win_rate_threshold",
                        "sufficient_sample",
                    ]
                )
            writer.writerow(
                [
                    report.generated_at.isoformat(),
                    summary.window_days,
                    summary.trade_count,
                    summary.wins,
                    summary.losses,
                    f"{summary.hit_rate:.6f}",
                    f"{summary.expectancy:.6f}",
                    f"{summary.cumulative_return:.6f}",
                    f"{summary.max_drawdown:.6f}",
                    summary.meets_win_rate_threshold,
                    summary.sufficient_sample,
                ]
            )
