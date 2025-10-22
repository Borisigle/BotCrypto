#!/usr/bin/env python3
"""Sample utility to execute the backtest runner from the command line."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

from app.backtest import BacktestOverrides, BacktestRunner
from app.config import get_settings
from app.data_source import FileMetricsRepository, MetricsRepositoryError

DEFAULT_WINDOWS: List[int] = [30, 60, 90]


def _parse_windows(raw: str) -> List[int]:
    parts = [segment.strip() for segment in raw.split(",") if segment.strip()]
    if not parts:
        raise ValueError("At least one window length must be provided.")

    windows: List[int] = []
    for fragment in parts:
        try:
            value = int(fragment)
        except ValueError as exc:  # pragma: no cover - defensive parsing guard
            raise ValueError(f"Invalid window length: {fragment}") from exc
        if value <= 0:
            raise ValueError("Window lengths must be positive integers.")
        windows.append(value)

    return sorted({window for window in windows})


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Execute the ingestion monitoring backtest engine against a metrics snapshot "
            "to evaluate signal performance over configurable windows."
        )
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        help="Path to a metrics snapshot JSON file. Defaults to the path configured for the service.",
    )
    parser.add_argument(
        "--windows",
        type=str,
        default=",".join(str(window) for window in DEFAULT_WINDOWS),
        help="Comma-separated list of window lengths (in days) to evaluate. Default: 30,60,90.",
    )
    parser.add_argument(
        "--win-threshold",
        type=float,
        default=None,
        help="Override minimum fractional return required to classify a trade as a win.",
    )
    parser.add_argument(
        "--loss-threshold",
        type=float,
        default=None,
        help="Override maximum fractional return tolerated before classifying a trade as a loss.",
    )
    parser.add_argument(
        "--min-trade-count",
        type=int,
        default=None,
        help=(
            "Minimum number of trades required before a window is marked as a sufficient sample "
            "(defaults to 5 when omitted)."
        ),
    )
    parser.add_argument(
        "--min-win-rate",
        type=float,
        default=None,
        help=(
            "Target win rate expectation used when evaluating windows (0.0-1.0). "
            "Defaults to the global threshold configured for the service."
        ),
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Optional path to write the backtest summary as CSV in addition to JSON output.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        windows = _parse_windows(args.windows)
    except ValueError as exc:
        parser.error(str(exc))
        return

    settings = get_settings()
    snapshot_path = args.snapshot if args.snapshot else settings.metrics_snapshot_path
    repository = FileMetricsRepository(snapshot_path)
    runner = BacktestRunner(repository=repository, settings=settings)

    overrides = BacktestOverrides(
        win_return_threshold=args.win_threshold,
        loss_return_threshold=args.loss_threshold,
        min_trade_count=args.min_trade_count,
        min_win_rate=args.min_win_rate,
    )

    try:
        report = runner.run(windows=windows, overrides=overrides)
    except (MetricsRepositoryError, ValueError) as exc:
        parser.error(str(exc))
        return

    print(report.model_dump_json(indent=2))

    csv_target: Optional[Path] = args.csv
    if csv_target is not None:
        csv_target.parent.mkdir(parents=True, exist_ok=True)
        csv_payload = runner.to_csv(report)
        csv_target.write_text(csv_payload, encoding="utf-8")
        print(f"CSV summary written to {csv_target}", file=sys.stderr)


if __name__ == "__main__":
    main()
