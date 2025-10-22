#!/usr/bin/env python3
"""Command line entrypoint for running Binance data backfills."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from app.binance_backfill import (
    BinanceAPIError,
    BinanceBackfillConfig,
    BinanceBackfillJob,
    BinanceBackfillError,
    BinanceRESTClient,
    IngestionMetrics,
)


def _parse_iso8601(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid ISO-8601 timestamp: {value}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill 1m candles, aggregated trades, open interest, and funding rate data "
            "from the Binance Futures REST API into JSONL snapshots."
        )
    )
    parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Trading pair to backfill (default: BTCUSDT).",
    )
    parser.add_argument(
        "--interval",
        default="1m",
        help="Candle interval to request (default: 1m).",
    )
    parser.add_argument(
        "--oi-period",
        default="5m",
        help="Aggregation period for open interest history (default: 5m).",
    )
    parser.add_argument(
        "--start",
        type=_parse_iso8601,
        help="UTC start timestamp (ISO-8601). Defaults to --window-days before end when omitted.",
    )
    parser.add_argument(
        "--end",
        type=_parse_iso8601,
        help="UTC end timestamp (ISO-8601). Defaults to current UTC time when omitted.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=90,
        help="Window length in days used when --start is not provided (default: 90).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("app/data/binance"),
        help="Directory to store the JSONL datasets (default: app/data/binance).",
    )
    parser.add_argument(
        "--candle-limit",
        type=int,
        default=1200,
        help="Batch size when requesting klines (default: 1200).",
    )
    parser.add_argument(
        "--trade-limit",
        type=int,
        default=1000,
        help="Batch size when requesting aggregated trades (default: 1000).",
    )
    parser.add_argument(
        "--open-interest-limit",
        type=int,
        default=500,
        help="Batch size when requesting open interest history (default: 500).",
    )
    parser.add_argument(
        "--funding-limit",
        type=int,
        default=1000,
        help="Batch size when requesting funding rates (default: 1000).",
    )
    parser.add_argument(
        "--skip-candles",
        action="store_true",
        help="Skip candle backfill stage.",
    )
    parser.add_argument(
        "--skip-trades",
        action="store_true",
        help="Skip trades backfill stage (useful for lightweight runs).",
    )
    parser.add_argument(
        "--skip-open-interest",
        action="store_true",
        help="Skip open interest backfill stage.",
    )
    parser.add_argument(
        "--skip-funding",
        action="store_true",
        help="Skip funding rate backfill stage.",
    )
    resume_group = parser.add_mutually_exclusive_group()
    resume_group.add_argument(
        "--resume",
        dest="resume",
        action="store_true",
        default=True,
        help="Resume from the most recent stored timestamp (default).",
    )
    resume_group.add_argument(
        "--full-refresh",
        dest="resume",
        action="store_false",
        help="Ignore existing data and re-fetch the entire window.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO).",
    )
    return parser


def _resolve_window(start: Optional[datetime], end: Optional[datetime], window_days: int) -> tuple[datetime, datetime]:
    resolved_end = end or datetime.now(timezone.utc)
    if start is None:
        resolved_start = resolved_end - timedelta(days=window_days)
    else:
        resolved_start = start
    if resolved_start >= resolved_end:
        raise argparse.ArgumentTypeError("start time must be earlier than end time")
    return resolved_start, resolved_end


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_config(args: argparse.Namespace, start: datetime, end: datetime) -> BinanceBackfillConfig:
    return BinanceBackfillConfig(
        symbol=args.symbol,
        start_time=start,
        end_time=end,
        interval=args.interval,
        resume=args.resume,
        include_candles=not args.skip_candles,
        include_trades=not args.skip_trades,
        include_open_interest=not args.skip_open_interest,
        include_funding=not args.skip_funding,
        data_directory=args.output.expanduser().resolve(),
        candle_limit=args.candle_limit,
        trade_limit=args.trade_limit,
        open_interest_limit=args.open_interest_limit,
        funding_limit=args.funding_limit,
        open_interest_period=args.oi_period,
    )


def _emit_summary(report: BackfillReport, metrics: IngestionMetrics) -> None:
    payload = {
        "backfill": report.as_dict(),
        "metrics": metrics.summary(),
    }
    json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    _configure_logging(args.log_level)

    try:
        start, end = _resolve_window(args.start, args.end, args.window_days)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
        return

    metrics = IngestionMetrics()
    client = BinanceRESTClient()
    job = BinanceBackfillJob(client, metrics=metrics)

    try:
        report = job.run(_build_config(args, start, end))
    except (BinanceAPIError, BinanceBackfillError, ValueError) as exc:
        logging.getLogger("binance_backfill").error("Backfill failed: %s", exc)
        parser.error(str(exc))
        return

    _emit_summary(report, metrics)


if __name__ == "__main__":
    main()
