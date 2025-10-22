"""Utilities for backfilling Binance market data snapshots.

This module provides a small synchronous client for the Binance Futures REST API
along with orchestration helpers to backfill historical candles, trades, open
interest, and funding rates while keeping the local dataset idempotent.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://fapi.binance.com"


class BinanceBackfillError(RuntimeError):
    """Raised when the backfill execution fails."""


class BinanceAPIError(RuntimeError):
    """Raised when the Binance API returns an error after retries."""


@dataclass(frozen=True)
class BinanceBackfillConfig:
    """Configuration describing the Binance backfill run."""

    symbol: str
    start_time: datetime
    end_time: datetime
    interval: str = "1m"
    resume: bool = True
    include_candles: bool = True
    include_trades: bool = True
    include_open_interest: bool = True
    include_funding: bool = True
    data_directory: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent / "data" / "binance"
    )
    candle_limit: int = 1200
    trade_limit: int = 1000
    open_interest_limit: int = 500
    funding_limit: int = 1000
    open_interest_period: str = "5m"

    def __post_init__(self) -> None:
        if self.start_time.tzinfo is None or self.end_time.tzinfo is None:
            raise ValueError("start_time and end_time must be timezone-aware (UTC).")
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be greater than start_time.")
        if self.candle_limit <= 0:
            raise ValueError("candle_limit must be positive.")
        if self.trade_limit <= 0:
            raise ValueError("trade_limit must be positive.")
        if self.open_interest_limit <= 0:
            raise ValueError("open_interest_limit must be positive.")
        if self.funding_limit <= 0:
            raise ValueError("funding_limit must be positive.")


@dataclass
class UpsertStats:
    """Statistics describing the result of a dataset upsert."""

    inserted: int = 0
    updated: int = 0
    unchanged: int = 0

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.unchanged


@dataclass
class DataTypeReport:
    """Summary statistics for a particular data type ingestion."""

    name: str
    batches: int = 0
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    earliest_key: Optional[int] = None
    latest_key: Optional[int] = None

    def record_batch(
        self,
        records: Iterable[Mapping[str, Any]],
        stats: UpsertStats,
        *,
        key_field: str,
        range_field: Optional[str] = None,
    ) -> None:
        records_list = list(records)
        if not records_list:
            return
        cursor_field = range_field or key_field
        records_list.sort(key=lambda entry: int(entry[cursor_field]))
        self.batches += 1
        self.fetched += len(records_list)
        self.inserted += stats.inserted
        self.updated += stats.updated
        self.unchanged += stats.unchanged
        first_key = int(records_list[0][cursor_field])
        last_key = int(records_list[-1][cursor_field])
        if self.earliest_key is None or first_key < self.earliest_key:
            self.earliest_key = first_key
        if self.latest_key is None or last_key > self.latest_key:
            self.latest_key = last_key

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "batches": self.batches,
            "fetched": self.fetched,
            "inserted": self.inserted,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "earliest": _format_epoch_ms(self.earliest_key),
            "latest": _format_epoch_ms(self.latest_key),
        }


@dataclass
class BackfillReport:
    """Aggregated report for a backfill execution."""

    started_at: datetime
    completed_at: datetime
    totals: Dict[str, DataTypeReport]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "duration_seconds": (self.completed_at - self.started_at).total_seconds(),
            "data_types": {name: report.as_dict() for name, report in self.totals.items()},
        }


class IngestionMetrics:
    """Collects ingestion pacing metrics during a backfill run."""

    def __init__(self) -> None:
        self._metrics: Dict[str, MutableMapping[str, Any]] = {}

    def observe(
        self,
        data_type: str,
        *,
        batch_size: int,
        inserted: int,
        updated: int,
        duration_seconds: float,
    ) -> None:
        payload = self._metrics.setdefault(
            data_type,
            {
                "batches": 0,
                "records": 0,
                "inserted": 0,
                "updated": 0,
                "elapsed": 0.0,
            },
        )
        payload["batches"] += 1
        payload["records"] += batch_size
        payload["inserted"] += inserted
        payload["updated"] += updated
        payload["elapsed"] += duration_seconds

    def summary(self) -> Dict[str, Any]:
        summary: Dict[str, Any] = {}
        for data_type, metrics in self._metrics.items():
            elapsed = float(metrics["elapsed"])
            throughput = metrics["records"] / elapsed if elapsed > 0 else 0.0
            summary[data_type] = {
                "batches": metrics["batches"],
                "records": metrics["records"],
                "inserted": metrics["inserted"],
                "updated": metrics["updated"],
                "duration_seconds": round(elapsed, 3),
                "records_per_second": round(throughput, 3),
            }
        return summary


class DatasetWriter:
    """Handles idempotent upserts into JSONL dataset files."""

    def __init__(self, path: Path, *, key_field: str) -> None:
        self._path = path
        self.key_field = key_field
        self._records: Dict[str, Dict[str, Any]] = {}
        self._max_key: Optional[int] = None
        self._dirty: bool = False
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        with self._path.open("r", encoding="utf-8") as handle:
            for line in handle:
                payload = line.strip()
                if not payload:
                    continue
                record: Dict[str, Any] = json.loads(payload)
                key = str(record[self.key_field])
                self._records[key] = record
        if self._records:
            numeric_keys = [self._safe_int(key) for key in self._records]
            numeric_values = [value for value in numeric_keys if value is not None]
            if numeric_values:
                self._max_key = max(numeric_values)

    @staticmethod
    def _safe_int(value: str) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @property
    def highest_key(self) -> Optional[int]:
        return self._max_key

    def upsert_many(self, records: Iterable[Mapping[str, Any]]) -> UpsertStats:
        stats = UpsertStats()
        for record in records:
            key_value = record[self.key_field]
            key = str(key_value)
            existing = self._records.get(key)
            if existing == record:
                stats.unchanged += 1
                continue
            if existing is None:
                stats.inserted += 1
            else:
                stats.updated += 1
            self._records[key] = dict(record)
            numeric_key = self._safe_int(key)
            if numeric_key is not None:
                if self._max_key is None or numeric_key > self._max_key:
                    self._max_key = numeric_key
            self._dirty = True
        return stats

    def flush(self) -> None:
        if not self._dirty:
            return
        sorted_records = sorted(
            self._records.values(), key=lambda entry: entry[self.key_field]
        )
        tmp_path = self._path.with_suffix(self._path.suffix + ".tmp")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with tmp_path.open("w", encoding="utf-8") as handle:
            for record in sorted_records:
                handle.write(json.dumps(record, sort_keys=True))
                handle.write("\n")
        tmp_path.replace(self._path)
        self._dirty = False


class BinanceRESTClient:
    """Small synchronous helper for Binance Futures REST endpoints."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_seconds: float = 0.5,
        sleep_func: Callable[[float], None] = time.sleep,
        client: Optional[httpx.Client] = None,
    ) -> None:
        self._client = client or httpx.Client(base_url=base_url, timeout=timeout)
        self._owns_client = client is None
        self._max_retries = max(1, max_retries)
        self._backoff_seconds = max(backoff_seconds, 0.0)
        self._sleep = sleep_func

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "BinanceRESTClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def fetch_klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1200,
    ) -> List[List[Any]]:
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self._get("/fapi/v1/klines", params)

    def fetch_agg_trades(
        self,
        *,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        from_id: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if from_id is not None:
            params["fromId"] = from_id
        return self._get("/fapi/v1/aggTrades", params)

    def fetch_open_interest(
        self,
        *,
        symbol: str,
        period: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "period": period,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self._get("/futures/data/openInterestHist", params)

    def fetch_funding_rates(
        self,
        *,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self._get("/fapi/v1/fundingRate", params)

    def _get(self, path: str, params: Dict[str, Any]) -> Any:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._client.get(path, params=params)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                last_exc = exc
                status = exc.response.status_code
                if status in {418, 429, 504} and attempt < self._max_retries:
                    self._backoff(attempt)
                    continue
                raise BinanceAPIError(str(exc)) from exc
            except httpx.HTTPError as exc:  # pragma: no cover - network failure defensive guard
                last_exc = exc
                if attempt < self._max_retries:
                    self._backoff(attempt)
                    continue
                raise BinanceAPIError(str(exc)) from exc
        if last_exc:
            raise BinanceAPIError(str(last_exc))
        raise BinanceAPIError("Unknown Binance API error")

    def _backoff(self, attempt: int) -> None:
        delay = self._backoff_seconds * (2 ** (attempt - 1))
        if delay > 0:
            self._sleep(delay)


class BinanceBackfillJob:
    """Coordinates the Binance backfill workflow."""

    def __init__(
        self,
        client: BinanceRESTClient,
        *,
        metrics: Optional[IngestionMetrics] = None,
        logger_obj: Optional[logging.Logger] = None,
    ) -> None:
        self._client = client
        self._metrics = metrics or IngestionMetrics()
        self._logger = logger_obj or logger

    @property
    def metrics(self) -> IngestionMetrics:
        return self._metrics

    def run(self, config: BinanceBackfillConfig) -> BackfillReport:
        start_time = datetime.now(timezone.utc)
        reports: Dict[str, DataTypeReport] = {}
        try:
            if config.include_candles:
                reports["candles"] = self._run_candles(config)
            if config.include_trades:
                reports["trades"] = self._run_trades(config)
            if config.include_open_interest:
                reports["open_interest"] = self._run_open_interest(config)
            if config.include_funding:
                reports["funding"] = self._run_funding(config)
        finally:
            self._client.close()
        completed_at = datetime.now(timezone.utc)
        return BackfillReport(started_at=start_time, completed_at=completed_at, totals=reports)

    def _run_candles(self, config: BinanceBackfillConfig) -> DataTypeReport:
        report = DataTypeReport(name="candles")
        interval_ms = _interval_to_milliseconds(config.interval)
        start_ms = _datetime_to_milliseconds(config.start_time)
        end_ms = _datetime_to_milliseconds(config.end_time)
        path = config.data_directory / f"{config.symbol.lower()}_{config.interval}_candles.jsonl"
        writer = DatasetWriter(path, key_field="open_time")
        if config.resume and writer.highest_key is not None:
            start_ms = max(start_ms, writer.highest_key + 1)
        cursor = start_ms
        while cursor <= end_ms:
            batch_cursor = cursor
            request_start = time.monotonic()
            batch = self._client.fetch_klines(
                symbol=config.symbol,
                interval=config.interval,
                start_time=cursor,
                end_time=end_ms,
                limit=config.candle_limit,
            )
            duration = time.monotonic() - request_start
            if not batch:
                break
            records = [self._transform_kline(entry) for entry in batch]
            records = [
                item
                for item in records
                if batch_cursor <= item["open_time"] <= end_ms
            ]
            if not records:
                cursor = batch_cursor + interval_ms
                continue
            stats = writer.upsert_many(records)
            writer.flush()
            report.record_batch(records, stats, key_field="open_time")
            self._metrics.observe(
                "candles",
                batch_size=len(records),
                inserted=stats.inserted,
                updated=stats.updated,
                duration_seconds=duration,
            )
            cursor = int(records[-1]["open_time"]) + interval_ms
            self._logger.info(
                "[candles] fetched=%s inserted=%s updated=%s next_cursor=%s",
                len(records),
                stats.inserted,
                stats.updated,
                _format_epoch_ms(cursor),
            )
            if cursor > end_ms:
                break
        writer.flush()
        return report

    def _run_trades(self, config: BinanceBackfillConfig) -> DataTypeReport:
        report = DataTypeReport(name="trades")
        start_ms = _datetime_to_milliseconds(config.start_time)
        end_ms = _datetime_to_milliseconds(config.end_time)
        path = config.data_directory / f"{config.symbol.lower()}_agg_trades.jsonl"
        writer = DatasetWriter(path, key_field="agg_id")
        from_id: Optional[int] = None
        if config.resume and writer.highest_key is not None:
            from_id = writer.highest_key + 1
        cursor = start_ms
        window_ms = 60 * 60 * 1000  # one hour window slices
        while cursor <= end_ms:
            batch_cursor = cursor
            target_end = min(cursor + window_ms, end_ms)
            request_start = time.monotonic()
            batch = self._client.fetch_agg_trades(
                symbol=config.symbol,
                start_time=cursor,
                end_time=target_end,
                from_id=from_id,
                limit=config.trade_limit,
            )
            duration = time.monotonic() - request_start
            if not batch:
                cursor = target_end + 1
                continue
            records = [self._transform_trade(entry) for entry in batch]
            records = [
                item
                for item in records
                if batch_cursor <= item["timestamp"] <= end_ms
            ]
            if not records:
                cursor = target_end + 1
                continue
            stats = writer.upsert_many(records)
            writer.flush()
            report.record_batch(
                records,
                stats,
                key_field="agg_id",
                range_field="timestamp",
            )
            self._metrics.observe(
                "trades",
                batch_size=len(records),
                inserted=stats.inserted,
                updated=stats.updated,
                duration_seconds=duration,
            )
            from_id = None
            cursor = int(records[-1]["timestamp"]) + 1
            self._logger.info(
                "[trades] fetched=%s inserted=%s updated=%s next_cursor=%s",
                len(records),
                stats.inserted,
                stats.updated,
                _format_epoch_ms(cursor),
            )
            if cursor > end_ms:
                break
        writer.flush()
        return report

    def _run_open_interest(self, config: BinanceBackfillConfig) -> DataTypeReport:
        report = DataTypeReport(name="open_interest")
        period_ms = _interval_to_milliseconds(config.open_interest_period)
        start_ms = _datetime_to_milliseconds(config.start_time)
        end_ms = _datetime_to_milliseconds(config.end_time)
        path = config.data_directory / f"{config.symbol.lower()}_open_interest_{config.open_interest_period}.jsonl"
        writer = DatasetWriter(path, key_field="timestamp")
        if config.resume and writer.highest_key is not None:
            start_ms = max(start_ms, writer.highest_key + 1)
        cursor = start_ms
        window_ms = period_ms * config.open_interest_limit
        while cursor <= end_ms:
            batch_cursor = cursor
            target_end = min(cursor + window_ms, end_ms)
            request_start = time.monotonic()
            batch = self._client.fetch_open_interest(
                symbol=config.symbol,
                period=config.open_interest_period,
                start_time=cursor,
                end_time=target_end,
                limit=config.open_interest_limit,
            )
            duration = time.monotonic() - request_start
            if not batch:
                cursor = target_end + period_ms
                continue
            records = [self._transform_open_interest(entry) for entry in batch]
            records = [
                item
                for item in records
                if batch_cursor <= item["timestamp"] <= end_ms
            ]
            if not records:
                cursor = target_end + period_ms
                continue
            stats = writer.upsert_many(records)
            writer.flush()
            report.record_batch(records, stats, key_field="timestamp")
            self._metrics.observe(
                "open_interest",
                batch_size=len(records),
                inserted=stats.inserted,
                updated=stats.updated,
                duration_seconds=duration,
            )
            cursor = int(records[-1]["timestamp"]) + period_ms
            self._logger.info(
                "[open_interest] fetched=%s inserted=%s updated=%s next_cursor=%s",
                len(records),
                stats.inserted,
                stats.updated,
                _format_epoch_ms(cursor),
            )
            if cursor > end_ms:
                break
        writer.flush()
        return report

    def _run_funding(self, config: BinanceBackfillConfig) -> DataTypeReport:
        report = DataTypeReport(name="funding")
        start_ms = _datetime_to_milliseconds(config.start_time)
        end_ms = _datetime_to_milliseconds(config.end_time)
        path = config.data_directory / f"{config.symbol.lower()}_funding.jsonl"
        writer = DatasetWriter(path, key_field="funding_time")
        if config.resume and writer.highest_key is not None:
            start_ms = max(start_ms, writer.highest_key + 1)
        cursor = start_ms
        window_ms = 8 * 60 * 60 * 1000 * config.funding_limit
        while cursor <= end_ms:
            batch_cursor = cursor
            target_end = min(cursor + window_ms, end_ms)
            request_start = time.monotonic()
            batch = self._client.fetch_funding_rates(
                symbol=config.symbol,
                start_time=cursor,
                end_time=target_end,
                limit=config.funding_limit,
            )
            duration = time.monotonic() - request_start
            if not batch:
                cursor = target_end + 1
                continue
            records = [self._transform_funding(entry) for entry in batch]
            records = [
                item
                for item in records
                if batch_cursor <= item["funding_time"] <= end_ms
            ]
            if not records:
                cursor = target_end + 1
                continue
            stats = writer.upsert_many(records)
            writer.flush()
            report.record_batch(records, stats, key_field="funding_time")
            self._metrics.observe(
                "funding",
                batch_size=len(records),
                inserted=stats.inserted,
                updated=stats.updated,
                duration_seconds=duration,
            )
            cursor = int(records[-1]["funding_time"]) + 1
            self._logger.info(
                "[funding] fetched=%s inserted=%s updated=%s next_cursor=%s",
                len(records),
                stats.inserted,
                stats.updated,
                _format_epoch_ms(cursor),
            )
            if cursor > end_ms:
                break
        writer.flush()
        return report

    @staticmethod
    def _transform_kline(payload: List[Any]) -> Dict[str, Any]:
        return {
            "open_time": int(payload[0]),
            "open": float(payload[1]),
            "high": float(payload[2]),
            "low": float(payload[3]),
            "close": float(payload[4]),
            "volume": float(payload[5]),
            "close_time": int(payload[6]),
            "quote_volume": float(payload[7]),
            "trade_count": int(payload[8]),
            "taker_buy_volume": float(payload[9]),
            "taker_buy_quote_volume": float(payload[10]),
        }

    @staticmethod
    def _transform_trade(payload: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "agg_id": int(payload["a"]),
            "price": float(payload["p"]),
            "quantity": float(payload["q"]),
            "first_trade_id": int(payload["f"]),
            "last_trade_id": int(payload["l"]),
            "timestamp": int(payload["T"]),
            "is_buyer_maker": bool(payload["m"]),
        }

    @staticmethod
    def _transform_open_interest(payload: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "symbol": payload.get("symbol"),
            "sum_open_interest": float(payload["sumOpenInterest"]),
            "sum_open_interest_value": float(payload["sumOpenInterestValue"]),
            "timestamp": int(payload["timestamp"]),
        }

    @staticmethod
    def _transform_funding(payload: Mapping[str, Any]) -> Dict[str, Any]:
        result = {
            "symbol": payload.get("symbol"),
            "funding_rate": float(payload["fundingRate"]),
            "funding_time": int(payload["fundingTime"]),
        }
        if "markPrice" in payload:
            result["mark_price"] = float(payload["markPrice"])
        if "indexPrice" in payload:
            result["index_price"] = float(payload["indexPrice"])
        return result


def _datetime_to_milliseconds(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _interval_to_milliseconds(interval: str) -> int:
    unit = interval[-1]
    try:
        magnitude = int(interval[:-1])
    except ValueError as exc:
        raise ValueError(f"Invalid interval: {interval}") from exc
    multiplier: Dict[str, int] = {
        "s": 1000,
        "m": 60 * 1000,
        "h": 60 * 60 * 1000,
        "d": 24 * 60 * 60 * 1000,
        "w": 7 * 24 * 60 * 60 * 1000,
    }
    if unit not in multiplier:
        raise ValueError(f"Unsupported interval unit: {unit}")
    return magnitude * multiplier[unit]


def _format_epoch_ms(value: Optional[int]) -> Optional[str]:
    if value is None:
        return None
    ts = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return ts.isoformat()
