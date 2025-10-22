from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.binance_backfill import (
    BinanceBackfillConfig,
    BinanceBackfillJob,
    DatasetWriter,
    IngestionMetrics,
)


def test_dataset_writer_upsert(tmp_path: Path) -> None:
    target = tmp_path / "candles.jsonl"
    writer = DatasetWriter(target, key_field="open_time")

    first = {"open_time": 1_700_000_000_000, "close": 30000.0}
    stats = writer.upsert_many([first])
    assert stats.inserted == 1
    assert stats.total == 1
    writer.flush()

    # Loading again should read the existing record and treat duplicates as unchanged.
    writer = DatasetWriter(target, key_field="open_time")
    duplicate_stats = writer.upsert_many([first])
    assert duplicate_stats.unchanged == 1
    writer.flush()

    updated = dict(first)
    updated["close"] = 30010.5
    update_stats = writer.upsert_many([updated])
    assert update_stats.updated == 1
    writer.flush()

    payloads = [json.loads(line) for line in target.read_text().strip().splitlines()]
    assert payloads == [updated]


class _FakeBinanceClient:
    def __init__(self, base_ms: int) -> None:
        self.base_ms = base_ms
        self._klines_served = False
        self._trades_served = False
        self._open_interest_served = False
        self._funding_served = False
        self.closed = False

    def fetch_klines(self, **_: object) -> list[list[object]]:
        if self._klines_served:
            return []
        self._klines_served = True
        return [
            [
                self.base_ms + offset * 60_000,
                "29500.0",
                "29600.0",
                "29400.0",
                "29550.0",
                "100.0",
                self.base_ms + offset * 60_000 + 59_000,
                "200.0",
                10,
                "50.0",
                "80.0",
                "0",
            ]
            for offset in range(3)
        ]

    def fetch_agg_trades(self, **_: object) -> list[dict[str, object]]:
        if self._trades_served:
            return []
        self._trades_served = True
        return [
            {
                "a": 1000 + idx,
                "p": "29500.0",
                "q": "0.1",
                "f": 2000 + idx,
                "l": 2000 + idx,
                "T": self.base_ms + idx * 30_000,
                "m": idx % 2 == 0,
            }
            for idx in range(2)
        ]

    def fetch_open_interest(self, **_: object) -> list[dict[str, object]]:
        if self._open_interest_served:
            return []
        self._open_interest_served = True
        return [
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "12345.6",
                "sumOpenInterestValue": "345.67",
                "timestamp": self.base_ms,
            }
        ]

    def fetch_funding_rates(self, **_: object) -> list[dict[str, object]]:
        if self._funding_served:
            return []
        self._funding_served = True
        return [
            {
                "symbol": "BTCUSDT",
                "fundingRate": "0.00025",
                "fundingTime": self.base_ms,
                "markPrice": "29550.0",
                "indexPrice": "29540.0",
            }
        ]

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("resume", [True, False])
def test_backfill_job_creates_datasets(tmp_path: Path, resume: bool) -> None:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(minutes=5)
    base_ms = int(start.timestamp() * 1000)

    client = _FakeBinanceClient(base_ms)
    metrics = IngestionMetrics()
    job = BinanceBackfillJob(client, metrics=metrics)

    config = BinanceBackfillConfig(
        symbol="BTCUSDT",
        start_time=start,
        end_time=end,
        data_directory=tmp_path,
        include_candles=True,
        include_trades=True,
        include_open_interest=True,
        include_funding=True,
        resume=resume,
        interval="1m",
        open_interest_period="5m",
    )

    report = job.run(config)

    expected_files = [
        tmp_path / "btcusdt_1m_candles.jsonl",
        tmp_path / "btcusdt_agg_trades.jsonl",
        tmp_path / "btcusdt_open_interest_5m.jsonl",
        tmp_path / "btcusdt_funding.jsonl",
    ]
    for file_path in expected_files:
        assert file_path.exists(), f"Missing dataset {file_path}"

    candles = [json.loads(line) for line in expected_files[0].read_text().strip().splitlines()]
    assert len(candles) == 3
    trades = [json.loads(line) for line in expected_files[1].read_text().strip().splitlines()]
    assert len(trades) == 2
    open_interest = [json.loads(line) for line in expected_files[2].read_text().strip().splitlines()]
    assert len(open_interest) == 1
    funding = [json.loads(line) for line in expected_files[3].read_text().strip().splitlines()]
    assert len(funding) == 1

    assert "candles" in report.totals
    assert report.totals["candles"].fetched == 3
    assert metrics.summary()["candles"]["records"] == 3

    if resume:
        second_client = _FakeBinanceClient(base_ms)
        second_job = BinanceBackfillJob(second_client, metrics=IngestionMetrics())
        second_report = second_job.run(config)
        candles_after = [
            json.loads(line) for line in expected_files[0].read_text().strip().splitlines()
        ]
        assert len(candles_after) == 3
        assert second_report.totals["candles"].inserted == 0
        assert second_client.closed is True

    assert client.closed is True
