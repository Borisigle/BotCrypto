import asyncio
import json
from typing import List, Optional

import httpx

from app.binance_futures_ingest import (
    BinanceAggTradeWebSocket,
    BinanceFuturesIngestionService,
    BinanceFuturesRESTClient,
    RateLimiter,
)
from app.binance_futures_types import AggTrade, Candle, FundingRate, OpenInterestStat
from app.timescale_repository import TimescaleRepository


class _FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def advance(self, amount: float) -> None:
        self.value += amount

    def __call__(self) -> float:
        return self.value


def test_rate_limiter_waits_when_capacity_exceeded() -> None:
    clock = _FakeClock()
    sleeps: List[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        clock.advance(delay)

    limiter = RateLimiter(capacity=2, interval=60.0, clock=clock, sleep=fake_sleep)

    async def runner() -> None:
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()

    asyncio.run(runner())

    assert sleeps == [60.0]
    assert clock.value == 60.0


def test_rest_client_retries_and_observes_rate_limits() -> None:
    clock = _FakeClock()
    sleeps: List[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        clock.advance(delay)

    class SpyLimiter(RateLimiter):
        def __init__(self) -> None:
            super().__init__(capacity=10, interval=60.0, clock=clock, sleep=fake_sleep)
            self.weights: List[int] = []

        async def acquire(self, weight: int = 1) -> None:  # type: ignore[override]
            self.weights.append(weight)
            await super().acquire(weight)

    limiter = SpyLimiter()

    call_count = {"value": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["value"] += 1
        if call_count["value"] == 1:
            return httpx.Response(429, json={"code": -1, "msg": "rate limit"})
        payload = [
            [
                1_700_000_000_000,
                "30000.0",
                "30100.0",
                "29900.0",
                "30050.0",
                "150.0",
                1_700_000_000_059,
                "200.0",
                42,
                "75.0",
                "120.0",
                "0",
            ]
        ]
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)

    async def runner() -> List[List[object]]:
        client = httpx.AsyncClient(transport=transport, base_url="https://test")
        rest_client = BinanceFuturesRESTClient(
            client=client,
            limiter=limiter,
            max_retries=2,
            backoff_seconds=1.0,
            backoff_factor=2.0,
            max_backoff_seconds=4.0,
            sleep=fake_sleep,
        )
        try:
            payload = await rest_client.fetch_klines(symbol="BTCUSDT", interval="1m", limit=10)
        finally:
            await rest_client.close()
        return payload

    result = asyncio.run(runner())

    assert call_count["value"] == 2
    assert limiter.weights == [2, 2]
    assert sleeps == [1.0]
    assert len(result) == 1
    assert result[0][0] == 1_700_000_000_000


def test_timescale_repository_memory_roundtrip() -> None:
    repo = TimescaleRepository(use_memory=True)

    candle = Candle(
        symbol="BTCUSDT",
        open_time=1,
        close_time=2,
        open_price=10.0,
        high_price=11.0,
        low_price=9.5,
        close_price=10.5,
        volume=50.0,
        quote_volume=500.0,
        number_of_trades=10,
        taker_buy_volume=25.0,
        taker_buy_quote_volume=250.0,
    )
    trade = AggTrade(
        symbol="BTCUSDT",
        agg_trade_id=123,
        price=30000.5,
        quantity=0.5,
        first_trade_id=100,
        last_trade_id=101,
        timestamp=5,
        is_buyer_maker=True,
    )
    open_interest = OpenInterestStat(
        symbol="BTCUSDT",
        timestamp=3,
        sum_open_interest=12345.6,
        sum_open_interest_value=345.67,
    )
    funding = FundingRate(
        symbol="BTCUSDT",
        funding_time=4,
        funding_rate=0.00025,
        mark_price=30050.0,
        index_price=30040.0,
    )

    async def runner() -> None:
        await repo.insert_candles("BTCUSDT", [candle])
        await repo.insert_trades("BTCUSDT", [trade])
        await repo.insert_open_interest("BTCUSDT", [open_interest])
        await repo.insert_funding_rates("BTCUSDT", [funding])

        candles = await repo.fetch_latest_candles("BTCUSDT", 10)
        trades = await repo.fetch_latest_trades("BTCUSDT", 10)
        open_interest_rows = await repo.fetch_latest_open_interest("BTCUSDT", 10)
        funding_rows = await repo.fetch_latest_funding("BTCUSDT", 10)

        assert candles == [candle]
        assert trades == [trade]
        assert open_interest_rows == [open_interest]
        assert funding_rows == [funding]

    asyncio.run(runner())


class _FakeRESTClient:
    def __init__(self) -> None:
        self.initial_klines = [
            [
                1_700_000_000_000,
                "30000.0",
                "30100.0",
                "29900.0",
                "30050.0",
                "150.0",
                1_700_000_000_059,
                "200.0",
                42,
                "75.0",
                "120.0",
                "0",
            ]
        ]
        self.open_interest_rows = [
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "12345.6",
                "sumOpenInterestValue": "345.67",
                "timestamp": 1_700_000_000_000,
            }
        ]
        self.funding_rows = [
            {
                "symbol": "BTCUSDT",
                "fundingRate": "0.00025",
                "fundingTime": 1_700_000_000_000,
                "markPrice": "30050.0",
                "indexPrice": "30040.0",
            }
        ]
        self.last_candle_start: Optional[int] = None
        self.last_open_interest_start: Optional[int] = None
        self.last_funding_start: Optional[int] = None

    async def fetch_klines(self, *, symbol: str, interval: str, start_time: Optional[int] = None, **_: object) -> List[List[object]]:
        if start_time is not None:
            self.last_candle_start = start_time
            return []
        return self.initial_klines

    async def fetch_open_interest(
        self,
        *,
        symbol: str,
        period: str,
        start_time: Optional[int] = None,
        **_: object,
    ) -> List[dict[str, object]]:
        if start_time is not None:
            self.last_open_interest_start = start_time
            return []
        return self.open_interest_rows

    async def fetch_funding_rates(
        self,
        *,
        symbol: str,
        start_time: Optional[int] = None,
        **_: object,
    ) -> List[dict[str, object]]:
        if start_time is not None:
            self.last_funding_start = start_time
            return []
        return self.funding_rows


class _StaticTradeStreamer:
    def __init__(self, trades: List[AggTrade]) -> None:
        self._trades = trades

    async def stream(self, symbol: str, stop_event: Optional[asyncio.Event] = None):  # type: ignore[override]
        for trade in self._trades:
            yield trade
        if stop_event is not None:
            stop_event.set()
        return


def test_ingestion_service_ingests_rest_payloads() -> None:
    repo = TimescaleRepository(use_memory=True)
    fake_rest = _FakeRESTClient()
    trades = [
        AggTrade(
            symbol="BTCUSDT",
            agg_trade_id=200,
            price=30010.0,
            quantity=0.2,
            first_trade_id=300,
            last_trade_id=300,
            timestamp=1_700_000_000_100,
            is_buyer_maker=False,
        )
    ]
    trade_streamer = _StaticTradeStreamer(trades)

    service = BinanceFuturesIngestionService(
        rest_client=fake_rest,  # type: ignore[arg-type]
        trade_streamer=trade_streamer,  # type: ignore[arg-type]
        repository=repo,
        symbols=["BTCUSDT"],
        candle_poll_interval=0.01,
        open_interest_poll_interval=0.01,
        funding_poll_interval=0.01,
    )

    async def runner() -> None:
        inserted_candles = await service._ingest_candles_once("BTCUSDT")
        assert inserted_candles is True
        inserted_open_interest = await service._ingest_open_interest_once("BTCUSDT")
        assert inserted_open_interest is True
        inserted_funding = await service._ingest_funding_once("BTCUSDT")
        assert inserted_funding is True

        # Second pass should request with start cursors and insert nothing.
        second_candles = await service._ingest_candles_once("BTCUSDT")
        assert second_candles is False
        second_open_interest = await service._ingest_open_interest_once("BTCUSDT")
        assert second_open_interest is False
        second_funding = await service._ingest_funding_once("BTCUSDT")
        assert second_funding is False

        assert fake_rest.last_candle_start == 1_700_000_000_000 + 1
        assert fake_rest.last_open_interest_start == 1_700_000_000_000 + 1
        assert fake_rest.last_funding_start == 1_700_000_000_000 + 1

        # Manually persist trades using the repository to emulate the trade loop.
        await repo.insert_trades("BTCUSDT", trades)

        candles = await repo.fetch_latest_candles("BTCUSDT", 10)
        open_interest_rows = await repo.fetch_latest_open_interest("BTCUSDT", 10)
        funding_rows = await repo.fetch_latest_funding("BTCUSDT", 10)
        trade_rows = await repo.fetch_latest_trades("BTCUSDT", 10)

        assert candles and candles[0].open_price == 30000.0
        assert open_interest_rows and open_interest_rows[0].sum_open_interest == 12345.6
        assert funding_rows and funding_rows[0].funding_rate == 0.00025
        assert trade_rows == trades

    asyncio.run(runner())


def test_websocket_decode_trade_handles_payload() -> None:
    client = BinanceAggTradeWebSocket()
    message = json.dumps(
        {
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": 1234,
            "p": "30000.0",
            "q": "0.5",
            "f": 100,
            "l": 101,
            "T": 1_700_000_000_000,
            "m": True,
        }
    )
    trade = client._decode_trade("BTCUSDT", message)
    assert isinstance(trade, AggTrade)
    assert trade.agg_trade_id == 1234
    assert trade.is_buyer_maker is True
