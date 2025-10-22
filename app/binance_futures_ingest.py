from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from contextlib import AbstractAsyncContextManager, suppress
from typing import AsyncIterator, Awaitable, Callable, Mapping, Optional, Sequence

import httpx
import websockets
from websockets.client import WebSocketClientProtocol

from .binance_futures_types import AggTrade, Candle, FundingRate, OpenInterestStat

logger = logging.getLogger(__name__)


class BinanceRESTError(RuntimeError):
    """Raised when the Binance REST API cannot be reached successfully."""


class BinanceWebSocketError(RuntimeError):
    """Raised when the Binance WebSocket feed encounters unrecoverable errors."""


class RateLimiter:
    """Simple async rate limiter supporting weighted acquisitions."""

    def __init__(
        self,
        capacity: int,
        interval: float,
        *,
        clock: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if interval <= 0:
            raise ValueError("interval must be positive")
        self._capacity = capacity
        self._interval = interval
        self._clock = clock
        self._sleep = sleep or asyncio.sleep
        self._lock = asyncio.Lock()
        self._events: deque[float] = deque()

    async def acquire(self, weight: int = 1) -> None:
        if weight <= 0:
            raise ValueError("weight must be positive")

        while True:
            async with self._lock:
                now = self._now()
                self._trim(now)
                if len(self._events) + weight <= self._capacity:
                    for _ in range(weight):
                        self._events.append(now)
                    return
                wait_time = max(0.0, self._interval - (now - self._events[0]))
            await self._sleep(wait_time)

    def _now(self) -> float:
        if self._clock is not None:
            return float(self._clock())
        return asyncio.get_running_loop().time()

    def _trim(self, now: float) -> None:
        while self._events and now - self._events[0] >= self._interval:
            self._events.popleft()


class ExponentialBackoff:
    """Helper implementing capped exponential backoff."""

    def __init__(self, initial: float = 1.0, factor: float = 2.0, maximum: float = 60.0) -> None:
        if initial <= 0:
            raise ValueError("initial backoff must be positive")
        if factor < 1:
            raise ValueError("factor must be >= 1")
        if maximum < initial:
            raise ValueError("maximum backoff must be >= initial")
        self._initial = initial
        self._factor = factor
        self._maximum = maximum
        self._attempts = 0

    def next_delay(self) -> float:
        delay = min(self._initial * (self._factor ** self._attempts), self._maximum)
        self._attempts += 1
        return delay

    def reset(self) -> None:
        self._attempts = 0


class BinanceFuturesRESTClient:
    """Asynchronous REST helper targeting Binance Futures endpoints."""

    def __init__(
        self,
        *,
        base_url: str = "https://fapi.binance.com",
        timeout: float = 10.0,
        limiter: Optional[RateLimiter] = None,
        client: Optional[httpx.AsyncClient] = None,
        max_retries: int = 3,
        backoff_seconds: float = 0.5,
        backoff_factor: float = 2.0,
        max_backoff_seconds: float = 5.0,
        retry_statuses: Optional[Sequence[int]] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        request_weights: Optional[Mapping[str, int]] = None,
    ) -> None:
        self._client = client or httpx.AsyncClient(base_url=base_url, timeout=timeout)
        self._owns_client = client is None
        self._limiter = limiter or RateLimiter(capacity=1200, interval=60.0)
        self._max_retries = max(1, max_retries)
        self._backoff_seconds = max(0.0, backoff_seconds)
        self._backoff_factor = max(1.0, backoff_factor)
        self._max_backoff_seconds = max_backoff_seconds
        self._sleep = sleep or asyncio.sleep
        self._retry_statuses = set(retry_statuses or (418, 429, 500, 502, 503, 504))
        self._weights = {
            "klines": 2,
            "open_interest": 2,
            "funding": 1,
        }
        if request_weights:
            self._weights.update({k: max(1, int(v)) for k, v in request_weights.items()})

    async def __aenter__(self) -> "BinanceFuturesRESTClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def fetch_klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1200,
    ) -> Sequence[Sequence[object]]:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        payload = await self._get("/fapi/v1/klines", params, weight=self._weights["klines"])
        assert isinstance(payload, list)
        return payload

    async def fetch_open_interest(
        self,
        *,
        symbol: str,
        period: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> Sequence[Mapping[str, object]]:
        params = {
            "symbol": symbol.upper(),
            "period": period,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        payload = await self._get(
            "/futures/data/openInterestHist",
            params,
            weight=self._weights["open_interest"],
        )
        assert isinstance(payload, list)
        return payload

    async def fetch_funding_rates(
        self,
        *,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> Sequence[Mapping[str, object]]:
        params = {
            "symbol": symbol.upper(),
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        payload = await self._get(
            "/fapi/v1/fundingRate",
            params,
            weight=self._weights["funding"],
        )
        assert isinstance(payload, list)
        return payload

    async def _get(self, path: str, params: Mapping[str, object], *, weight: int) -> object:
        attempt = 0
        backoff = max(self._backoff_seconds, 0.0)
        while True:
            attempt += 1
            try:
                await self._limiter.acquire(weight)
                response = await self._client.get(path, params=params)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                if attempt >= self._max_retries or status_code not in self._retry_statuses:
                    raise BinanceRESTError(str(exc)) from exc
                delay = min(backoff, self._max_backoff_seconds)
                if delay > 0:
                    await self._sleep(delay)
                backoff = delay * self._backoff_factor if delay > 0 else self._backoff_factor
            except httpx.HTTPError as exc:
                if attempt >= self._max_retries:
                    raise BinanceRESTError(str(exc)) from exc
                delay = min(backoff, self._max_backoff_seconds)
                if delay > 0:
                    await self._sleep(delay)
                backoff = delay * self._backoff_factor if delay > 0 else self._backoff_factor


class TradeStreamerProtocol:
    """Protocol describing an aggregated trade streamer."""

    async def stream(
        self, symbol: str, stop_event: Optional[asyncio.Event] = None
    ) -> AsyncIterator[AggTrade]:
        raise NotImplementedError


class BinanceAggTradeWebSocket(TradeStreamerProtocol):
    """WebSocket client streaming aggregated trades for Binance Futures."""

    def __init__(
        self,
        *,
        base_url: str = "wss://fstream.binance.com",
        connect: Optional[Callable[[str], AbstractAsyncContextManager]] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._connect_factory = connect or (lambda uri: websockets.connect(uri, ping_interval=20, ping_timeout=20))
        self._sleep = sleep or asyncio.sleep

    async def stream(
        self, symbol: str, stop_event: Optional[asyncio.Event] = None
    ) -> AsyncIterator[AggTrade]:
        stream_path = f"{self._base_url}/ws/{symbol.lower()}@aggTrade"
        backoff = ExponentialBackoff(initial=1.0, factor=2.0, maximum=30.0)

        while stop_event is None or not stop_event.is_set():
            try:
                async with self._connect_factory(stream_path) as connection:
                    if not isinstance(connection, WebSocketClientProtocol):
                        # Some custom factories may return arbitrary objects exposing async iteration.
                        websocket = connection
                    else:
                        websocket = connection
                    backoff.reset()
                    async for message in websocket:
                        if stop_event and stop_event.is_set():
                            return
                        trade = self._decode_trade(symbol, message)
                        if trade is None:
                            continue
                        backoff.reset()
                        yield trade
            except Exception as exc:  # pragma: no cover - safety net for unexpected failures
                logger.warning("WebSocket error for %s: %s", symbol.upper(), exc, exc_info=True)
                delay = backoff.next_delay()
                if stop_event and stop_event.is_set():
                    break
                await self._sleep(delay)
            else:
                if stop_event and stop_event.is_set():
                    break
                delay = backoff.next_delay()
                await self._sleep(delay)

    def _decode_trade(self, symbol: str, message: object) -> Optional[AggTrade]:
        try:
            if isinstance(message, (bytes, bytearray)):
                payload = json.loads(message.decode("utf-8"))
            elif isinstance(message, str):
                payload = json.loads(message)
            else:
                payload = message
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.debug("Skipping malformed websocket payload for %s", symbol)
            return None

        if isinstance(payload, Mapping) and "data" in payload:
            payload = payload["data"]

        if not isinstance(payload, Mapping):
            return None

        required_keys = {"a", "p", "q", "f", "l", "T", "m"}
        if not required_keys <= payload.keys():
            return None

        return AggTrade(
            symbol=symbol.upper(),
            agg_trade_id=int(payload["a"]),
            price=float(payload["p"]),
            quantity=float(payload["q"]),
            first_trade_id=int(payload["f"]),
            last_trade_id=int(payload["l"]),
            timestamp=int(payload["T"]),
            is_buyer_maker=bool(payload["m"]),
        )


class BinanceFuturesIngestionService:
    """Coordinates REST and WebSocket ingestion pipelines for Binance Futures."""

    def __init__(
        self,
        *,
        rest_client: BinanceFuturesRESTClient,
        trade_streamer: TradeStreamerProtocol,
        repository: "TimescaleRepositoryProtocol",
        symbols: Sequence[str] = ("BTCUSDT", "ETHUSDT"),
        candle_interval: str = "1m",
        open_interest_period: str = "5m",
        candle_poll_interval: float = 30.0,
        open_interest_poll_interval: float = 60.0,
        funding_poll_interval: float = 60.0,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
    ) -> None:
        if not symbols:
            raise ValueError("At least one symbol must be configured for ingestion")
        self._rest_client = rest_client
        self._trade_streamer = trade_streamer
        self._repository = repository
        self._symbols = [symbol.upper() for symbol in symbols]
        self._candle_interval = candle_interval
        self._open_interest_period = open_interest_period
        self._candle_poll_interval = candle_poll_interval
        self._open_interest_poll_interval = open_interest_poll_interval
        self._funding_poll_interval = funding_poll_interval
        self._sleep = sleep or asyncio.sleep
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        if self._tasks:
            raise RuntimeError("Ingestion service already running")
        self._stop_event.clear()
        for symbol in self._symbols:
            self._tasks.append(asyncio.create_task(self._candles_loop(symbol), name=f"candles-{symbol}"))
            self._tasks.append(
                asyncio.create_task(self._open_interest_loop(symbol), name=f"open-interest-{symbol}")
            )
            self._tasks.append(asyncio.create_task(self._funding_loop(symbol), name=f"funding-{symbol}"))
            self._tasks.append(asyncio.create_task(self._trades_loop(symbol), name=f"trades-{symbol}"))

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:  # pragma: no cover - expected cancellation path
                pass
        self._tasks.clear()

    async def _candles_loop(self, symbol: str) -> None:
        backoff = ExponentialBackoff(initial=2.0, factor=2.0, maximum=120.0)
        while not self._stop_event.is_set():
            try:
                inserted = await self._ingest_candles_once(symbol)
                if inserted:
                    backoff.reset()
                    await self._wait_or_sleep(self._candle_poll_interval)
                else:
                    await self._wait_or_sleep(max(5.0, self._candle_poll_interval / 2))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive guard
                logger.exception("Candle ingestion loop failed for %s", symbol, exc_info=exc)
                delay = backoff.next_delay()
                await self._wait_or_sleep(delay)

    async def _open_interest_loop(self, symbol: str) -> None:
        backoff = ExponentialBackoff(initial=5.0, factor=2.0, maximum=180.0)
        while not self._stop_event.is_set():
            try:
                inserted = await self._ingest_open_interest_once(symbol)
                if inserted:
                    backoff.reset()
                await self._wait_or_sleep(self._open_interest_poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover
                logger.exception("Open interest ingestion failed for %s", symbol, exc_info=exc)
                delay = backoff.next_delay()
                await self._wait_or_sleep(delay)

    async def _funding_loop(self, symbol: str) -> None:
        backoff = ExponentialBackoff(initial=10.0, factor=2.0, maximum=300.0)
        while not self._stop_event.is_set():
            try:
                inserted = await self._ingest_funding_once(symbol)
                if inserted:
                    backoff.reset()
                await self._wait_or_sleep(self._funding_poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover
                logger.exception("Funding ingestion failed for %s", symbol, exc_info=exc)
                delay = backoff.next_delay()
                await self._wait_or_sleep(delay)

    async def _trades_loop(self, symbol: str) -> None:
        backoff = ExponentialBackoff(initial=2.0, factor=2.0, maximum=60.0)
        while not self._stop_event.is_set():
            try:
                async for trade in self._trade_streamer.stream(symbol, stop_event=self._stop_event):
                    await self._repository.insert_trades(symbol, [trade])
                    backoff.reset()
                if self._stop_event.is_set():
                    break
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover
                logger.exception("Trade stream failure for %s", symbol, exc_info=exc)
                delay = backoff.next_delay()
                await self._wait_or_sleep(delay)
            else:
                await self._wait_or_sleep(backoff.next_delay())

    async def _ingest_candles_once(self, symbol: str) -> bool:
        last_open_time = await self._repository.latest_candle_open_time(symbol)
        start_time = last_open_time + 1 if last_open_time is not None else None
        raw = await self._rest_client.fetch_klines(
            symbol=symbol,
            interval=self._candle_interval,
            start_time=start_time,
        )
        candles = [Candle.from_rest(symbol, entry) for entry in raw]
        if not candles:
            return False
        await self._repository.insert_candles(symbol, candles)
        return True

    async def _ingest_open_interest_once(self, symbol: str) -> bool:
        last_timestamp = await self._repository.latest_open_interest_timestamp(symbol)
        start_time = last_timestamp + 1 if last_timestamp is not None else None
        raw = await self._rest_client.fetch_open_interest(
            symbol=symbol,
            period=self._open_interest_period,
            start_time=start_time,
        )
        entries = [OpenInterestStat.from_rest(symbol, payload) for payload in raw]
        if not entries:
            return False
        await self._repository.insert_open_interest(symbol, entries)
        return True

    async def _ingest_funding_once(self, symbol: str) -> bool:
        last_timestamp = await self._repository.latest_funding_timestamp(symbol)
        start_time = last_timestamp + 1 if last_timestamp is not None else None
        raw = await self._rest_client.fetch_funding_rates(
            symbol=symbol,
            start_time=start_time,
        )
        entries = [FundingRate.from_rest(symbol, payload) for payload in raw]
        if not entries:
            return False
        await self._repository.insert_funding_rates(symbol, entries)
        return True

    async def _wait_or_sleep(self, duration: float) -> None:
        if duration <= 0:
            return
        if self._stop_event.is_set():
            return
        stop_task = asyncio.create_task(self._stop_event.wait())
        sleep_task = asyncio.create_task(self._sleep(duration))
        done, pending = await asyncio.wait(
            {stop_task, sleep_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        for task in done:
            if task is stop_task:
                return


class TimescaleRepositoryProtocol:
    """Protocol representing the persistence layer consumed by the ingestion service."""

    async def latest_candle_open_time(self, symbol: str) -> Optional[int]:
        raise NotImplementedError

    async def latest_open_interest_timestamp(self, symbol: str) -> Optional[int]:
        raise NotImplementedError

    async def latest_funding_timestamp(self, symbol: str) -> Optional[int]:
        raise NotImplementedError

    async def insert_candles(self, symbol: str, candles: Sequence[Candle]) -> None:
        raise NotImplementedError

    async def insert_open_interest(self, symbol: str, rows: Sequence[OpenInterestStat]) -> None:
        raise NotImplementedError

    async def insert_funding_rates(self, symbol: str, rows: Sequence[FundingRate]) -> None:
        raise NotImplementedError

    async def insert_trades(self, symbol: str, trades: Sequence[AggTrade]) -> None:
        raise NotImplementedError
