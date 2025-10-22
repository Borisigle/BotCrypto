from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Callable, Dict, Optional, Sequence

try:  # pragma: no cover - asyncpg is optional during unit tests
    import asyncpg  # type: ignore
except ImportError:  # pragma: no cover - handled gracefully in memory mode
    asyncpg = None  # type: ignore

from .binance_futures_types import AggTrade, Candle, FundingRate, OpenInterestStat


class TimescaleRepositoryError(RuntimeError):
    """Raised when the TimescaleDB repository encounters an unrecoverable error."""


class TimescaleRepository:
    """Persistence adapter capable of targeting TimescaleDB or in-memory storage."""

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        pool: Optional["asyncpg.Pool"] = None,
        use_memory: Optional[bool] = None,
    ) -> None:
        if use_memory is None:
            use_memory = dsn is None and pool is None
        self._use_memory = use_memory
        self._dsn = dsn
        self._pool: Optional["asyncpg.Pool"] = pool
        self._pool_lock = asyncio.Lock()
        self._sleep = asyncio.sleep

        if self._use_memory:
            self._candles: Dict[str, Dict[int, Candle]] = defaultdict(dict)
            self._trades: Dict[str, Dict[int, AggTrade]] = defaultdict(dict)
            self._open_interest: Dict[str, Dict[int, OpenInterestStat]] = defaultdict(dict)
            self._funding: Dict[str, Dict[int, FundingRate]] = defaultdict(dict)
            self._memory_lock = asyncio.Lock()
        else:
            if pool is None and dsn is None:
                raise TimescaleRepositoryError(
                    "TimescaleRepository requires a DSN when not operating in memory mode."
                )
            self._memory_lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._use_memory or self._pool is not None:
            return
        if asyncpg is None:  # pragma: no cover - dependency missing in tests
            raise TimescaleRepositoryError("asyncpg package is required for Timescale connectivity")
        async with self._pool_lock:
            if self._pool is None:
                if self._dsn is None:
                    raise TimescaleRepositoryError("Timescale DSN must be provided")
                self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5)

    async def close(self) -> None:
        if self._use_memory:
            return
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def insert_candles(self, symbol: str, candles: Sequence[Candle]) -> None:
        symbol_key = symbol.upper()
        if not candles:
            return
        if self._use_memory:
            await self._memory_upsert(self._candles, symbol_key, candles, key=lambda c: c.open_time)
            return
        await self._ensure_pool()
        assert self._pool is not None
        records = [
            (
                candle.symbol,
                candle.open_time,
                candle.close_time,
                candle.open_price,
                candle.high_price,
                candle.low_price,
                candle.close_price,
                candle.volume,
                candle.quote_volume,
                candle.number_of_trades,
                candle.taker_buy_volume,
                candle.taker_buy_quote_volume,
            )
            for candle in candles
        ]
        query = """
        INSERT INTO binance_futures_candles (
            symbol,
            open_time,
            close_time,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            quote_volume,
            number_of_trades,
            taker_buy_volume,
            taker_buy_quote_volume
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
        ON CONFLICT (symbol, open_time) DO UPDATE SET
            close_time = EXCLUDED.close_time,
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            quote_volume = EXCLUDED.quote_volume,
            number_of_trades = EXCLUDED.number_of_trades,
            taker_buy_volume = EXCLUDED.taker_buy_volume,
            taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume
        """
        async with self._pool.acquire() as connection:
            await connection.executemany(query, records)

    async def insert_trades(self, symbol: str, trades: Sequence[AggTrade]) -> None:
        symbol_key = symbol.upper()
        if not trades:
            return
        if self._use_memory:
            await self._memory_upsert(self._trades, symbol_key, trades, key=lambda t: t.agg_trade_id)
            return
        await self._ensure_pool()
        assert self._pool is not None
        records = [
            (
                trade.symbol,
                trade.agg_trade_id,
                trade.price,
                trade.quantity,
                trade.first_trade_id,
                trade.last_trade_id,
                trade.timestamp,
                trade.is_buyer_maker,
            )
            for trade in trades
        ]
        query = """
        INSERT INTO binance_futures_agg_trades (
            symbol,
            agg_trade_id,
            price,
            quantity,
            first_trade_id,
            last_trade_id,
            trade_timestamp,
            is_buyer_maker
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8
        )
        ON CONFLICT (symbol, agg_trade_id) DO UPDATE SET
            price = EXCLUDED.price,
            quantity = EXCLUDED.quantity,
            first_trade_id = EXCLUDED.first_trade_id,
            last_trade_id = EXCLUDED.last_trade_id,
            trade_timestamp = EXCLUDED.trade_timestamp,
            is_buyer_maker = EXCLUDED.is_buyer_maker
        """
        async with self._pool.acquire() as connection:
            await connection.executemany(query, records)

    async def insert_open_interest(
        self, symbol: str, rows: Sequence[OpenInterestStat]
    ) -> None:
        symbol_key = symbol.upper()
        if not rows:
            return
        if self._use_memory:
            await self._memory_upsert(self._open_interest, symbol_key, rows, key=lambda r: r.timestamp)
            return
        await self._ensure_pool()
        assert self._pool is not None
        records = [
            (
                row.symbol,
                row.timestamp,
                row.sum_open_interest,
                row.sum_open_interest_value,
            )
            for row in rows
        ]
        query = """
        INSERT INTO binance_futures_open_interest (
            symbol,
            observation_time,
            sum_open_interest,
            sum_open_interest_value
        ) VALUES (
            $1, $2, $3, $4
        )
        ON CONFLICT (symbol, observation_time) DO UPDATE SET
            sum_open_interest = EXCLUDED.sum_open_interest,
            sum_open_interest_value = EXCLUDED.sum_open_interest_value
        """
        async with self._pool.acquire() as connection:
            await connection.executemany(query, records)

    async def insert_funding_rates(self, symbol: str, rows: Sequence[FundingRate]) -> None:
        symbol_key = symbol.upper()
        if not rows:
            return
        if self._use_memory:
            await self._memory_upsert(self._funding, symbol_key, rows, key=lambda r: r.funding_time)
            return
        await self._ensure_pool()
        assert self._pool is not None
        records = [
            (
                row.symbol,
                row.funding_time,
                row.funding_rate,
                row.mark_price,
                row.index_price,
            )
            for row in rows
        ]
        query = """
        INSERT INTO binance_futures_funding (
            symbol,
            funding_time,
            funding_rate,
            mark_price,
            index_price
        ) VALUES (
            $1, $2, $3, $4, $5
        )
        ON CONFLICT (symbol, funding_time) DO UPDATE SET
            funding_rate = EXCLUDED.funding_rate,
            mark_price = EXCLUDED.mark_price,
            index_price = EXCLUDED.index_price
        """
        async with self._pool.acquire() as connection:
            await connection.executemany(query, records)

    async def latest_candle_open_time(self, symbol: str) -> Optional[int]:
        symbol_key = symbol.upper()
        if self._use_memory:
            async with self._memory_lock:
                store = self._candles.get(symbol_key)
                if not store:
                    return None
                return max(store.keys()) if store else None
        await self._ensure_pool()
        assert self._pool is not None
        query = """
        SELECT open_time FROM binance_futures_candles
        WHERE symbol = $1
        ORDER BY open_time DESC
        LIMIT 1
        """
        async with self._pool.acquire() as connection:
            result = await connection.fetchrow(query, symbol_key)
        return None if result is None else int(result["open_time"])

    async def latest_open_interest_timestamp(self, symbol: str) -> Optional[int]:
        symbol_key = symbol.upper()
        if self._use_memory:
            async with self._memory_lock:
                store = self._open_interest.get(symbol_key)
                if not store:
                    return None
                return max(store.keys()) if store else None
        await self._ensure_pool()
        assert self._pool is not None
        query = """
        SELECT observation_time FROM binance_futures_open_interest
        WHERE symbol = $1
        ORDER BY observation_time DESC
        LIMIT 1
        """
        async with self._pool.acquire() as connection:
            result = await connection.fetchrow(query, symbol_key)
        return None if result is None else int(result["observation_time"])

    async def latest_funding_timestamp(self, symbol: str) -> Optional[int]:
        symbol_key = symbol.upper()
        if self._use_memory:
            async with self._memory_lock:
                store = self._funding.get(symbol_key)
                if not store:
                    return None
                return max(store.keys()) if store else None
        await self._ensure_pool()
        assert self._pool is not None
        query = """
        SELECT funding_time FROM binance_futures_funding
        WHERE symbol = $1
        ORDER BY funding_time DESC
        LIMIT 1
        """
        async with self._pool.acquire() as connection:
            result = await connection.fetchrow(query, symbol_key)
        return None if result is None else int(result["funding_time"])

    async def fetch_latest_candles(self, symbol: str, limit: int) -> Sequence[Candle]:
        if limit <= 0:
            return []
        symbol_key = symbol.upper()
        if self._use_memory:
            async with self._memory_lock:
                store = self._candles.get(symbol_key, {})
                ordered = sorted(store.values(), key=lambda c: c.open_time)
                return ordered[-limit:]
        await self._ensure_pool()
        assert self._pool is not None
        query = """
        SELECT symbol, open_time, close_time, open_price, high_price, low_price, close_price,
               volume, quote_volume, number_of_trades, taker_buy_volume, taker_buy_quote_volume
        FROM binance_futures_candles
        WHERE symbol = $1
        ORDER BY open_time DESC
        LIMIT $2
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, symbol_key, limit)
        records = [
            Candle(
                symbol=row["symbol"],
                open_time=int(row["open_time"]),
                close_time=int(row["close_time"]),
                open_price=float(row["open_price"]),
                high_price=float(row["high_price"]),
                low_price=float(row["low_price"]),
                close_price=float(row["close_price"]),
                volume=float(row["volume"]),
                quote_volume=float(row["quote_volume"]),
                number_of_trades=int(row["number_of_trades"]),
                taker_buy_volume=float(row["taker_buy_volume"]),
                taker_buy_quote_volume=float(row["taker_buy_quote_volume"]),
            )
            for row in rows
        ]
        return list(reversed(records))

    async def fetch_latest_trades(self, symbol: str, limit: int) -> Sequence[AggTrade]:
        if limit <= 0:
            return []
        symbol_key = symbol.upper()
        if self._use_memory:
            async with self._memory_lock:
                store = self._trades.get(symbol_key, {})
                ordered = sorted(store.values(), key=lambda t: t.timestamp)
                return ordered[-limit:]
        await self._ensure_pool()
        assert self._pool is not None
        query = """
        SELECT symbol, agg_trade_id, price, quantity, first_trade_id, last_trade_id,
               trade_timestamp, is_buyer_maker
        FROM binance_futures_agg_trades
        WHERE symbol = $1
        ORDER BY trade_timestamp DESC, agg_trade_id DESC
        LIMIT $2
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, symbol_key, limit)
        records = [
            AggTrade(
                symbol=row["symbol"],
                agg_trade_id=int(row["agg_trade_id"]),
                price=float(row["price"]),
                quantity=float(row["quantity"]),
                first_trade_id=int(row["first_trade_id"]),
                last_trade_id=int(row["last_trade_id"]),
                timestamp=int(row["trade_timestamp"]),
                is_buyer_maker=bool(row["is_buyer_maker"]),
            )
            for row in rows
        ]
        return list(reversed(records))

    async def fetch_latest_open_interest(
        self, symbol: str, limit: int
    ) -> Sequence[OpenInterestStat]:
        if limit <= 0:
            return []
        symbol_key = symbol.upper()
        if self._use_memory:
            async with self._memory_lock:
                store = self._open_interest.get(symbol_key, {})
                ordered = sorted(store.values(), key=lambda r: r.timestamp)
                return ordered[-limit:]
        await self._ensure_pool()
        assert self._pool is not None
        query = """
        SELECT symbol, observation_time, sum_open_interest, sum_open_interest_value
        FROM binance_futures_open_interest
        WHERE symbol = $1
        ORDER BY observation_time DESC
        LIMIT $2
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, symbol_key, limit)
        records = [
            OpenInterestStat(
                symbol=row["symbol"],
                timestamp=int(row["observation_time"]),
                sum_open_interest=float(row["sum_open_interest"]),
                sum_open_interest_value=float(row["sum_open_interest_value"]),
            )
            for row in rows
        ]
        return list(reversed(records))

    async def fetch_latest_funding(self, symbol: str, limit: int) -> Sequence[FundingRate]:
        if limit <= 0:
            return []
        symbol_key = symbol.upper()
        if self._use_memory:
            async with self._memory_lock:
                store = self._funding.get(symbol_key, {})
                ordered = sorted(store.values(), key=lambda r: r.funding_time)
                return ordered[-limit:]
        await self._ensure_pool()
        assert self._pool is not None
        query = """
        SELECT symbol, funding_time, funding_rate, mark_price, index_price
        FROM binance_futures_funding
        WHERE symbol = $1
        ORDER BY funding_time DESC
        LIMIT $2
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, symbol_key, limit)
        records = [
            FundingRate(
                symbol=row["symbol"],
                funding_time=int(row["funding_time"]),
                funding_rate=float(row["funding_rate"]),
                mark_price=float(row["mark_price"]),
                index_price=float(row["index_price"]),
            )
            for row in rows
        ]
        return list(reversed(records))

    async def _memory_upsert(
        self,
        store: Dict[str, Dict[int, object]],
        symbol: str,
        records: Sequence[object],
        *,
        key: Callable[[object], int],
    ) -> None:
        async with self._memory_lock:
            bucket = store.setdefault(symbol, {})
            for record in records:
                bucket[key(record)] = record

    async def _ensure_pool(self) -> None:
        if self._use_memory:
            return
        if self._pool is None:
            await self.connect()
        if self._pool is None:
            raise TimescaleRepositoryError("Database pool is not initialised")
