import asyncio

from fastapi.testclient import TestClient

from app.binance_futures_types import AggTrade, Candle, FundingRate, OpenInterestStat
from app.main import app, get_timescale_repository
from app.timescale_repository import TimescaleRepository


_repo = TimescaleRepository(use_memory=True)


async def _seed_repository() -> None:
    candles = [
        Candle(
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
        ),
        Candle(
            symbol="BTCUSDT",
            open_time=3,
            close_time=4,
            open_price=10.5,
            high_price=12.0,
            low_price=10.0,
            close_price=11.0,
            volume=60.0,
            quote_volume=600.0,
            number_of_trades=12,
            taker_buy_volume=30.0,
            taker_buy_quote_volume=300.0,
        ),
    ]
    trades = [
        AggTrade(
            symbol="BTCUSDT",
            agg_trade_id=100,
            price=30000.0,
            quantity=0.1,
            first_trade_id=50,
            last_trade_id=50,
            timestamp=1_000,
            is_buyer_maker=False,
        ),
        AggTrade(
            symbol="BTCUSDT",
            agg_trade_id=101,
            price=30010.0,
            quantity=0.2,
            first_trade_id=51,
            last_trade_id=51,
            timestamp=1_100,
            is_buyer_maker=True,
        ),
    ]
    open_interest = [
        OpenInterestStat(
            symbol="BTCUSDT",
            timestamp=900,
            sum_open_interest=12000.0,
            sum_open_interest_value=340.0,
        )
    ]
    funding = [
        FundingRate(
            symbol="BTCUSDT",
            funding_time=1_200,
            funding_rate=0.00025,
            mark_price=30020.0,
            index_price=30015.0,
        )
    ]

    await _repo.insert_candles("BTCUSDT", candles)
    await _repo.insert_trades("BTCUSDT", trades)
    await _repo.insert_open_interest("BTCUSDT", open_interest)
    await _repo.insert_funding_rates("BTCUSDT", funding)


asyncio.run(_seed_repository())


async def _override_repository():  # pragma: no cover - dependency override hook
    return _repo


def teardown_module(_: object) -> None:
    app.dependency_overrides.pop(get_timescale_repository, None)


app.dependency_overrides[get_timescale_repository] = _override_repository

client = TestClient(app)


def test_binance_candles_endpoint_returns_latest_records() -> None:
    response = client.get("/api/v1/binance/candles", params={"symbol": "BTCUSDT", "limit": 5})
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTCUSDT"
    assert len(payload["candles"]) == 2
    assert payload["candles"][0]["open_time"] == 1
    assert payload["candles"][1]["open_time"] == 3


def test_binance_trades_endpoint_returns_trades() -> None:
    response = client.get("/api/v1/binance/trades", params={"symbol": "BTCUSDT", "limit": 10})
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTCUSDT"
    assert len(payload["trades"]) == 2
    assert payload["trades"][0]["agg_trade_id"] == 100
    assert payload["trades"][1]["agg_trade_id"] == 101


def test_binance_open_interest_endpoint_returns_rows() -> None:
    response = client.get(
        "/api/v1/binance/open-interest",
        params={"symbol": "BTCUSDT", "limit": 10},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTCUSDT"
    assert payload["open_interest"][0]["sum_open_interest"] == 12000.0


def test_binance_funding_endpoint_returns_rows() -> None:
    response = client.get("/api/v1/binance/funding", params={"symbol": "BTCUSDT", "limit": 5})
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTCUSDT"
    assert payload["funding"][0]["funding_rate"] == 0.00025
