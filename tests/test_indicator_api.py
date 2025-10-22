from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_fetch_cvd_curve_returns_points() -> None:
    params = {"symbol": "BTCUSDT", "timeframe": "5m", "session": "new_york"}
    resp = client.get("/api/v1/indicators/cvd", params=params)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["symbol"] == "BTCUSDT"
    assert payload["timeframe"] == "5m"
    assert payload["session"] == "new_york"
    assert len(payload["points"]) >= 3
    first_point = payload["points"][0]
    assert first_point["time"].startswith("2099-10-22T12:00:00")
    assert first_point["value"] == 1215.0

    # Second call should hit the cache but still return identical payload
    cached = client.get("/api/v1/indicators/cvd", params=params)
    assert cached.status_code == 200
    assert cached.json() == payload


def test_delta_oi_allows_sessionless_series() -> None:
    resp = client.get(
        "/api/v1/indicators/delta-oi",
        params={"symbol": "ETHUSDT", "timeframe": "1h"},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["symbol"] == "ETHUSDT"
    assert payload["timeframe"] == "1h"
    assert payload["session"] is None
    values = [point["value"] for point in payload["points"]]
    assert values == [0.18, 0.24, 0.21, 0.26, 0.29]


def test_volume_profile_returns_distribution_and_value_area() -> None:
    resp = client.get(
        "/api/v1/indicators/volume-profile",
        params={"symbol": "BTCUSDT", "timeframe": "5m", "session": "new_york"},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["vah"] == 27220.0
    assert payload["val"] == 26920.0
    assert payload["poc"] == 27080.0
    assert payload["vwap"] == 27065.0
    assert payload["value_area_volume_pct"] == 0.68
    assert len(payload["distribution"]) == 5
    assert payload["distribution"][0] == {"price": 26920.0, "volume": 182.0}


def test_indicator_not_found_returns_404() -> None:
    resp = client.get(
        "/api/v1/indicators/cvd",
        params={"symbol": "SOLUSDT", "timeframe": "5m"},
    )
    assert resp.status_code == 404
    body = resp.json()
    assert "detail" in body
