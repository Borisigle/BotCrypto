from __future__ import annotations

from typing import List

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_signal_feed_contract_and_filters() -> None:
    resp = client.get("/api/v1/signals/feed")
    assert resp.status_code == 200
    payload = resp.json()
    assert "generated_at" in payload
    assert "signals" in payload and isinstance(payload["signals"], list) and payload["signals"]
    assert "filters" in payload and isinstance(payload["filters"], dict)

    filters = payload["filters"]
    assert "symbols" in filters and "BTCUSDT" in filters["symbols"]
    assert "confidences" in filters and set(filters["confidences"]) >= {"high", "medium"}
    assert "sessions" in filters and set(filters["sessions"]) >= {"london", "new_york", "asia"}

    # Apply filters and validate all results match
    resp = client.get("/api/v1/signals/feed", params={"symbol": "BTCUSDT", "confidence": "high", "session": "new_york"})
    assert resp.status_code == 200
    filtered = resp.json()
    assert filtered["signals"], "Expected at least one signal after filtering"
    for item in filtered["signals"]:
        assert item["symbol"] == "BTCUSDT"
        assert item.get("confidence") == "high"
        assert item.get("session") == "new_york"


def test_signal_fetch_by_id_and_debug() -> None:
    # Known id from sample data
    signal_id = 1201

    resp = client.get(f"/api/v1/signals/{signal_id}")
    assert resp.status_code == 200
    item = resp.json()
    assert item["id"] == signal_id
    assert item["symbol"] == "BTCUSDT"
    assert "market_point" in item and isinstance(item["market_point"], dict)

    debug = client.get(f"/api/v1/signals/{signal_id}/debug")
    assert debug.status_code == 200
    report = debug.json()
    assert report["signal_id"] == signal_id
    assert report["symbol"] == "BTCUSDT"
    assert isinstance(report.get("contributions"), dict)
    assert set(report["contributions"].keys()) >= {"confidence_weight", "delta_oi_weight", "cvd_weight"}
    assert isinstance(report.get("total_score"), float)


def test_prometheus_metrics_include_setup_and_confidence() -> None:
    resp = client.get("/metrics/prometheus")
    assert resp.status_code == 200
    text = resp.text
    # Ensure new metric series are present
    assert "signals_by_setup_total" in text
    assert 'signals_by_setup_total{setup="squeeze_reversal"}' in text
    assert "signals_confidence_total" in text
    # At least one confidence bucket appears
    assert ('signals_confidence_total{confidence="high"}' in text) or (
        'signals_confidence_total{confidence="medium"}' in text
    )
