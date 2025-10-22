#!/usr/bin/env python
"""Simple worker entrypoint used in the Docker Compose stack."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from app.market_models import MarketDataset
from app.signal_worker import SignalScoringWorker

logging.basicConfig(
    level=os.getenv("WORKER_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

DATA_PATH = Path(os.getenv("MARKET_DATA_PATH", "/app/app/data/sample_market_data.json"))
SLEEP_SECONDS = int(os.getenv("WORKER_INTERVAL_SECONDS", "60"))


def load_dataset() -> MarketDataset:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Market data snapshot not found at {DATA_PATH}")
    with DATA_PATH.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return MarketDataset.model_validate(payload)


def main() -> None:
    worker = SignalScoringWorker()
    while True:
        try:
            dataset = load_dataset()
            events = worker.run(dataset)
            logging.info("Processed dataset; generated %d candidate signals", len(events))
        except Exception as exc:  # noqa: BLE001 - log and continue loop
            logging.exception("Worker iteration failed: %s", exc)
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
