from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from .models import MetricsSnapshot


class MetricsRepositoryError(RuntimeError):
    """Raised when the metrics snapshot cannot be loaded."""


def _parse_iso8601(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class FileMetricsRepository:
    """Loads monitoring snapshots from a JSON file.

    The repository expects a JSON payload matching :class:`MetricsSnapshot` where datetime
    values are ISO-8601 strings. Datetime parsing is delegated to Pydantic model validation.
    """

    def __init__(self, snapshot_path: Path) -> None:
        self._snapshot_path = snapshot_path

    def fetch_snapshot(self) -> MetricsSnapshot:
        if not self._snapshot_path.exists():
            raise MetricsRepositoryError(
                f"Metrics snapshot not found at {self._snapshot_path}"
            )

        try:
            with self._snapshot_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError as exc:
            raise MetricsRepositoryError("Invalid metrics snapshot JSON") from exc

        # Coerce ISO strings into datetime before validation for better error messaging
        for entry in payload.get("ingestions", []):
            if isinstance(entry.get("received_at"), str):
                entry["received_at"] = _parse_iso8601(entry["received_at"])
        for entry in payload.get("signals", []):
            if isinstance(entry.get("generated_at"), str):
                entry["generated_at"] = _parse_iso8601(entry["generated_at"])
        for entry in payload.get("executions", []):
            if isinstance(entry.get("closed_at"), str):
                entry["closed_at"] = _parse_iso8601(entry["closed_at"])

        return MetricsSnapshot.model_validate(payload)

    @property
    def snapshot_path(self) -> Path:
        return self._snapshot_path
