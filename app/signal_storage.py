from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Protocol, Sequence

from .models import SignalEvent


def _parse_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


@dataclass(frozen=True)
class SignalRecord:
    """Audit record capturing a generated signal and associated metadata."""

    event: SignalEvent
    metadata: Dict[str, float | None]
    created_at: datetime

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "event": self.event.model_dump(mode="json"),
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
        }
        return payload


class SignalStorage(Protocol):
    """Protocol describing storage behaviour for scored signals."""

    def next_id(self) -> int:  # pragma: no cover - exercised via implementations
        ...

    def store(self, record: SignalRecord) -> None:  # pragma: no cover - exercised via implementations
        ...

    def records(self) -> Sequence[SignalRecord]:  # pragma: no cover - exercised via implementations
        ...


class InMemorySignalStorage:
    """Simple in-memory storage useful for tests and local prototyping."""

    def __init__(self, *, starting_id: int = 1000) -> None:
        self._records: List[SignalRecord] = []
        self._next_id = starting_id

    def next_id(self) -> int:
        value = self._next_id
        self._next_id += 1
        return value

    def store(self, record: SignalRecord) -> None:
        self._records.append(record)

    def records(self) -> Sequence[SignalRecord]:
        return tuple(self._records)


class FileSignalStorage:
    """File-backed storage that preserves generated signals for auditability."""

    def __init__(self, path: Path, *, starting_id: int = 1000) -> None:
        self._path = path
        self._records: List[SignalRecord] = []
        self._next_id = starting_id
        self._load_existing()

    def _load_existing(self) -> None:
        if not self._path.exists():
            return
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError:  # pragma: no cover - defensive guard
            return

        for raw in payload:
            event = SignalEvent.model_validate(raw.get("event", {}))
            created_at_raw = raw.get("created_at")
            if isinstance(created_at_raw, str):
                try:
                    created_at = _parse_timestamp(created_at_raw)
                except ValueError:  # pragma: no cover - defensive guard
                    created_at = event.generated_at
            else:  # pragma: no cover - defensive
                created_at = event.generated_at
            metadata = raw.get("metadata", {})
            record = SignalRecord(event=event, metadata=metadata, created_at=created_at)
            self._records.append(record)
            if event.id >= self._next_id:
                self._next_id = event.id + 1

    def next_id(self) -> int:
        value = self._next_id
        self._next_id += 1
        return value

    def store(self, record: SignalRecord) -> None:
        self._records.append(record)
        self._persist()

    def records(self) -> Sequence[SignalRecord]:
        return tuple(self._records)

    def _persist(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = [record.to_dict() for record in self._records]
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
