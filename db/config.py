from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Optional

_DEFAULT_DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/ingestion"


def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_int(value: Optional[str], default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_optional_int(value: Optional[str]) -> Optional[int]:
    if value is None or value.strip() == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


@dataclass(frozen=True)
class DatabaseConfig:
    """Configuration for the ingestion warehouse connection."""

    url: str
    echo: bool
    pool_size: int
    max_overflow: int
    pool_timeout: int
    statement_timeout_ms: Optional[int] = None

    @property
    def connect_args(self) -> Dict[str, str]:
        args: Dict[str, str] = {}
        if self.statement_timeout_ms is not None:
            args["options"] = f"-c statement_timeout={self.statement_timeout_ms}"
        return args


@lru_cache(maxsize=1)
def get_database_config() -> DatabaseConfig:
    """Return the lazily cached database configuration."""

    return DatabaseConfig(
        url=os.getenv("DATABASE_URL", _DEFAULT_DATABASE_URL),
        echo=_parse_bool(os.getenv("DATABASE_ECHO"), False),
        pool_size=max(1, _parse_int(os.getenv("DATABASE_POOL_SIZE"), 10)),
        max_overflow=max(0, _parse_int(os.getenv("DATABASE_MAX_OVERFLOW"), 10)),
        pool_timeout=max(1, _parse_int(os.getenv("DATABASE_POOL_TIMEOUT"), 30)),
        statement_timeout_ms=_parse_optional_int(os.getenv("DATABASE_STATEMENT_TIMEOUT_MS")),
    )
