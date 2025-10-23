from __future__ import annotations

from functools import lru_cache
from typing import Optional

from dotenv import find_dotenv
from pydantic import Field
try:
    # Pydantic v2
    from pydantic_settings import BaseSettings, SettingsConfigDict
except Exception:  # pragma: no cover - fallback for environments without pydantic-settings
    from pydantic import BaseSettings  # type: ignore
    SettingsConfigDict = dict  # type: ignore


class Settings(BaseSettings):
    """Shared application configuration loaded from environment variables.

    All services (API, worker, web) should align on these names for consistency.
    """

    app_name: str = Field(default="monorepo-app", description="Application name")
    log_level: str = Field(default="INFO", description="Logging level: DEBUG/INFO/WARN/ERROR")

    # API
    api_host: str = Field(default="0.0.0.0", description="Host interface for the FastAPI server")
    api_port: int = Field(default=8000, description="Port for the FastAPI server")

    # Worker
    worker_poll_interval: float = Field(default=5.0, description="Seconds between worker loop iterations")

    # Web
    next_public_api_url: Optional[str] = Field(default=None, description="Public URL of the API for the Next.js app")

    try:
        model_config = SettingsConfigDict(
            env_file=find_dotenv(usecwd=True) or None,
            env_prefix="",
            extra="ignore",
        )  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover
        # pydantic v1 fallback, silently ignore if unavailable
        pass


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
