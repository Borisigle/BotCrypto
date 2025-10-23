from __future__ import annotations

import logging
from logging.config import dictConfig

from .config import get_settings


def setup_logging() -> None:
    settings = get_settings()
    level_name = settings.log_level.upper()
    # Validate level
    level = getattr(logging, level_name, logging.INFO)
    level_name = logging.getLevelName(level)

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                    "datefmt": "%Y-%m-%dT%H:%M:%S%z",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                    "level": level_name,
                }
            },
            "root": {
                "level": level_name,
                "handlers": ["console"],
            },
        }
    )
