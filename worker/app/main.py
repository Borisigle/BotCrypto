from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import NoReturn

CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
SHARED_PYTHON = os.path.join(ROOT_DIR, "shared", "python")
if SHARED_PYTHON not in sys.path:
    sys.path.insert(0, SHARED_PYTHON)

from monorepo_common import get_settings, setup_logging

logger = logging.getLogger("worker")


async def run_loop() -> NoReturn:
    setup_logging()
    settings = get_settings()
    logger.info("Starting worker loop (interval=%ss)", settings.worker_poll_interval)

    while True:
        try:
            # Placeholder for actual work: fetch tasks, process jobs, etc.
            logger.debug("Worker heartbeat")
            await asyncio.sleep(settings.worker_poll_interval)
        except asyncio.CancelledError:
            logger.info("Worker loop cancelled, shutting down")
            raise
        except Exception:
            logger.exception("Unhandled error in worker loop")
            # Backoff a bit before retrying
            await asyncio.sleep(min(5.0, settings.worker_poll_interval))


def main() -> None:
    asyncio.run(run_loop())


if __name__ == "__main__":
    main()
