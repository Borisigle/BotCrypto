from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict

# Ensure shared python packages are importable when running directly
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
SHARED_PYTHON = os.path.join(ROOT_DIR, "shared", "python")
if SHARED_PYTHON not in sys.path:
    sys.path.insert(0, SHARED_PYTHON)

from fastapi import FastAPI
from monorepo_common import get_settings, setup_logging


def create_app() -> FastAPI:
    setup_logging()
    settings = get_settings()

    app = FastAPI(title=f"{settings.app_name} API")

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        return {
            "status": "ok",
            "app": settings.app_name,
            "time": now,
        }

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    s = get_settings()
    uvicorn.run(app, host=s.api_host, port=s.api_port)
