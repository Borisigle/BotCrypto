#!/usr/bin/env bash
set -euo pipefail

ALEMBIC_INI="${ALEMBIC_CONFIG:-/app/alembic.ini}"

if command -v alembic >/dev/null 2>&1 && [[ -f "${ALEMBIC_INI}" ]]; then
    echo "Running Alembic migrations..."
    alembic upgrade head
    echo "Alembic migrations complete."
else
    echo "Alembic not configured; skipping migration step."
fi
