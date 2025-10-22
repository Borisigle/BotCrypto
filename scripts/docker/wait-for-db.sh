#!/usr/bin/env bash
set -euo pipefail

HOST="${DB_HOST:-timescaledb}"
PORT="${DB_PORT:-5432}"
USER="${DB_USER:-monitor}"
TIMEOUT="${DB_WAIT_TIMEOUT:-60}"

export PGPASSWORD="${DB_PASSWORD:-monitor}"

echo "Waiting for PostgreSQL at ${HOST}:${PORT} as ${USER}..."
for ((attempt = 1; attempt <= TIMEOUT; attempt++)); do
    if pg_isready -h "${HOST}" -p "${PORT}" -U "${USER}" >/dev/null 2>&1; then
        echo "PostgreSQL is available."
        exit 0
    fi
    sleep 1
    echo "Attempt ${attempt}/${TIMEOUT}: database not ready yet"
done

echo "Timed out waiting for PostgreSQL after ${TIMEOUT} seconds"
exit 1
