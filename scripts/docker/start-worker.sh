#!/usr/bin/env bash
set -euo pipefail

/app/scripts/docker/wait-for-db.sh
/app/scripts/docker/run-migrations.sh

echo "Starting signal worker..."
exec python /app/scripts/docker/run_worker.py
