#!/usr/bin/env bash
set -euo pipefail

/app/scripts/docker/wait-for-db.sh
/app/scripts/docker/run-migrations.sh

echo "Starting API server..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8080
