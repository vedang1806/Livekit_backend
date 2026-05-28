#!/usr/bin/env bash
set -euo pipefail

# start.sh — run DB migrations, optional Celery worker, then start the backend
cd "$(dirname "$0")"

# Load .env if present
if [ -f .env ]; then
  # shellcheck disable=SC1091
  set -o allexport
  # shellcheck disable=SC1091
  source .env
  set +o allexport
fi

# Activate virtualenv if present
if [ -f .venv/bin/activate ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

echo "Running alembic migrations..."
alembic upgrade head

# Optionally start Celery when START_CELERY=yes
if [ "${START_CELERY:-no}" = "yes" ]; then
  echo "Starting Celery worker in background..."
  celery -A app.workers.celery_app worker --loglevel=info --concurrency="${CELERY_CONCURRENCY:-2}" -Q compliance &
fi

echo "Starting backend (uvicorn)..."
uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}" --reload
