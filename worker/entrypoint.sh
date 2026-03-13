#!/bin/sh
set -e

echo "Running database migrations..."
alembic upgrade head

echo "Starting worker..."
exec celery -A worker.celery_app worker -Q etl_jobs -c 1 --loglevel=info
