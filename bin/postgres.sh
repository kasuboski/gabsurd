#!/usr/bin/env bash
set -euo pipefail

# Start a postgres container for gabsurd development via Lima Docker.
#
# Port:       5432
# User:       gabsurd
# Password:   gabsurd
# Database:   gabsurd

CONTAINER_NAME="gabsurd-postgres"
DB_NAME="gabsurd"
DB_USER="gabsurd"
DB_PASS="gabsurd"
DB_PORT="5432"
IMAGE="postgres:17-alpine"

if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "✅ Postgres container '${CONTAINER_NAME}' already running on :${DB_PORT}"
else
  # Remove stale stopped container if any
  docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true

  echo "🚀 Starting Postgres container..."
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -e POSTGRES_USER="${DB_USER}" \
    -e POSTGRES_PASSWORD="${DB_PASS}" \
    -e POSTGRES_DB="${DB_NAME}" \
    -p "${DB_PORT}:5432" \
    "${IMAGE}"

  echo "⏳ Waiting for Postgres to be ready..."
  for i in $(seq 1 30); do
    if docker exec "${CONTAINER_NAME}" pg_isready -U "${DB_USER}" -d "${DB_NAME}" >/dev/null 2>&1; then
      echo "✅ Postgres is ready!"
      break
    fi
    sleep 1
  done
fi

echo ""
echo "DATABASE_URL=postgresql://${DB_USER}:${DB_PASS}@127.0.0.1:${DB_PORT}/${DB_NAME}"
