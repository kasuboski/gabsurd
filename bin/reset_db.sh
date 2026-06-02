#!/usr/bin/env bash
set -euo pipefail

# Reset the gabsurd database: drops and recreates, then applies schema.
# Assumes the postgres container is running (run bin/postgres.sh first).

CONTAINER_NAME="gabsurd-postgres"
DB_NAME="gabsurd"
DB_USER="gabsurd"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "❌ Postgres container not running. Run bin/postgres.sh first."
  exit 1
fi

echo "🔄 Resetting database '${DB_NAME}'..."

docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d postgres -c "
  SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
   WHERE datname = '${DB_NAME}'
     AND pid <> pg_backend_pid();
" > /dev/null 2>&1 || true

docker exec "${CONTAINER_NAME}" dropdb --if-exists -U "${DB_USER}" "${DB_NAME}"
docker exec "${CONTAINER_NAME}" createdb -U "${DB_USER}" "${DB_NAME}"

echo "📋 Applying stubs..."
docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" < "${PROJECT_ROOT}/priv/stubs.sql"

echo "📋 Applying absurd schema..."
docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" < "${PROJECT_ROOT}/priv/absurd.sql"

echo "✅ Database reset complete."
