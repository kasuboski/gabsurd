#!/usr/bin/env bash
set -euo pipefail

# Stop the gabsurd postgres container.
docker stop gabsurd-postgres 2>/dev/null || true
docker rm gabsurd-postgres 2>/dev/null || true
echo "✅ Postgres container stopped and removed."
