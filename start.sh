#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -f .env && -f .env.example ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

if [[ "${1:-}" != "--no-sync" ]]; then
  uv sync --dev
fi

PORT="${APP_PORT:-8000}"
MODE="${APP_MODE:-dev}"

if [[ "$MODE" == "prod" ]]; then
  exec uv run waitress-serve --host=0.0.0.0 --port="$PORT" app:app
else
  exec uv run flask --app app run --debug --host 0.0.0.0 --port "$PORT"
fi
