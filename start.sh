#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
LOCK_FILE="$PWD/.app.lock"

if [[ ! -f .env && -f .env.example ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if [[ "${1:-}" != "--no-sync" ]]; then
  uv sync --dev
fi

if [[ -x "$PWD/.venv/bin/python" ]]; then
  PYTHON_EXE="$PWD/.venv/bin/python"
elif [[ -x "$PWD/.venv/Scripts/python.exe" ]]; then
  PYTHON_EXE="$PWD/.venv/Scripts/python.exe"
else
  echo "Python executable not found in .venv. Run uv sync --dev first." >&2
  exit 1
fi

if ! "$PYTHON_EXE" "$PWD/validate_env.py"; then
  echo "Fix the .env values listed above and retry." >&2
  exit 1
fi

if [[ -f "$LOCK_FILE" ]]; then
  EXISTING_PID="$(head -n 1 "$LOCK_FILE" | tr -d '[:space:]')"
  if [[ "$EXISTING_PID" =~ ^[0-9]+$ ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "Stopping previous Octo Playlist Sync process (PID $EXISTING_PID)..."
    kill "$EXISTING_PID" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
      if ! kill -0 "$EXISTING_PID" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    kill -9 "$EXISTING_PID" 2>/dev/null || true
  fi
  rm -f "$LOCK_FILE"
fi

APP_PIDS="$(ps -eo pid=,command= | grep "$PWD" | grep -E 'flask|waitress' | grep -v grep | awk '{print $1}' || true)"
for proc_id in $APP_PIDS; do
  if [[ -n "$proc_id" ]]; then
    echo "Stopping stale repo app process (PID $proc_id)..."
    kill "$proc_id" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
      if ! kill -0 "$proc_id" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    kill -9 "$proc_id" 2>/dev/null || true
  fi
done

PORT=3000
export APP_PORT="$PORT"
MODE="${APP_MODE:-dev}"
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

if command -v lsof >/dev/null 2>&1; then
  PORT_PIDS="$(lsof -ti tcp:$PORT -sTCP:LISTEN 2>/dev/null || true)"
elif command -v fuser >/dev/null 2>&1; then
  PORT_PIDS="$(fuser ${PORT}/tcp 2>/dev/null || true)"
else
  PORT_PIDS=""
fi

for proc_id in $PORT_PIDS; do
  if [[ -n "$proc_id" ]]; then
    echo "Clearing process on port $PORT (PID $proc_id)..."
    kill "$proc_id" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
      if ! kill -0 "$proc_id" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    kill -9 "$proc_id" 2>/dev/null || true
  fi
done

if command -v lsof >/dev/null 2>&1; then
  for _ in 1 2 3 4 5; do
    if ! lsof -ti tcp:$PORT -sTCP:LISTEN >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
  if lsof -ti tcp:$PORT -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port $PORT is still in use after cleanup. Stop the conflicting process and retry."
    exit 1
  fi
fi

echo "Starting Octo Playlist Sync UI + API on http://127.0.0.1:$PORT"

if [[ "$MODE" == "prod" ]]; then
  uv run python -m waitress --host=0.0.0.0 --port="$PORT" app:app &
else
  uv run python -m flask --app app:create_app run --debug --no-reload --host 0.0.0.0 --port "$PORT" &
fi

APP_PID=$!
echo "$APP_PID" > "$LOCK_FILE"
echo "Octo Playlist Sync started with PID $APP_PID. Lock file: $LOCK_FILE"
echo "Open http://127.0.0.1:$PORT/"
wait "$APP_PID"
