#!/usr/bin/env bash
# Periodic player birth-date backfill job wrapper for launchd.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:?PROJECT_ROOT is required}"
NBA_DB_URL="${NBA_DB_URL:?NBA_DB_URL is required}"
LOG_DIR="${LOG_DIR:-$HOME/Library/Logs/funba}"
STATE_DIR="${STATE_DIR:-$HOME/Library/Application Support/funba/player-bio-backfill}"
BATCH_LIMIT="${BATCH_LIMIT:-30}"
WITH_GAMES_ONLY="${WITH_GAMES_ONLY:-0}"

mkdir -p "$LOG_DIR" "$STATE_DIR"

LOCK_DIR="$STATE_DIR/lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "player bio backfill already running; skipping this interval"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

cd "$PROJECT_ROOT"

ARGS=(
  --state-path "$STATE_DIR/state.json"
  --limit "$BATCH_LIMIT"
)
if [ "$WITH_GAMES_ONLY" = "1" ]; then
  ARGS+=(--with-games-only)
fi

exec "$PROJECT_ROOT/.venv/bin/python" -u -m db.player_bio_backfill_job "${ARGS[@]}"
