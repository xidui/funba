#!/usr/bin/env bash
# backup_mysql.sh — daily mysqldump for the funba nba_data database
# Retention: 7 days
# Output: <project_root>/backups/nba_data_YYYYMMDD_HHMMSS.sql.gz
#
# Usage (manual):  bash db/backup_mysql.sh
# Scheduled via:   ~/Library/LaunchAgents/app.funba.backup.plist
#
# Env var overrides (useful for testing or multi-env setups):
#   BACKUP_DIR        — override backup output directory
#   LOG_DIR           — override log directory
#   DB_NAME           — database name          (default: nba_data)
#   DB_USER           — MySQL user             (default: derived from NBA_DB_URL, else root)
#   DB_HOST           — MySQL host             (default: derived from NBA_DB_URL, else 127.0.0.1)
#   RETENTION_DAYS    — days to keep backups   (default: 7)

# Set restrictive umask before any file creation so no backup file is
# ever created world- or group-readable.
umask 077

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BACKUP_DIR="${BACKUP_DIR:-$PROJECT_ROOT/backups}"
LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/logs}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"

# --- Derive DB connection settings ---
# Prefer explicit env vars; fall back to parsing NBA_DB_URL; then use defaults.
# NBA_DB_URL format: mysql+pymysql://user[:pass]@host[:port]/dbname
_derive_from_url() {
    local url="${NBA_DB_URL:-}"
    [ -z "$url" ] && return
    local rest="${url#*://}"          # strip scheme
    local userinfo="${rest%%@*}"      # user[:pass]
    local hostdb="${rest#*@}"         # host[:port]/db
    local host="${hostdb%%/*}"        # host[:port]
    host="${host%%:*}"                # strip port
    local user="${userinfo%%:*}"      # strip password
    local db="${hostdb#*/}"           # dbname
    printf "%s %s %s" "$user" "$host" "$db"
}

_URL_DEFAULTS="$(_derive_from_url 2>/dev/null || true)"
DB_USER="${DB_USER:-${_URL_DEFAULTS%% *}}"
_tmp="${_URL_DEFAULTS#* }"
DB_HOST="${DB_HOST:-${_tmp%% *}}"
DB_NAME="${DB_NAME:-${_tmp##* }}"
# Final fallbacks
DB_USER="${DB_USER:-root}"
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_NAME="${DB_NAME:-nba_data}"

# --- Prepare directories ---
mkdir -p "$BACKUP_DIR" "$LOG_DIR"
chmod 700 "$BACKUP_DIR"   # restrict directory access as well

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_FILE="$BACKUP_DIR/${DB_NAME}_${TIMESTAMP}.sql.gz"
LOG_FILE="$LOG_DIR/backup_mysql.log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"
}

# Remove partial backup file if the script exits unexpectedly mid-dump
cleanup_on_err() {
    if [ -f "${BACKUP_FILE:-}" ]; then
        rm -f "$BACKUP_FILE"
        log "ERROR: removed partial backup file: $BACKUP_FILE"
    fi
}
trap cleanup_on_err ERR

log "=== Starting MySQL backup ==="
log "Database : $DB_NAME"
log "Host     : $DB_HOST"
log "Output   : $BACKUP_FILE"

mysqldump \
    --host="$DB_HOST" \
    --user="$DB_USER" \
    --single-transaction \
    --quick \
    --lock-tables=false \
    "$DB_NAME" \
    | gzip -9 > "$BACKUP_FILE"

SIZE="$(du -sh "$BACKUP_FILE" | cut -f1)"
log "Backup complete: $BACKUP_FILE ($SIZE)"

# Prune backups older than RETENTION_DAYS
log "Pruning backups older than $RETENTION_DAYS days..."
PRUNED=0
while IFS= read -r old_file; do
    rm -f "$old_file"
    log "  Deleted: $old_file"
    PRUNED=$((PRUNED + 1))
done < <(find "$BACKUP_DIR" -maxdepth 1 -name "${DB_NAME}_*.sql.gz" -mtime +"$RETENTION_DAYS")

log "Pruned $PRUNED old backup(s)"
log "=== Backup finished ==="
