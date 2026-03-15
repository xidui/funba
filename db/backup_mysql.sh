#!/usr/bin/env bash
# backup_mysql.sh — daily mysqldump for the funba nba_data database
# Retention: 7 days
# Output: <project_root>/backups/nba_data_YYYYMMDD_HHMMSS.sql.gz
#
# Usage (manual):  bash db/backup_mysql.sh
# Scheduled via:   ~/Library/LaunchAgents/app.funba.backup.plist

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BACKUP_DIR="$PROJECT_ROOT/backups"
LOG_DIR="$PROJECT_ROOT/logs"
DB_NAME="nba_data"
DB_USER="root"
DB_HOST="127.0.0.1"
RETENTION_DAYS=7

mkdir -p "$BACKUP_DIR" "$LOG_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_FILE="$BACKUP_DIR/${DB_NAME}_${TIMESTAMP}.sql.gz"
LOG_FILE="$LOG_DIR/backup_mysql.log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"
}

log "=== Starting MySQL backup ==="
log "Database : $DB_NAME"
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
