#!/usr/bin/env bash
# test_backup.sh — verification tests for db/backup_mysql.sh
#
# Usage:  bash db/test_backup.sh
# Exit 0 = all tests passed; exit 1 = one or more tests failed.
#
# Each test runs the backup script in an isolated temp directory with a mock
# mysqldump binary, so no real database connection is needed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_SCRIPT="$SCRIPT_DIR/backup_mysql.sh"

PASS=0
FAIL=0

# Print a padded test result line
run_test() {
    local name="$1"
    local fn="$2"
    printf "  %-60s" "$name"
    local out
    if out=$("$fn" 2>&1); then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        echo "    $out" | head -5
        FAIL=$((FAIL + 1))
    fi
}

# ---------------------------------------------------------------------------
# Helper: create a temp working directory + mock mysqldump; run the script
# with overridden env vars.  Returns the tmpdir path on stdout.
# ---------------------------------------------------------------------------
_setup_tmpdir() {
    local tmpdir
    tmpdir="$(mktemp -d)"
    mkdir -p "$tmpdir/bin"
    printf '#!/bin/bash\necho "-- Fake SQL dump for test"\n' > "$tmpdir/bin/mysqldump"
    chmod +x "$tmpdir/bin/mysqldump"
    echo "$tmpdir"
}

_run_script() {
    # $1 = tmpdir, rest = extra env vars
    local tmpdir="$1"; shift
    BACKUP_DIR="$tmpdir/backups" \
    LOG_DIR="$tmpdir/logs" \
    DB_NAME="testdb" \
    DB_USER="root" \
    DB_HOST="127.0.0.1" \
    PATH="$tmpdir/bin:$PATH" \
    "$@" \
    bash "$BACKUP_SCRIPT"
}

# ---------------------------------------------------------------------------
# Test 1: Successful dump creates a .sql.gz file in BACKUP_DIR
# ---------------------------------------------------------------------------
test_successful_dump() {
    local tmpdir
    tmpdir="$(_setup_tmpdir)"
    trap "rm -rf '$tmpdir'" RETURN

    _run_script "$tmpdir" >/dev/null 2>&1

    local count
    count=$(find "$tmpdir/backups" -name "testdb_*.sql.gz" 2>/dev/null | wc -l | tr -d ' ')
    [ "$count" -ge 1 ] || { echo "Expected >=1 backup file, got $count"; return 1; }
}

# ---------------------------------------------------------------------------
# Test 2: Backup file has restrictive permissions (0600, not group/world readable)
# ---------------------------------------------------------------------------
test_file_permissions() {
    local tmpdir
    tmpdir="$(_setup_tmpdir)"
    trap "rm -rf '$tmpdir'" RETURN

    _run_script "$tmpdir" >/dev/null 2>&1

    local f perms
    f=$(find "$tmpdir/backups" -name "testdb_*.sql.gz" | head -1)
    [ -n "$f" ] || { echo "No backup file found"; return 1; }

    # stat syntax differs on macOS vs Linux
    perms=$(stat -f "%A" "$f" 2>/dev/null || stat --format="%a" "$f" 2>/dev/null)
    [ "$perms" = "600" ] || { echo "Expected permissions 600, got $perms on $f"; return 1; }
}

# ---------------------------------------------------------------------------
# Test 3: Backup directory itself has restrictive permissions (700)
# ---------------------------------------------------------------------------
test_dir_permissions() {
    local tmpdir
    tmpdir="$(_setup_tmpdir)"
    trap "rm -rf '$tmpdir'" RETURN

    _run_script "$tmpdir" >/dev/null 2>&1

    local perms
    perms=$(stat -f "%A" "$tmpdir/backups" 2>/dev/null || stat --format="%a" "$tmpdir/backups" 2>/dev/null)
    [ "$perms" = "700" ] || { echo "Expected backup dir permissions 700, got $perms"; return 1; }
}

# ---------------------------------------------------------------------------
# Test 4: Retention pruning deletes files older than RETENTION_DAYS
# ---------------------------------------------------------------------------
test_retention_pruning_removes_old() {
    local tmpdir
    tmpdir="$(_setup_tmpdir)"
    trap "rm -rf '$tmpdir'" RETURN
    mkdir -p "$tmpdir/backups"

    # Create a fake backup file timestamped 10 days ago.
    # Use 10 days (not 8) to ensure find's integer-day truncation doesn't
    # cause a borderline file to appear as only 7 full days old on macOS.
    local old_file="$tmpdir/backups/testdb_old_fake.sql.gz"
    touch "$old_file"
    # Set mtime to 10 days ago (cross-platform)
    if ! touch -t "$(date -v-10d '+%Y%m%d%H%M.%S' 2>/dev/null)" "$old_file" 2>/dev/null; then
        touch -d "10 days ago" "$old_file" 2>/dev/null || \
            { echo "SKIP (cannot set mtime, skipping on this OS)"; return 0; }
    fi

    RETENTION_DAYS=7 _run_script "$tmpdir" >/dev/null 2>&1

    [ ! -f "$old_file" ] || { echo "Old backup file was NOT pruned: $old_file"; return 1; }
}

# ---------------------------------------------------------------------------
# Test 5: Retention pruning keeps files within RETENTION_DAYS
# ---------------------------------------------------------------------------
test_retention_pruning_keeps_recent() {
    local tmpdir
    tmpdir="$(_setup_tmpdir)"
    trap "rm -rf '$tmpdir'" RETURN
    mkdir -p "$tmpdir/backups"

    # Create a fake backup file timestamped 3 days ago (should be kept)
    local recent_file="$tmpdir/backups/testdb_recent_fake.sql.gz"
    touch "$recent_file"
    if ! touch -t "$(date -v-3d '+%Y%m%d%H%M.%S' 2>/dev/null)" "$recent_file" 2>/dev/null; then
        touch -d "3 days ago" "$recent_file" 2>/dev/null || \
            { echo "SKIP (cannot set mtime, skipping on this OS)"; return 0; }
    fi

    RETENTION_DAYS=7 _run_script "$tmpdir" >/dev/null 2>&1

    [ -f "$recent_file" ] || { echo "Recent backup file was incorrectly pruned: $recent_file"; return 1; }
}

# ---------------------------------------------------------------------------
# Test 6: Failed mysqldump propagates a non-zero exit and leaves no partial file
# ---------------------------------------------------------------------------
test_failure_propagation() {
    local tmpdir
    tmpdir="$(_setup_tmpdir)"
    trap "rm -rf '$tmpdir'" RETURN

    # Override mysqldump with one that exits 1
    printf '#!/bin/bash\necho "error: connection refused" >&2\nexit 1\n' > "$tmpdir/bin/mysqldump"
    chmod +x "$tmpdir/bin/mysqldump"

    # Script should fail
    if _run_script "$tmpdir" >/dev/null 2>&1; then
        echo "Expected script to fail, but it exited 0"
        return 1
    fi

    # No partial backup file should remain
    local count
    count=$(find "$tmpdir/backups" -name "testdb_*.sql.gz" 2>/dev/null | wc -l | tr -d ' ')
    [ "$count" -eq 0 ] || { echo "Partial backup file(s) left behind after failure ($count files)"; return 1; }
}

# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------
echo "=== backup_mysql.sh verification tests ==="
echo ""
run_test "Successful dump creates .sql.gz file"                test_successful_dump
run_test "Backup file has permissions 600"                     test_file_permissions
run_test "Backup directory has permissions 700"                test_dir_permissions
run_test "Retention pruning removes files older than 7 days"  test_retention_pruning_removes_old
run_test "Retention pruning keeps files within 7 days"        test_retention_pruning_keeps_recent
run_test "Failed mysqldump propagates failure, no partial file" test_failure_propagation
echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
