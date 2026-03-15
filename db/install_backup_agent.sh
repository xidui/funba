#!/usr/bin/env bash
# install_backup_agent.sh — sets up the launchd MySQL backup job
#
# macOS TCC context: launchd agents cannot read shell scripts or list
# directories that were created in an interactive terminal session from
# ~/Documents (they carry com.apple.provenance which blocks launchd's
# file-system calls even with Aqua session type). Two fixes applied:
#
#   1. Script installed to ~/Library/Scripts/funba/ — Library paths are
#      not TCC-protected, so launchd can read and execute the script.
#
#   2. BACKUP_DIR moved to ~/Library/Application Support/funba/backups/
#      — Library paths are not TCC-protected, so launchd's `find` can
#      list the directory for 7-day retention pruning.
#      A symlink funba/backups -> the Library path makes backups visible
#      under the project directory as required.
#
# Run this script:
#   - On first install
#   - After every update to db/backup_mysql.sh
#   - After any macOS upgrade that resets permissions
#
# Usage:
#   bash db/install_backup_agent.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PLIST_LABEL="app.funba.backup"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
SCRIPTS_DIR="$HOME/Library/Scripts/funba"
LOGS_DIR="$HOME/Library/Logs/funba"
BACKUP_DIR="$HOME/Library/Application Support/funba/backups"
SYMLINK_PATH="$PROJECT_ROOT/backups"

echo "=== Funba MySQL Backup Agent Installer ==="
echo "Project root : $PROJECT_ROOT"
echo "Script runs from : $SCRIPTS_DIR/backup_mysql.sh"
echo "Backup dir   : $BACKUP_DIR"
echo "Symlink      : $SYMLINK_PATH -> $BACKUP_DIR"
echo "Logs dir     : $LOGS_DIR"
echo ""

# 1. Create required directories
mkdir -p "$SCRIPTS_DIR" "$LOGS_DIR" "$LAUNCH_AGENTS_DIR"
mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"

# 2. Install backup script to ~/Library/Scripts/funba/ (TCC-safe location)
echo "Copying backup script to $SCRIPTS_DIR/..."
cp "$SCRIPT_DIR/backup_mysql.sh" "$SCRIPTS_DIR/backup_mysql.sh"
chmod +x "$SCRIPTS_DIR/backup_mysql.sh"
echo "  Done."

# 3. Migrate any existing backups from the old Documents-based location
OLD_BACKUP_DIR="$PROJECT_ROOT/backups"
if [ -d "$OLD_BACKUP_DIR" ] && [ ! -L "$OLD_BACKUP_DIR" ]; then
    shopt -s nullglob
    OLD_FILES=("$OLD_BACKUP_DIR"/*.sql.gz)
    if [ ${#OLD_FILES[@]} -gt 0 ]; then
        echo "Migrating ${#OLD_FILES[@]} existing backup(s) to $BACKUP_DIR/..."
        for f in "${OLD_FILES[@]}"; do
            mv "$f" "$BACKUP_DIR/"
            echo "  Moved: $(basename "$f")"
        done
    fi
    rmdir "$OLD_BACKUP_DIR" 2>/dev/null && echo "Removed old backups dir." || \
        echo "Old backups dir not empty (non-sql.gz files remain); leaving it."
fi

# 4. Create symlink funba/backups -> ~/Library/Application Support/funba/backups
#    so backups appear under the project directory as required.
if [ -L "$SYMLINK_PATH" ]; then
    EXISTING_TARGET="$(readlink "$SYMLINK_PATH")"
    if [ "$EXISTING_TARGET" != "$BACKUP_DIR" ]; then
        echo "Updating symlink $SYMLINK_PATH -> $BACKUP_DIR ..."
        ln -sf "$BACKUP_DIR" "$SYMLINK_PATH"
    else
        echo "Symlink already correct: $SYMLINK_PATH -> $BACKUP_DIR"
    fi
elif [ -e "$SYMLINK_PATH" ]; then
    echo "WARNING: $SYMLINK_PATH exists and is not a symlink — skipping symlink creation."
    echo "  Remove it manually and re-run to fix."
else
    echo "Creating symlink $SYMLINK_PATH -> $BACKUP_DIR ..."
    ln -s "$BACKUP_DIR" "$SYMLINK_PATH"
    echo "  Done."
fi

# 5. Write plist to ~/Library/LaunchAgents/
PLIST_DEST="$LAUNCH_AGENTS_DIR/$PLIST_LABEL.plist"
echo "Writing plist to $PLIST_DEST..."
cat > "$PLIST_DEST" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$PLIST_LABEL</string>

    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPTS_DIR/backup_mysql.sh</string>
    </array>

    <key>EnvironmentVariables</key>
    <dict>
        <!-- Homebrew bin needed for mysqldump on Apple Silicon -->
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
        <!-- Library path: not TCC-restricted, so launchd find/retention works -->
        <key>BACKUP_DIR</key>
        <string>$BACKUP_DIR</string>
        <key>LOG_DIR</key>
        <string>$LOGS_DIR</string>
    </dict>

    <!-- Run daily at 02:00 local time -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>$LOGS_DIR/backup-stdout.log</string>

    <key>StandardErrorPath</key>
    <string>$LOGS_DIR/backup-stderr.log</string>

    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
PLIST
echo "  Done."

# 6. Reload launchd job
echo "Reloading launchd job..."
launchctl unload "$PLIST_DEST" 2>/dev/null || true
launchctl load "$PLIST_DEST"
echo "  Done."

echo ""
echo "=== Installation complete ==="
echo ""
echo "Verify the job is loaded:"
echo "  launchctl list $PLIST_LABEL"
echo ""
echo "Run a test backup now:"
echo "  launchctl kickstart gui/\$(id -u)/$PLIST_LABEL"
echo "  sleep 180 && tail -20 $LOGS_DIR/backup-stdout.log"
echo ""
echo "Backups location (via project symlink):"
echo "  ls -lh $SYMLINK_PATH/"
echo ""
echo "Logs:"
echo "  $LOGS_DIR/backup-stdout.log"
echo "  $LOGS_DIR/backup-stderr.log"
echo "  $LOGS_DIR/backup_mysql.log"
echo ""
echo "NOTE: After any update to db/backup_mysql.sh, re-run this installer"
echo "      to sync changes to $SCRIPTS_DIR/backup_mysql.sh"
