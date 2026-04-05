#!/usr/bin/env bash
# install_player_bio_backfill_agent.sh — installs a launchd agent that
# incrementally fills missing player birth dates in the background.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PLIST_LABEL="app.funba.player-bio-backfill"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
SCRIPTS_DIR="$HOME/Library/Scripts/funba"
LOGS_DIR="$HOME/Library/Logs/funba"
STATE_DIR="$HOME/Library/Application Support/funba/player-bio-backfill"
PLIST_DEST="$LAUNCH_AGENTS_DIR/$PLIST_LABEL.plist"
WEB_PLIST="$HOME/Library/LaunchAgents/app.funba.web.plist"

if [ ! -f "$WEB_PLIST" ]; then
  echo "Missing $WEB_PLIST; cannot infer NBA_DB_URL"
  exit 1
fi

NBA_DB_URL="$(python3 - <<'PY'
import plistlib
from pathlib import Path
plist_path = Path.home() / "Library/LaunchAgents/app.funba.web.plist"
with plist_path.open("rb") as f:
    data = plistlib.load(f)
env = data.get("EnvironmentVariables", {})
print(env.get("NBA_DB_URL", ""))
PY
)"

if [ -z "$NBA_DB_URL" ]; then
  echo "Could not read NBA_DB_URL from $WEB_PLIST"
  exit 1
fi

mkdir -p "$LAUNCH_AGENTS_DIR" "$SCRIPTS_DIR" "$LOGS_DIR" "$STATE_DIR"

cp "$SCRIPT_DIR/player_bio_backfill.sh" "$SCRIPTS_DIR/player_bio_backfill.sh"
chmod +x "$SCRIPTS_DIR/player_bio_backfill.sh"

cat > "$PLIST_DEST" <<PLIST
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
        <string>$SCRIPTS_DIR/player_bio_backfill.sh</string>
    </array>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
        <key>PROJECT_ROOT</key>
        <string>$PROJECT_ROOT</string>
        <key>NBA_DB_URL</key>
        <string>$NBA_DB_URL</string>
        <key>LOG_DIR</key>
        <string>$LOGS_DIR</string>
        <key>STATE_DIR</key>
        <string>$STATE_DIR</string>
        <key>BATCH_LIMIT</key>
        <string>30</string>
        <key>WITH_GAMES_ONLY</key>
        <string>0</string>
    </dict>

    <key>StartInterval</key>
    <integer>600</integer>

    <key>RunAtLoad</key>
    <true/>

    <key>StandardOutPath</key>
    <string>$LOGS_DIR/player-bio-backfill-stdout.log</string>

    <key>StandardErrorPath</key>
    <string>$LOGS_DIR/player-bio-backfill-stderr.log</string>
</dict>
</plist>
PLIST

launchctl unload "$PLIST_DEST" 2>/dev/null || true
launchctl load "$PLIST_DEST"

echo "Installed $PLIST_LABEL"
echo "State dir: $STATE_DIR"
echo "Logs:"
echo "  $LOGS_DIR/player-bio-backfill-stdout.log"
echo "  $LOGS_DIR/player-bio-backfill-stderr.log"
echo "Check status with: launchctl list $PLIST_LABEL"
