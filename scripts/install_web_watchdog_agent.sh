#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${FUNBA_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
DEPLOY_ROOT="${FUNBA_DEPLOY_ROOT:-$REPO_ROOT/.paperclip/deploy-main}"
SCRIPT_PATH="${FUNBA_WATCHDOG_SCRIPT:-$DEPLOY_ROOT/scripts/funba_web_watchdog.py}"
if [[ ! -f "$SCRIPT_PATH" ]]; then
  SCRIPT_PATH="$REPO_ROOT/scripts/funba_web_watchdog.py"
fi

PYTHON_BIN="${FUNBA_WATCHDOG_PYTHON:-$REPO_ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

LOG_DIR="${FUNBA_LOG_DIR:-$REPO_ROOT/logs}"
PLIST_DEST="$HOME/Library/LaunchAgents/app.funba.web-watchdog.plist"
LABEL="app.funba.web-watchdog"

mkdir -p "$LOG_DIR" "$(dirname "$PLIST_DEST")"

cat > "$PLIST_DEST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PYTHON_BIN</string>
    <string>$SCRIPT_PATH</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$DEPLOY_ROOT</string>
  <key>RunAtLoad</key>
  <true/>
  <key>StartInterval</key>
  <integer>30</integer>
  <key>StandardOutPath</key>
  <string>$LOG_DIR/web-watchdog-stdout.log</string>
  <key>StandardErrorPath</key>
  <string>$LOG_DIR/web-watchdog-stderr.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
PLIST

plutil -lint "$PLIST_DEST" >/dev/null
launchctl unload "$PLIST_DEST" 2>/dev/null || true
launchctl load "$PLIST_DEST"
launchctl kickstart "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true

echo "Installed $LABEL"
echo "  plist: $PLIST_DEST"
echo "  script: $SCRIPT_PATH"
echo "  logs: $LOG_DIR/web-watchdog-stdout.log"
