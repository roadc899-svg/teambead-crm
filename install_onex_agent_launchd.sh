#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$PLIST_DIR/work.teambead.onex-agent.plist"
LOG_DIR="$SCRIPT_DIR/uploaded_data/onexbet_runtime/logs"

mkdir -p "$PLIST_DIR"
mkdir -p "$LOG_DIR"

USER_ID="$(id -u)"

cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>work.teambead.onex-agent</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/zsh</string>
        <string>$SCRIPT_DIR/run_onex_agent.sh</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$SCRIPT_DIR</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/onex-agent.out.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/onex-agent.err.log</string>
</dict>
</plist>
PLIST

launchctl bootout "gui/$USER_ID" "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$USER_ID" "$PLIST_PATH"
launchctl kickstart -k "gui/$USER_ID/work.teambead.onex-agent"
echo "Installed launchd agent: $PLIST_PATH"
