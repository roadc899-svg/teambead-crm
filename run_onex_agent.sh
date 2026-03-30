#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export TEAMBEAD_ONEX_AGENT_KEY="${TEAMBEAD_ONEX_AGENT_KEY:-12345}"
export TEAMBEAD_CRM_URL="${TEAMBEAD_CRM_URL:-https://crm.teambead.work}"
export PATH="/Library/Frameworks/Python.framework/Versions/3.13/bin:/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"

exec /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 "$SCRIPT_DIR/one_xbet_parser.py"
