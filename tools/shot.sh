#!/usr/bin/env bash
# Screenshot a web UI with headless Chromium.
# usage: shot.sh URL OUT.png [WIDTH] [HEIGHT] [WAIT_MS]
set -euo pipefail
URL=$1 OUT=$2 W=${3:-1400} H=${4:-1000} WAIT=${5:-8000}
BIN=$(ls ~/.cache/ms-playwright/chromium_headless_shell-*/*/headless_shell 2>/dev/null | tail -1)
[ -x "$BIN" ] || BIN=$(command -v chromium-browser)
mkdir -p "$(dirname "$OUT")"
timeout 90 "$BIN" --no-sandbox --hide-scrollbars --window-size="$W,$H" \
  --virtual-time-budget="$WAIT" --screenshot="$(realpath -m "$OUT")" "$URL" >/dev/null 2>&1
echo "saved $OUT"
