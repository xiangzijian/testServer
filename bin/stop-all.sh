#!/usr/bin/env bash
set -uo pipefail
BASE="$(cd "$(dirname "$0")/.." && pwd)"
for app in gateway game world; do
  f="$BASE/logs/$app.pid"
  if [ -f "$f" ]; then
    kill "$(cat "$f")" 2>/dev/null && echo "stopped $app"
    rm -f "$f"
  fi
done
