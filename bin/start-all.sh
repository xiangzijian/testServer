#!/usr/bin/env bash
# 后台启动 world、game、gateway；stdout 写 logs/<app>.out，pid 写 logs/<app>.pid
set -euo pipefail
BASE="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$BASE/logs"
for app in world game gateway; do
  nohup "$BASE/bin/run.sh" "$app" > "$BASE/logs/$app.out" 2>&1 &
  echo $! > "$BASE/logs/$app.pid"
  echo "started $app pid=$(cat "$BASE/logs/$app.pid")"
  if [ "$app" = game ]; then sleep 2; fi
done
echo "run bin/run.sh robot to test ping/pong; bin/stop-all.sh to stop."
