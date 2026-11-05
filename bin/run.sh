#!/usr/bin/env bash
# 前台启动单个服务：bin/run.sh <world|game|gateway|robot> [程序参数...]
set -euo pipefail
BASE="$(cd "$(dirname "$0")/.." && pwd)"
APP="${1:?usage: run.sh <world|game|gateway|robot> [args...]}"
shift
JAR="$BASE/slg-$APP/target/slg-$APP-all.jar"
[ -f "$JAR" ] || { echo "[ERROR] $JAR not found, run bin/build.sh first" >&2; exit 1; }
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
JAVA_OPTS="${JAVA_OPTS:--Xms256m -Xmx512m -XX:+UseG1GC}"
cd "$BASE"
# shellcheck disable=SC2086
exec "$JAVA" $JAVA_OPTS -Dfile.encoding=UTF-8 -Dslg.app="$APP" -jar "$JAR" "$@"
