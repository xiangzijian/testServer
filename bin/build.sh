#!/usr/bin/env bash
# 构建全部模块：bin/build.sh [-DskipTests]
set -euo pipefail
cd "$(dirname "$0")/.."
exec mvn -B clean package "$@"
