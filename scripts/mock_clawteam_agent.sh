#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="${CLAWTEAM_MOCK_LOG_DIR:-/tmp/clawteam-mock-logs}"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)-$$"
LOG_FILE="$LOG_DIR/$STAMP.log"

{
  echo "pwd=$(pwd)"
  echo "args_count=$#"
  i=1
  for arg in "$@"; do
    echo "arg_${i}=$arg"
    i=$((i + 1))
  done
  echo "stdin_begin"
  cat || true
  echo "stdin_end"
} >"$LOG_FILE"

echo "mock_agent_log=$LOG_FILE"
