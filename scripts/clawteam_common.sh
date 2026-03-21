#!/usr/bin/env bash
set -euo pipefail

SCRIPT_SOURCE="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "$SCRIPT_SOURCE")/.." && pwd)"
WORKBENCH_PYTHON="${WORKBENCH_PYTHON:-$ROOT_DIR/.venv/bin/python}"
MAX_PRICE="${MAX_PRICE:-30}"
REPORT_LIMIT="${REPORT_LIMIT:-30}"

require_workbench_python() {
  if [[ ! -x "$WORKBENCH_PYTHON" ]]; then
    echo "Missing workbench python: $WORKBENCH_PYTHON" >&2
    exit 1
  fi
}

latest_local_date() {
  "$WORKBENCH_PYTHON" "$ROOT_DIR/roll.py" data status \
    | awk -F= '/^local_calendar_date=/{gsub(/\r/, "", $2); print $2}' \
    | tail -n 1
}

require_latest_local_date() {
  local value
  value="$(latest_local_date)"
  if [[ -z "$value" || "$value" == "None" ]]; then
    echo "Unable to determine latest local calendar date." >&2
    exit 1
  fi
  printf '%s\n' "$value"
}
