#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/clawteam_common.sh"
require_workbench_python

cd "$ROOT_DIR"
AS_OF_DATE="$(require_latest_local_date)"
if [ "$#" -gt 0 ]; then
  "$WORKBENCH_PYTHON" roll.py data sync-events "$@"
else
  "$WORKBENCH_PYTHON" roll.py data sync-events --date "$AS_OF_DATE"
fi
