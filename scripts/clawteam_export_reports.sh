#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/clawteam_common.sh"
require_workbench_python

cd "$ROOT_DIR"
AS_OF_DATE="$(require_latest_local_date)"
REPORT_OUTPUT="$("$WORKBENCH_PYTHON" roll.py model report "$@")"
SELECTION_DIR="$(printf '%s\n' "$REPORT_OUTPUT" | awk -F= '/^saved=/{print $2}' | tail -n 1)"

if [[ -z "$SELECTION_DIR" ]]; then
  echo "Failed to resolve selection directory from model report output." >&2
  exit 1
fi

CSV_PATH="$("$WORKBENCH_PYTHON" roll.py model save-recommendations --date "$AS_OF_DATE" --limit "$REPORT_LIMIT" --max-price "$MAX_PRICE" --selection-dir "$SELECTION_DIR" | awk -F= '/^saved=/{print $2}' | tail -n 1)"
MD_PATH="$("$WORKBENCH_PYTHON" roll.py model save-recommendation-report --date "$AS_OF_DATE" --limit "$REPORT_LIMIT" --max-price "$MAX_PRICE" --selection-dir "$SELECTION_DIR" | awk -F= '/^saved=/{print $2}' | tail -n 1)"
HTML_PATH="$("$WORKBENCH_PYTHON" roll.py model save-recommendation-html --date "$AS_OF_DATE" --limit "$REPORT_LIMIT" --max-price "$MAX_PRICE" --selection-dir "$SELECTION_DIR" | awk -F= '/^saved=/{print $2}' | tail -n 1)"
SPOTLIGHT_MD_PATH="$("$WORKBENCH_PYTHON" roll.py model save-recommendation-spotlight --date "$AS_OF_DATE" --limit 3 --max-price "$MAX_PRICE" --selection-dir "$SELECTION_DIR" | awk -F= '/^saved=/{print $2}' | tail -n 1)"
SPOTLIGHT_HTML_PATH="$("$WORKBENCH_PYTHON" roll.py model save-recommendation-spotlight-html --date "$AS_OF_DATE" --limit 3 --max-price "$MAX_PRICE" --selection-dir "$SELECTION_DIR" | awk -F= '/^saved=/{print $2}' | tail -n 1)"

ANALYSIS_DIR="$HOME/.qlibAssistant/analysis"
mkdir -p "$ANALYSIS_DIR"
cp -f "$CSV_PATH" "$ANALYSIS_DIR/latest_recommendations.csv"
cp -f "$MD_PATH" "$ANALYSIS_DIR/latest_recommendation_report.md"
cp -f "$HTML_PATH" "$ANALYSIS_DIR/latest_recommendation_report.html"
cp -f "$SPOTLIGHT_MD_PATH" "$ANALYSIS_DIR/latest_recommendation_spotlight.md"
cp -f "$SPOTLIGHT_HTML_PATH" "$ANALYSIS_DIR/latest_recommendation_spotlight.html"

printf 'selection_dir=%s\n' "$SELECTION_DIR"
printf 'latest_html=%s\n' "$ANALYSIS_DIR/latest_recommendation_report.html"
