#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv-clawteam}"

if [[ -n "${PYTHON_BIN:-}" ]]; then
  PYTHON_CANDIDATE="$PYTHON_BIN"
elif command -v python3.11 >/dev/null 2>&1; then
  PYTHON_CANDIDATE="$(command -v python3.11)"
elif command -v python3.12 >/dev/null 2>&1; then
  PYTHON_CANDIDATE="$(command -v python3.12)"
else
  echo "Python 3.11+ is required for ClawTeam." >&2
  exit 1
fi

"$PYTHON_CANDIDATE" -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install -U pip
"$VENV_DIR/bin/pip" install clawteam
"$VENV_DIR/bin/clawteam" --version
