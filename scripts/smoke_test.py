from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qlib_assistant_refactor.cli import main as cli_main


def main() -> int:
    return cli_main(["probe"])


if __name__ == "__main__":
    raise SystemExit(main())
