from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qlib_assistant_refactor.roll_cli import main as roll_main


def main() -> int:
    return roll_main(["data", "qlib-check"])


if __name__ == "__main__":
    raise SystemExit(main())
