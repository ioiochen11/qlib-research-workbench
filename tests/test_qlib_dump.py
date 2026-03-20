from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd

from qlib_assistant_refactor.qlib_dump import dump_csv_folder_to_qlib


class QlibDumpTests(TestCase):
    def test_dump_csv_folder_creates_qlib_structure(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            csv_dir = root / "csv"
            qlib_dir = root / "qlib"
            csv_dir.mkdir()

            df = pd.DataFrame(
                [
                    {
                        "date": "2026-03-19",
                        "symbol": "SZ000001",
                        "open": 10.0,
                        "close": 10.5,
                        "high": 10.6,
                        "low": 9.9,
                        "volume": 1000,
                        "factor": 1.0,
                    },
                    {
                        "date": "2026-03-20",
                        "symbol": "SZ000001",
                        "open": 10.2,
                        "close": 10.7,
                        "high": 10.8,
                        "low": 10.1,
                        "volume": 1100,
                        "factor": 1.0,
                    },
                ]
            )
            df.to_csv(csv_dir / "SZ000001.csv", index=False)

            summary = dump_csv_folder_to_qlib(csv_dir, qlib_dir)

            self.assertEqual(summary.mode, "all")
            self.assertTrue((qlib_dir / "calendars" / "day.txt").exists())
            self.assertTrue((qlib_dir / "instruments" / "all.txt").exists())
            self.assertTrue((qlib_dir / "features" / "sz000001" / "close.day.bin").exists())

    def test_dump_csv_folder_updates_existing_calendar(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            csv_dir = root / "csv"
            qlib_dir = root / "qlib"
            csv_dir.mkdir()

            first = pd.DataFrame(
                [
                    {
                        "date": "2026-03-19",
                        "symbol": "SZ000001",
                        "open": 10.0,
                        "close": 10.5,
                        "high": 10.6,
                        "low": 9.9,
                        "volume": 1000,
                        "factor": 1.0,
                    }
                ]
            )
            first.to_csv(csv_dir / "SZ000001.csv", index=False)
            dump_csv_folder_to_qlib(csv_dir, qlib_dir)

            second = pd.DataFrame(
                [
                    {
                        "date": "2026-03-19",
                        "symbol": "SZ000001",
                        "open": 10.0,
                        "close": 10.5,
                        "high": 10.6,
                        "low": 9.9,
                        "volume": 1000,
                        "factor": 1.0,
                    },
                    {
                        "date": "2026-03-20",
                        "symbol": "SZ000001",
                        "open": 10.2,
                        "close": 10.7,
                        "high": 10.8,
                        "low": 10.1,
                        "volume": 1100,
                        "factor": 1.0,
                    },
                ]
            )
            second.to_csv(csv_dir / "SZ000001.csv", index=False)
            summary = dump_csv_folder_to_qlib(csv_dir, qlib_dir)

            calendars = (qlib_dir / "calendars" / "day.txt").read_text(encoding="utf-8").splitlines()
            self.assertEqual(summary.mode, "update")
            self.assertEqual(calendars[-1], "2026-03-20")
