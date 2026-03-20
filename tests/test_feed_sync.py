from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd

from qlib_assistant_refactor.config import AppConfig
from qlib_assistant_refactor.feed_sync import FeedSyncManager


class FeedSyncTests(TestCase):
    def test_validate_market_pair_allows_benchmark_backup_only(self) -> None:
        manager = FeedSyncManager(AppConfig(benchmark_symbol="SH000300"))
        backup = pd.DataFrame(
            [
                {
                    "date": "2026-03-20",
                    "symbol": "SH000300",
                    "open": 4600.0,
                    "close": 4567.0,
                    "high": 4628.0,
                    "low": 4563.0,
                    "volume": 0.0,
                    "factor": 1.0,
                }
            ]
        )
        validated, errors = manager._validate_market_pair(
            symbol="SH000300",
            primary=pd.DataFrame(),
            backup=backup,
            start_date="2026-03-20",
            end_date="2026-03-20",
        )
        self.assertEqual(len(validated), 1)
        self.assertIn("benchmark_backup_only:SH000300", errors)

    def test_sync_fundamentals_uses_equity_symbols_only_for_coverage(self) -> None:
        with TemporaryDirectory() as tmpdir:
            provider_uri = Path(tmpdir) / "provider"
            instruments_dir = provider_uri / "instruments"
            instruments_dir.mkdir(parents=True, exist_ok=True)
            (instruments_dir / "csi300.txt").write_text(
                "SH000300\t2026-03-19\t2026-03-20\nSH600000\t2026-03-19\t2026-03-20\nSZ000001\t2026-03-19\t2026-03-20\n",
                encoding="utf-8",
            )
            manager = FeedSyncManager(AppConfig(provider_uri=str(provider_uri), sync_dir=tmpdir, sync_universe="csi300"))
            manager._collect_report_frames = lambda as_of_date: []
            manager._build_latest_report_lookup = lambda frames: {}
            manager._fetch_individual_info = lambda instrument, raw_dir: {"股票简称": instrument}

            summary = manager.sync_fundamentals(as_of_date="2026-03-20")

            self.assertEqual(summary.record_count, 2)
            self.assertEqual(summary.coverage_ratio, 1.0)
            self.assertTrue(summary.eligible_for_daily_run)
