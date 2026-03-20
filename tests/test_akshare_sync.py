from tempfile import TemporaryDirectory
from pathlib import Path
from unittest import TestCase

import pandas as pd

from qlib_assistant_refactor.akshare_sync import AkshareDailySync, normalize_index_daily, normalize_stock_daily
from qlib_assistant_refactor.config import AppConfig


class AkshareSyncTests(TestCase):
    def test_normalize_stock_daily(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "日期": "2026-03-20",
                    "开盘": 10.0,
                    "收盘": 10.5,
                    "最高": 10.6,
                    "最低": 9.9,
                    "成交量": 12345,
                }
            ]
        )

        result = normalize_stock_daily(raw, symbol="SZ000001")
        self.assertEqual(
            result.columns.tolist(),
            ["date", "symbol", "open", "close", "high", "low", "volume", "factor"],
        )
        self.assertEqual(result.iloc[0]["symbol"], "SZ000001")
        self.assertEqual(result.iloc[0]["factor"], 1.0)

    def test_normalize_index_daily_sets_volume_zero(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "日期": "2026-03-20",
                    "开盘": 4000.0,
                    "收盘": 4010.0,
                    "最高": 4020.0,
                    "最低": 3990.0,
                }
            ]
        )

        result = normalize_index_daily(raw, symbol="SH000300")
        self.assertEqual(result.iloc[0]["symbol"], "SH000300")
        self.assertEqual(result.iloc[0]["volume"], 0.0)

    def test_normalize_stock_daily_keeps_name_when_provided(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "日期": "2026-03-20",
                    "开盘": 10.0,
                    "收盘": 10.5,
                    "最高": 10.6,
                    "最低": 9.9,
                    "成交量": 12345,
                }
            ]
        )

        result = normalize_stock_daily(raw, symbol="SZ000001", name="平安银行")
        self.assertEqual(result.iloc[0]["name"], "平安银行")

    def test_load_cached_symbol_csv_filters_requested_window(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sync = AkshareDailySync(AppConfig(sync_dir=tmpdir))
            csv_root = Path(tmpdir)
            pd.DataFrame(
                [
                    {"date": "2026-03-19", "symbol": "SZ000001", "open": 10, "close": 10, "high": 10, "low": 10, "volume": 1, "factor": 1},
                    {"date": "2026-03-20", "symbol": "SZ000001", "open": 11, "close": 11, "high": 11, "low": 11, "volume": 1, "factor": 1},
                ]
            ).to_csv(csv_root / "SZ000001.csv", index=False)

            result = sync._load_cached_symbol_csv(
                csv_dir=csv_root,
                symbol="SZ000001",
                start_date="2026-03-20",
                end_date="2026-03-20",
            )
            self.assertEqual(result["date"].tolist(), ["2026-03-20"])

    def test_merge_with_existing_csv_preserves_history(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sync = AkshareDailySync(AppConfig(sync_dir=tmpdir))
            csv_root = Path(tmpdir)
            pd.DataFrame(
                [
                    {"date": "2026-03-19", "symbol": "SZ000001", "open": 10, "close": 10, "high": 10, "low": 10, "volume": 1, "factor": 1},
                ]
            ).to_csv(csv_root / "SZ000001.csv", index=False)
            fresh = pd.DataFrame(
                [
                    {"date": "2026-03-20", "symbol": "SZ000001", "open": 11, "close": 11, "high": 11, "low": 11, "volume": 1, "factor": 1},
                ]
            )

            result = sync._merge_with_existing_csv(csv_dir=csv_root, symbol="SZ000001", fresh=fresh)
            self.assertEqual(result["date"].tolist(), ["2026-03-19", "2026-03-20"])

    def test_normalize_sse180_constituents(self) -> None:
        sync = AkshareDailySync(AppConfig())
        raw = pd.DataFrame(
            [
                {"成分券代码": "600000", "成分券名称": "浦发银行"},
                {"成分券代码": "600009", "成分券名称": "上海机场"},
            ]
        )
        result = sync._normalize_sse180_constituents(raw)
        self.assertEqual(result["instrument"].tolist(), ["SH600000", "SH600009"])
        self.assertEqual(result["name"].tolist(), ["浦发银行", "上海机场"])

    def test_write_universe_file_creates_sse180_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            provider_uri = Path(tmpdir) / "qlib"
            (provider_uri / "calendars").mkdir(parents=True, exist_ok=True)
            (provider_uri / "calendars" / "day.txt").write_text("2026-03-19\n2026-03-20\n", encoding="utf-8")
            sync = AkshareDailySync(AppConfig(provider_uri=str(provider_uri)))
            path = sync._write_universe_file(
                universe_name="sse180",
                symbols=["SH600000", "SH600009"],
                latest_date="2026-03-20",
            )
            self.assertTrue(path.exists())
            rows = path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(rows[0], "SH600000\t2026-03-19\t2026-03-20")
