from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd

from qlib_assistant_refactor.config import AppConfig
from qlib_assistant_refactor.feed_sync import FeedSyncManager


class FeedSyncTests(TestCase):
    def test_fetch_sina_quote_snapshot_parses_stock_row(self) -> None:
        manager = FeedSyncManager(AppConfig())

        class DummyResponse:
            text = (
                'var hq_str_sz000333="美的集团,73.950,75.120,73.370,73.970,72.350,73.360,73.370,46200800,'
                '3377621417.850,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-03-23,15:00:00,00";'
            )

            def raise_for_status(self) -> None:
                return None

        import requests

        original_get = requests.get
        requests.get = lambda *args, **kwargs: DummyResponse()
        try:
            frame = manager._fetch_sina_quote_snapshot("SZ000333", "2026-03-23")
        finally:
            requests.get = original_get

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["date"], "2026-03-23")
        self.assertEqual(frame.iloc[0]["symbol"], "SZ000333")
        self.assertEqual(frame.iloc[0]["name"], "美的集团")
        self.assertAlmostEqual(float(frame.iloc[0]["open"]), 73.95, places=2)
        self.assertAlmostEqual(float(frame.iloc[0]["close"]), 73.37, places=2)
        self.assertAlmostEqual(float(frame.iloc[0]["volume"]), 46200800.0, places=1)

    def test_fetch_tencent_quote_snapshot_normalizes_equity_volume(self) -> None:
        manager = FeedSyncManager(AppConfig())

        class DummyResponse:
            text = (
                'v_sz000333="51~美的集团~000333~73.37~75.12~73.95~462008~220227~241781~73.36~24~73.35~37~73.34'
                '~6~73.33~188~73.32~1~73.37~31~73.38~50~73.39~5~73.40~109~73.41~7~~20260323160739~-1.75~-2.33'
                '~73.97~72.35~73.37/462008/3377621418~462008~337762~0.67";'
            )

            def raise_for_status(self) -> None:
                return None

        import requests

        original_get = requests.get
        requests.get = lambda *args, **kwargs: DummyResponse()
        try:
            frame = manager._fetch_tencent_quote_snapshot("SZ000333", "2026-03-23")
        finally:
            requests.get = original_get

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["date"], "2026-03-23")
        self.assertAlmostEqual(float(frame.iloc[0]["open"]), 73.95, places=2)
        self.assertAlmostEqual(float(frame.iloc[0]["close"]), 73.37, places=2)
        self.assertAlmostEqual(float(frame.iloc[0]["high"]), 73.97, places=2)
        self.assertAlmostEqual(float(frame.iloc[0]["low"]), 72.35, places=2)
        self.assertAlmostEqual(float(frame.iloc[0]["volume"]), 46200800.0, places=1)

    def test_supplement_market_frame_with_snapshot_adds_latest_row(self) -> None:
        manager = FeedSyncManager(AppConfig())
        manager._should_use_quote_snapshot = lambda as_of_date: True
        manager._fetch_quote_snapshot = lambda symbol, as_of_date, source_name: pd.DataFrame(
            [
                {
                    "date": "2026-03-23",
                    "symbol": symbol,
                    "name": "中国石化",
                    "open": 6.08,
                    "close": 6.04,
                    "high": 6.11,
                    "low": 5.93,
                    "volume": 180411500.0,
                    "factor": 1.0,
                }
            ]
        )
        frame = pd.DataFrame(
            [
                {
                    "date": "2026-03-20",
                    "symbol": "SH600028",
                    "name": "中国石化",
                    "open": 6.08,
                    "close": 6.08,
                    "high": 6.11,
                    "low": 5.93,
                    "volume": 120000000.0,
                    "factor": 1.0,
                }
            ]
        )

        supplemented = manager._supplement_market_frame_with_snapshot(
            frame=frame,
            symbol="SH600028",
            end_date="2026-03-23",
            source_name="akshare",
        )

        self.assertEqual(len(supplemented), 2)
        self.assertEqual(list(supplemented["date"]), ["2026-03-20", "2026-03-23"])

    def test_supplement_market_frame_with_snapshot_overrides_existing_latest_row(self) -> None:
        manager = FeedSyncManager(AppConfig())
        manager._should_use_quote_snapshot = lambda as_of_date: True
        manager._fetch_quote_snapshot = lambda symbol, as_of_date, source_name: pd.DataFrame(
            [
                {
                    "date": "2026-03-23",
                    "symbol": symbol,
                    "name": "浦发银行",
                    "open": 10.23,
                    "close": 9.86,
                    "high": 10.23,
                    "low": 9.85,
                    "volume": 93394500.0,
                    "factor": 1.0,
                }
            ]
        )
        frame = pd.DataFrame(
            [
                {
                    "date": "2026-03-23",
                    "symbol": "SH600000",
                    "name": "浦发银行",
                    "open": 10.23,
                    "close": 9.86,
                    "high": 10.23,
                    "low": 9.85,
                    "volume": 933945.0,
                    "factor": 1.0,
                }
            ]
        )

        supplemented = manager._supplement_market_frame_with_snapshot(
            frame=frame,
            symbol="SH600000",
            end_date="2026-03-23",
            source_name="akshare",
        )

        self.assertEqual(len(supplemented), 1)
        self.assertAlmostEqual(float(supplemented.iloc[0]["volume"]), 93394500.0, places=1)

    def test_prefer_cached_market_frame_uses_same_day_cache(self) -> None:
        manager = FeedSyncManager(AppConfig())
        cached = pd.DataFrame(
            [
                {
                    "date": "2026-03-23",
                    "symbol": "SH600000",
                    "open": 10.23,
                    "close": 9.86,
                    "high": 10.23,
                    "low": 9.85,
                    "volume": 93394504.0,
                    "factor": 1.0,
                }
            ]
        )

        self.assertTrue(manager._prefer_cached_market_frame(cached, "2026-03-23"))

    def test_reuse_validated_market_cache_returns_summary_when_all_symbols_ready(self) -> None:
        with TemporaryDirectory() as tmpdir:
            provider_uri = Path(tmpdir) / "provider"
            gold_dir = Path(tmpdir) / "sync" / "gold" / "market" / "validated_daily"
            gold_dir.mkdir(parents=True, exist_ok=True)
            for symbol in ["SH600000", "SH000300"]:
                pd.DataFrame(
                    [
                        {
                            "date": "2026-03-23",
                            "symbol": symbol,
                            "name": symbol,
                            "open": 1.0,
                            "close": 1.0,
                            "high": 1.0,
                            "low": 1.0,
                            "volume": 1.0,
                            "factor": 1.0,
                        }
                    ]
                ).to_csv(gold_dir / f"{symbol}.csv", index=False)
            manager = FeedSyncManager(
                AppConfig(provider_uri=str(provider_uri), sync_dir=str(Path(tmpdir) / "sync"), benchmark_symbol="SH000300")
            )
            manager._should_use_quote_snapshot = lambda as_of_date: True

            summary = manager._reuse_validated_market_cache(
                gold_dir=gold_dir,
                symbols=["SH600000", "SH000300"],
                as_of_date="2026-03-23",
            )

            self.assertIsNotNone(summary)
            assert summary is not None
            self.assertTrue(summary.eligible_for_daily_run)
            self.assertEqual(summary.validation_status, "passed")
            self.assertTrue(summary.manifest_path.exists())

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

    def test_build_fundamental_row_derives_richer_tags_and_pe(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sync_dir = Path(tmpdir)
            daily_dir = sync_dir / "akshare_daily"
            daily_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {"date": "2026-03-20", "close": 24.0},
                ]
            ).to_csv(daily_dir / "SH600000.csv", index=False)

            manager = FeedSyncManager(AppConfig(sync_dir=tmpdir))
            row = manager._build_fundamental_row(
                instrument="SH600000",
                as_of_date="2026-03-20",
                info={"股票简称": "浦发银行"},
                report={
                    "report_period": "20251231",
                    "report_source": "eastmoney_yjkb",
                    "营业收入同比增长": "18",
                    "净利润同比增长": "26",
                    "净资产收益率": "16",
                    "每股收益": "2",
                    "销售毛利率": "32",
                },
            )

            self.assertEqual(row["fundamental_risk_tag"], "营收增长较快、利润增长较快")
            self.assertEqual(row["valuation_tag"], "估值中性、质地较好")
            self.assertIn("估算PE 12.00", row["fundamental_summary"])

    def test_event_summaries_generate_richer_labels(self) -> None:
        manager = FeedSyncManager(AppConfig())
        notice_df = pd.DataFrame(
            [
                {"代码": "600000", "公告标题": "浦发银行关于回购股份进展公告"},
                {"代码": "600000", "公告标题": "浦发银行签署重大合同的公告"},
            ]
        )
        news_df = pd.DataFrame(
            [
                {"instrument": "SH600000", "title": "浦发银行订单增长 创新高", "published_at": "2026-03-20", "source_name": "东方财富"},
                {"instrument": "SH600000", "title": "浦发银行被问询 业绩下滑风险引关注", "published_at": "2026-03-20", "source_name": "东方财富"},
            ]
        )

        notice = manager._summarize_notice_events(notice_df, symbols=["SH600000"])
        news = manager._summarize_news_events(news_df, symbols=["SH600000"])
        event_tag = manager._merge_event_risk(notice["SH600000"], news["SH600000"])

        self.assertIn("公告标签 回购、重大合同", notice["SH600000"]["notice_summary"])
        self.assertIn("情绪 中性", news["SH600000"]["news_summary"])
        self.assertEqual(event_tag, "公告多空交织")
