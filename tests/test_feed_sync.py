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
