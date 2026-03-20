from unittest import TestCase
from tempfile import TemporaryDirectory
from unittest.mock import Mock
from pathlib import Path

import pandas as pd

from qlib_assistant_refactor.config import AppConfig
from qlib_assistant_refactor.model_cli import ModelCLI, ModelContext


class ModelCLITests(TestCase):
    def test_filter_rec_true_when_thresholds_pass(self) -> None:
        cli = ModelCLI(AppConfig(rec_filter=[{"ic": 0.01}, {"icir": 0.01}]))
        cli.get_ic_info = Mock(return_value=({}, [0.05, 0.10]))
        self.assertTrue(cli.filter_rec(Mock()))

    def test_assign_weights_normalizes_positive_rank_icir(self) -> None:
        cli = ModelCLI(AppConfig())
        cli.rid_rank_icir = {"a": 0.2, "b": 0.3}
        cli._assign_weights([ModelContext("exp", ["a", "b"])])
        self.assertAlmostEqual(cli.rid_weight["a"], 0.4, places=3)
        self.assertAlmostEqual(cli.rid_weight["b"], 0.6, places=3)

    def test_top_predictions_aggregates_scores(self) -> None:
        cli = ModelCLI(AppConfig())
        cli.get_model_list = Mock(return_value=[ModelContext("exp", ["r1", "r2"])])
        cli.rid_weight = {"r1": 0.25, "r2": 0.75}

        rec1 = Mock()
        rec1.load_object.return_value = pd.DataFrame(
            {"score": [0.2, -0.1]},
            index=pd.MultiIndex.from_tuples(
                [("2026-03-19", "SH600000"), ("2026-03-19", "SH600010")],
                names=["datetime", "instrument"],
            ),
        )
        rec2 = Mock()
        rec2.load_object.return_value = pd.DataFrame(
            {"score": [0.4, 0.3]},
            index=pd.MultiIndex.from_tuples(
                [("2026-03-19", "SH600000"), ("2026-03-19", "SH600010")],
                names=["datetime", "instrument"],
            ),
        )

        exp = Mock()
        exp.get_recorder.side_effect = [rec1, rec2]
        R = Mock()
        R.get_exp.return_value = exp
        cli._get_R = Mock(return_value=R)

        df = cli.top_predictions(limit=10, date="2026-03-19")
        row = df[df["instrument"] == "SH600000"].iloc[0]
        self.assertAlmostEqual(row["avg_score"], 0.35, places=6)
        self.assertAlmostEqual(row["pos_ratio"], 1.0, places=6)

    def test_filter_ret_df_applies_original_rules(self) -> None:
        cli = ModelCLI(AppConfig())
        df = pd.DataFrame(
            {
                "STD5": [0.02, 0.20],
                "STD20": [0.02, 0.02],
                "STD60": [0.02, 0.02],
                "ROC10": [1.0, 1.0],
                "ROC20": [1.0, 1.0],
                "ROC60": [1.0, 1.0],
            }
        )
        filtered = cli.filter_ret_df(df)
        self.assertEqual(len(filtered), 1)

    def test_aggregate_predictions_uses_weighted_scores(self) -> None:
        cli = ModelCLI(AppConfig())
        raw = pd.DataFrame(
            {
                "datetime": pd.to_datetime(["2026-03-19", "2026-03-19"]),
                "instrument": ["SH600000", "SH600000"],
                "score": [0.2, 0.4],
                "weight": [0.25, 0.75],
            }
        )
        agg = cli._aggregate_predictions(raw)
        self.assertAlmostEqual(float(agg.iloc[0]["avg_score"]), 0.35, places=6)

    def test_latest_selection_dir_picks_newest_name(self) -> None:
        with TemporaryDirectory() as tmpdir:
            cli = ModelCLI(AppConfig(analysis_folder=tmpdir))
            from pathlib import Path

            (Path(tmpdir) / "selection_20260101_000000").mkdir()
            latest = Path(tmpdir) / "selection_20260320_162435"
            latest.mkdir()
            self.assertEqual(cli.latest_selection_dir(), latest)

    def test_calculate_daily_equity(self) -> None:
        cli = ModelCLI(AppConfig())
        df = pd.DataFrame(
            {
                "avg_real_label": [0.01, 0.02],
                "turnover_rate": [0.5, 0.0],
                "csi300_real_label": [0.0, 0.01],
            }
        )
        out = cli._calculate_daily_equity(df)
        self.assertIn("strategy_equity", out.columns)
        self.assertGreater(float(out["strategy_equity"].iloc[-1]), 1.0)

    def test_list_backups_empty(self) -> None:
        with TemporaryDirectory() as tmpdir:
            cli = ModelCLI(AppConfig(uri_folder=f"{tmpdir}/mlruns", backup_folder=f"{tmpdir}/backup"))
            info = cli.list_backups()
            self.assertEqual(info["count"], 0)

    def test_entry_plan_builds_price_levels(self) -> None:
        cli = ModelCLI(AppConfig())
        cli._load_entry_candidates = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": pd.to_datetime(["2026-03-19"]),
                    "instrument": ["SH600000"],
                    "avg_score": [0.05],
                }
            )
        )
        cli._get_entry_price_history = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": pd.date_range("2026-03-06", periods=10, freq="B"),
                    "instrument": ["SH600000"] * 10,
                    "close": [10.0, 10.1, 10.2, 10.3, 10.4, 10.2, 10.5, 10.6, 10.7, 10.8],
                    "open": [9.9, 10.0, 10.1, 10.2, 10.3, 10.1, 10.4, 10.5, 10.6, 10.7],
                    "high": [10.1, 10.2, 10.3, 10.4, 10.5, 10.3, 10.6, 10.7, 10.8, 10.9],
                    "low": [9.8, 9.9, 10.0, 10.1, 10.2, 10.0, 10.3, 10.4, 10.5, 10.6],
                }
            )
        )

        df = cli.entry_plan(limit=1, date="2026-03-19")
        self.assertEqual(df.iloc[0]["instrument"], "SH600000")
        self.assertLess(float(df.iloc[0]["buy_low"]), float(df.iloc[0]["buy_high"]))
        self.assertLess(float(df.iloc[0]["stop_loss"]), float(df.iloc[0]["buy_low"]))
        self.assertGreater(float(df.iloc[0]["take_profit_2"]), float(df.iloc[0]["take_profit_1"]))
        self.assertIn("signal_reason", df.columns)
        self.assertIn("price_source", df.columns)
        self.assertIn("validation_status", df.columns)
        self.assertIn("action_plan", df.columns)

    def test_entry_plan_uses_raw_sync_price_scale_when_available(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sync_dir = tmpdir
            cli = ModelCLI(AppConfig(sync_dir=sync_dir, max_price=None))
            target = Path(sync_dir) / "akshare_daily"
            target.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [{"date": "2026-03-19", "close": 75.12}]
            ).to_csv(target / "SZ000333.csv", index=False)

            cli._load_entry_candidates = Mock(
                return_value=pd.DataFrame(
                    {
                        "datetime": pd.to_datetime(["2026-03-19"]),
                        "instrument": ["SZ000333"],
                        "avg_score": [0.05],
                    }
                )
            )
            cli._get_entry_price_history = Mock(
                return_value=pd.DataFrame(
                    {
                        "datetime": pd.date_range("2026-03-06", periods=10, freq="B"),
                        "instrument": ["SZ000333"] * 10,
                        "close": [1.1, 1.12, 1.14, 1.16, 1.18, 1.17, 1.19, 1.2, 1.21, 1.22],
                        "open": [1.09] * 10,
                        "high": [1.12] * 10,
                        "low": [1.08] * 10,
                    }
                )
            )

            df = cli.entry_plan(limit=1, date="2026-03-19")
            self.assertAlmostEqual(float(df.iloc[0]["close"]), 75.12, places=2)

    def test_entry_plan_uses_sync_name_and_validation_window(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sync_dir = tmpdir
            cli = ModelCLI(AppConfig(sync_dir=sync_dir, max_price=None))
            target = Path(sync_dir) / "akshare_daily"
            target.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {"date": "2026-03-19", "name": "美的集团", "open": 76.1, "close": 76.0, "high": 76.5, "low": 75.8},
                    {"date": "2026-03-20", "name": "美的集团", "open": 75.9, "close": 75.1, "high": 76.2, "low": 75.0},
                ]
            ).to_csv(target / "SZ000333.csv", index=False)

            cli._load_entry_candidates = Mock(
                return_value=pd.DataFrame(
                    {
                        "datetime": pd.to_datetime(["2026-03-19"]),
                        "instrument": ["SZ000333"],
                        "avg_score": [0.05],
                    }
                )
            )
            cli._get_entry_price_history = Mock(
                return_value=pd.DataFrame(
                    {
                        "datetime": pd.date_range("2026-03-06", periods=10, freq="B"),
                        "instrument": ["SZ000333"] * 10,
                        "close": [76.0, 76.1, 76.2, 76.0, 75.9, 75.8, 75.7, 75.9, 76.0, 76.0],
                        "open": [75.9] * 10,
                        "high": [76.3] * 10,
                        "low": [75.6] * 10,
                    }
                )
            )
            cli._next_trade_date = Mock(side_effect=lambda date_str, offset: "2026-03-20" if offset == 1 else "2026-03-21")

            df = cli.entry_plan(limit=1, date="2026-03-19")
            self.assertEqual(df.iloc[0]["name"], "美的集团")
            self.assertEqual(df.iloc[0]["validation_date"], "2026-03-20")
            self.assertIn(df.iloc[0]["validation_status"], {"buy_zone_touched", "closed_below_buy_zone", "closed_above_buy_zone", "watchlist", "breakout_triggered", "stop_loss_hit", "take_profit_1_hit", "take_profit_2_hit", "both_stop_and_target_hit"})

    def test_recommendation_sheet_keeps_validation_columns(self) -> None:
        cli = ModelCLI(AppConfig())
        cli.entry_plan = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": ["2026-03-19"],
                    "validation_date": ["2026-03-20"],
                    "score_rank": [1],
                    "instrument": ["SH600000"],
                    "name": ["浦发银行"],
                    "avg_score": [0.03],
                    "close": [10.2],
                    "buy_low": [10.0],
                    "buy_high": [10.1],
                    "breakout_price": [10.5],
                    "stop_loss": [9.8],
                    "take_profit_1": [10.6],
                    "take_profit_2": [10.9],
                    "action_plan": ["prefer_pullback_entry"],
                    "signal_reason": ["score_0.0300; holding_above_ma10"],
                    "entry_zone_hit": [True],
                    "breakout_hit": [False],
                    "stop_loss_hit_2d": [False],
                    "take_profit_1_hit_2d": [False],
                    "take_profit_2_hit_2d": [False],
                    "validation_status": ["buy_zone_touched"],
                    "validation_note": ["day1_range_touched_10.00_10.10"],
                    "price_source": ["akshare_sync_csv"],
                    "fundamental_risk_tag": ["基本面中性"],
                    "valuation_tag": ["估值信息有限"],
                    "fundamental_summary": ["报告期 20251231；营收同比 10.00%"],
                    "event_risk_tag": ["事件中性"],
                    "notice_summary": ["近三日无重点公告"],
                    "news_sentiment": ["中性"],
                    "news_summary": ["近三日无重点新闻"],
                    "data_as_of_date": ["2026-03-19"],
                    "data_fetched_at": ["2026-03-20T16:20:00"],
                    "data_sources": ["akshare / eastmoney"],
                    "data_validation_status": ["passed"],
                    "data_gate_status": ["通过"],
                }
            )
        )

        df = cli.recommendation_sheet(limit=1, date="2026-03-19")
        self.assertEqual(df.iloc[0]["validation_status"], "buy_zone_touched")
        self.assertIn("validation_note", df.columns)
        self.assertIn("fundamental_summary", df.columns)
        self.assertIn("data_gate_status", df.columns)

    def test_entry_plan_filters_by_max_price(self) -> None:
        cli = ModelCLI(AppConfig())
        cli._load_entry_candidates = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": pd.to_datetime(["2026-03-19", "2026-03-19"]),
                    "instrument": ["SH600000", "SZ000333"],
                    "avg_score": [0.05, 0.04],
                }
            )
        )
        cli._get_entry_price_history = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": list(pd.date_range("2026-03-06", periods=10, freq="B")) * 2,
                    "instrument": ["SH600000"] * 10 + ["SZ000333"] * 10,
                    "close": [10.0] * 10 + [35.0] * 10,
                    "open": [9.9] * 10 + [34.8] * 10,
                    "high": [10.1] * 10 + [35.3] * 10,
                    "low": [9.8] * 10 + [34.5] * 10,
                }
            )
        )

        df = cli.entry_plan(limit=5, date="2026-03-19", max_price=30)
        self.assertEqual(df["instrument"].tolist(), ["SH600000"])
        self.assertTrue((df["close"] <= 30).all())

    def test_recommendation_report_contains_summary_and_table(self) -> None:
        cli = ModelCLI(AppConfig())
        cli.recommendation_sheet = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": ["2026-03-19", "2026-03-19"],
                    "validation_date": ["2026-03-20", "2026-03-20"],
                    "score_rank": [1, 2],
                    "instrument": ["SZ000333", "SH601318"],
                    "name": ["美的集团", "中国平安"],
                    "avg_score": [0.0417, 0.0364],
                    "close": [76.0, 60.62],
                    "buy_low": [75.3977, 59.9102],
                    "buy_high": [76.1506, 60.7974],
                    "breakout_price": [77.9068, 63.4746],
                    "stop_loss": [74.2667, 58.7411],
                    "take_profit_1": [78.28, 62.9859],
                    "take_profit_2": [80.56, 65.3518],
                    "action_plan": ["wait_for_breakout_confirmation", "wait_for_breakout_confirmation"],
                    "signal_reason": ["score_0.0417; below_short_ma_wait_breakout", "score_0.0364; below_short_ma_wait_breakout"],
                    "entry_zone_hit": [True, True],
                    "breakout_hit": [False, False],
                    "stop_loss_hit_2d": [False, False],
                    "take_profit_1_hit_2d": [False, False],
                    "take_profit_2_hit_2d": [False, False],
                    "validation_status": ["buy_zone_touched", "buy_zone_touched"],
                    "validation_note": ["day1_range_touched_75.40_76.15", "day1_range_touched_59.91_60.80"],
                    "price_source": ["akshare_sync_csv", "akshare_sync_csv"],
                }
            )
        )

        report = cli.recommendation_report(limit=2, date="2026-03-19")
        self.assertIn("# 推荐验证日报（2026-03-19）", report)
        self.assertIn("## 数据可信度摘要", report)
        self.assertIn("## 验证摘要", report)
        self.assertIn("美的集团", report)
        self.assertIn("触及买入区间", report)

    def test_save_recommendation_report_writes_markdown_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            cli = ModelCLI(AppConfig(analysis_folder=tmpdir))
            cli.recommendation_report = Mock(return_value="# Demo\n")

            output = cli.save_recommendation_report(limit=3, date="2026-03-19")
            self.assertTrue(output.exists())
            self.assertEqual(output.read_text(encoding="utf-8"), "# Demo\n")

    def test_save_recommendation_sheet_writes_chinese_columns(self) -> None:
        with TemporaryDirectory() as tmpdir:
            cli = ModelCLI(AppConfig(analysis_folder=tmpdir, max_price=30.0))
            cli.recommendation_sheet = Mock(
                return_value=pd.DataFrame(
                    {
                        "datetime": ["2026-03-19"],
                        "validation_date": ["2026-03-20"],
                        "score_rank": [1],
                        "instrument": ["SH600000"],
                        "name": ["浦发银行"],
                        "avg_score": [0.03],
                        "close": [10.2],
                        "buy_low": [10.0],
                        "buy_high": [10.1],
                        "breakout_price": [10.5],
                        "stop_loss": [9.8],
                        "take_profit_1": [10.6],
                        "take_profit_2": [10.9],
                        "action_plan": ["prefer_pullback_entry"],
                        "signal_reason": ["score_0.0300; holding_above_ma10"],
                        "entry_zone_hit": [True],
                        "breakout_hit": [False],
                        "stop_loss_hit_2d": [False],
                        "take_profit_1_hit_2d": [False],
                        "take_profit_2_hit_2d": [False],
                        "validation_status": ["buy_zone_touched"],
                        "validation_note": ["day1_range_touched_10.00_10.10"],
                        "price_source": ["qlib_raw_by_factor"],
                    }
                )
            )

            output = cli.save_recommendation_sheet(limit=3, date="2026-03-19")
            content = pd.read_csv(output)
            self.assertIn("股票代码", content.columns)
            self.assertIn("操作计划", content.columns)
            self.assertEqual(content.iloc[0]["验证状态"], "触及买入区间")

    def test_recommendation_sheet_uses_config_max_price_by_default(self) -> None:
        cli = ModelCLI(AppConfig(max_price=30.0))
        cli._load_entry_candidates = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": pd.to_datetime(["2026-03-19", "2026-03-19"]),
                    "instrument": ["SH600000", "SH600009"],
                    "avg_score": [0.05, 0.04],
                }
            )
        )
        cli._get_entry_price_history = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": list(pd.date_range("2026-03-06", periods=10, freq="B")) * 2,
                    "instrument": ["SH600000"] * 10 + ["SH600009"] * 10,
                    "close": [10.0] * 10 + [35.0] * 10,
                    "open": [9.9] * 10 + [34.8] * 10,
                    "high": [10.1] * 10 + [35.3] * 10,
                    "low": [9.8] * 10 + [34.5] * 10,
                }
            )
        )

        df = cli.recommendation_sheet(limit=5, date="2026-03-19")
        self.assertEqual(df["instrument"].tolist(), ["SH600000"])

    def test_recommendation_html_contains_table_and_title(self) -> None:
        cli = ModelCLI(AppConfig())
        cli.recommendation_sheet = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": ["2026-03-19"],
                    "validation_date": ["2026-03-20"],
                    "score_rank": [1],
                    "instrument": ["SZ000333"],
                    "name": ["美的集团"],
                    "avg_score": [0.0417],
                    "close": [76.0],
                    "buy_low": [75.3977],
                    "buy_high": [76.1506],
                    "breakout_price": [77.9068],
                    "stop_loss": [74.2667],
                    "take_profit_1": [78.28],
                    "take_profit_2": [80.56],
                    "action_plan": ["wait_for_breakout_confirmation"],
                    "signal_reason": ["score_0.0417; below_short_ma_wait_breakout"],
                    "entry_zone_hit": [True],
                    "breakout_hit": [False],
                    "stop_loss_hit_2d": [False],
                    "take_profit_1_hit_2d": [False],
                    "take_profit_2_hit_2d": [False],
                    "validation_status": ["buy_zone_touched"],
                    "validation_note": ["day1_range_touched_75.40_76.15"],
                    "price_source": ["akshare_sync_csv"],
                }
            )
        )

        report = cli.recommendation_html(limit=1, date="2026-03-19")
        self.assertIn("<!DOCTYPE html>", report)
        self.assertIn("推荐验证日报 - 2026-03-19", report)
        self.assertIn("数据可信度摘要", report)
        self.assertIn("美的集团", report)
        self.assertIn("触及买入区间", report)

    def test_save_recommendation_html_writes_html_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            cli = ModelCLI(AppConfig(analysis_folder=tmpdir))
            cli.recommendation_html = Mock(return_value="<html>demo</html>")

            output = cli.save_recommendation_html(limit=3, date="2026-03-19")
            self.assertTrue(output.exists())
            self.assertEqual(output.read_text(encoding="utf-8"), "<html>demo</html>")

    def test_recommendation_spotlight_contains_industry_and_focus(self) -> None:
        cli = ModelCLI(AppConfig())
        cli.recommendation_sheet = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": ["2026-03-20"],
                    "validation_date": [None],
                    "score_rank": [1],
                    "instrument": ["SH601012"],
                    "name": ["隆基绿能"],
                    "avg_score": [0.0123],
                    "close": [18.99],
                    "buy_low": [18.2743],
                    "buy_high": [18.8114],
                    "breakout_price": [19.6392],
                    "stop_loss": [17.5746],
                    "take_profit_1": [20.4442],
                    "take_profit_2": [21.8984],
                    "action_plan": ["prefer_pullback_entry"],
                    "signal_reason": ["score_0.0123; holding_above_ma10"],
                    "entry_zone_hit": [False],
                    "breakout_hit": [False],
                    "stop_loss_hit_2d": [False],
                    "take_profit_1_hit_2d": [False],
                    "take_profit_2_hit_2d": [False],
                    "validation_status": ["pending_future_data"],
                    "validation_note": ["next_trade_day_not_available"],
                    "price_source": ["akshare_sync_csv"],
                }
            )
        )
        cli._lookup_instrument_industry = Mock(return_value="光伏设备")

        report = cli.recommendation_spotlight(limit=1, date="2026-03-20")
        self.assertIn("# 前三候选解读（2026-03-20）", report)
        self.assertIn("## 数据可信度摘要", report)
        self.assertIn("光伏设备", report)
        self.assertIn("观察重点", report)
        self.assertIn("隆基绿能", report)

    def test_attach_feed_context_merges_structured_feeds(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sync_dir = Path(tmpdir)
            manifest_dir = sync_dir / "manifests" / "2026-03-20"
            manifest_dir.mkdir(parents=True, exist_ok=True)
            (sync_dir / "gold" / "fundamentals").mkdir(parents=True, exist_ok=True)
            (sync_dir / "gold" / "events").mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "instrument": "SH600000",
                        "fundamental_risk_tag": "基本面中性",
                        "valuation_tag": "估值信息有限",
                        "fundamental_summary": "报告期 20251231；营收同比 10.00%",
                    }
                ]
            ).to_csv(sync_dir / "gold" / "fundamentals" / "fundamentals_2026-03-20.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "instrument": "SH600000",
                        "event_risk_tag": "事件中性",
                        "notice_summary": "近三日无重点公告",
                        "news_sentiment": "中性",
                        "news_summary": "近三日无重点新闻",
                    }
                ]
            ).to_csv(sync_dir / "gold" / "events" / "events_2026-03-20.csv", index=False)
            for feed_name, source_name in [
                ("market", "akshare+eastmoney"),
                ("fundamentals", "eastmoney_individual"),
                ("events", "eastmoney_notice+eastmoney_news"),
                ("freshness", "market+fundamentals+events"),
            ]:
                (manifest_dir / f"{feed_name}.json").write_text(
                    __import__("json").dumps(
                        {
                            "feed_type": feed_name,
                            "source_name": source_name,
                            "as_of_date": "2026-03-20",
                            "fetched_at": "2026-03-20T16:30:00",
                            "coverage_ratio": 1.0,
                            "record_count": 1,
                            "validation_status": "passed",
                            "validation_errors": [],
                            "eligible_for_daily_run": True,
                            "output_path": "",
                            "raw_paths": [],
                            "extra": {},
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )

            cli = ModelCLI(AppConfig(sync_dir=tmpdir))
            sheet = pd.DataFrame(
                [
                    {
                        "datetime": "2026-03-20",
                        "instrument": "SH600000",
                        "name": "浦发银行",
                    }
                ]
            )
            enriched = cli._attach_feed_context(sheet, as_of_date="2026-03-20")
            self.assertEqual(enriched.iloc[0]["fundamental_risk_tag"], "基本面中性")
            self.assertEqual(enriched.iloc[0]["event_risk_tag"], "事件中性")
            self.assertEqual(enriched.iloc[0]["data_gate_status"], "通过")

    def test_save_recommendation_spotlight_html_writes_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            cli = ModelCLI(AppConfig(analysis_folder=tmpdir))
            cli.recommendation_spotlight_html = Mock(return_value="<html>spotlight</html>")

            output = cli.save_recommendation_spotlight_html(limit=3, date="2026-03-20")
            self.assertTrue(output.exists())
            self.assertEqual(output.read_text(encoding="utf-8"), "<html>spotlight</html>")
