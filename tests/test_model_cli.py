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
        self.assertIn("bucket_reliable", df.columns)
        self.assertIn("bucket_note", df.columns)
        self.assertIn("confidence_level", df.columns)
        self.assertIn("confidence_note", df.columns)

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
        cli._score_bucket_filter_lines = Mock(return_value=["- 历史分桶过滤：`关闭`"])
        cli.recommendation_sheet = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": ["2026-03-19", "2026-03-19"],
                    "validation_date": ["2026-03-20", "2026-03-20"],
                    "score_rank": [1, 2],
                    "instrument": ["SZ000333", "SH601318"],
                    "name": ["美的集团", "中国平安"],
                    "avg_score": [0.0417, 0.0364],
                    "confidence_score": [82.0, 58.0],
                    "confidence_level": ["高", "中"],
                    "confidence_note": ["正向模型占比 100%；参与模型数 5；当前分数档历史统计更可靠", "正向模型占比 60%；参与模型数 3；当前分数档历史统计偏弱"],
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
        self.assertIn("历史分桶过滤", report)
        self.assertIn("高置信度过滤", report)
        self.assertIn("置信度摘要", report)
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
            self.assertIn("置信度", content.columns)
            self.assertEqual(content.iloc[0]["验证状态"], "触及买入区间")
            self.assertEqual(content.iloc[0]["信号说明"], "模型平均分 0.0300；价格仍在 10 日线之上，偏向回踩型机会")

    def test_attach_confidence_context_uses_model_agreement_and_bucket_reliability(self) -> None:
        cli = ModelCLI(AppConfig())
        plan = pd.DataFrame(
            {
                "avg_score": [0.03, 0.001],
                "pos_ratio": [1.0, 0.4],
                "model_count": [5, 1],
                "bucket_reliable": ["是", "否"],
            }
        )

        enriched = cli._attach_confidence_context(plan)

        self.assertEqual(enriched.iloc[0]["confidence_level"], "高")
        self.assertEqual(enriched.iloc[1]["confidence_level"], "低")
        self.assertIn("正向模型占比", enriched.iloc[0]["confidence_note"])

    def test_recommendation_sheet_applies_high_confidence_filter_when_enabled(self) -> None:
        cli = ModelCLI(
            AppConfig(
                confidence_filter_enabled=True,
                confidence_min_level="高",
                confidence_filter_fallback_to_unfiltered=False,
            )
        )
        cli.entry_plan = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": ["2026-03-25", "2026-03-25"],
                    "validation_date": [None, None],
                    "score_rank": [1, 2],
                    "instrument": ["SH601012", "SH600028"],
                    "name": ["隆基绿能", "中国石化"],
                    "avg_score": [0.03, 0.004],
                    "pos_ratio": [1.0, 0.4],
                    "model_count": [5, 1],
                    "bucket_reliable": ["是", "否"],
                    "close": [18.9, 5.9],
                    "buy_low": [18.2, 5.8],
                    "buy_high": [18.8, 5.9],
                    "breakout_price": [19.6, 6.6],
                    "stop_loss": [17.5, 5.7],
                    "take_profit_1": [20.5, 6.2],
                    "take_profit_2": [22.2, 6.4],
                    "action_plan": ["prefer_pullback_entry", "wait_for_breakout_confirmation"],
                    "signal_reason": ["score_0.0300; holding_above_ma10", "score_0.0040; below_short_ma_wait_breakout"],
                    "entry_zone_hit": [False, False],
                    "breakout_hit": [False, False],
                    "stop_loss_hit_2d": [False, False],
                    "take_profit_1_hit_2d": [False, False],
                    "take_profit_2_hit_2d": [False, False],
                    "validation_status": ["pending_future_data", "pending_future_data"],
                    "validation_note": ["下一交易日数据暂不可用", "下一交易日数据暂不可用"],
                    "price_source": ["AkShare 同步原始日线", "AkShare 同步原始日线"],
                }
            )
        )
        cli._attach_feed_context = Mock(side_effect=lambda plan, as_of_date: plan)
        cli._attach_score_bucket_context = Mock(side_effect=lambda plan, as_of_date, filtered, max_price: plan)

        df = cli.recommendation_sheet(limit=5, date="2026-03-25")

        self.assertEqual(df["instrument"].tolist(), ["SH601012"])
        self.assertEqual(df.iloc[0]["confidence_level"], "高")

    def test_recommendation_sheet_uses_config_max_price_by_default(self) -> None:
        with TemporaryDirectory() as tmpdir:
            cli = ModelCLI(AppConfig(sync_dir=tmpdir, max_price=30.0))
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

    def test_recommendation_sheet_applies_reliable_bucket_filter(self) -> None:
        cli = ModelCLI(
            AppConfig(
                score_bucket_filter_enabled=True,
                score_bucket_fallback_to_unfiltered=False,
                score_bucket_min_evaluable_count=2,
                score_bucket_min_hit_rate=0.5,
                score_bucket_min_avg_weekly_return=0.0,
            )
        )
        cli.entry_plan = Mock(
            return_value=pd.DataFrame(
                {
                    "datetime": ["2026-03-25", "2026-03-25"],
                    "validation_date": [None, None],
                    "score_rank": [1, 2],
                    "instrument": ["SH601012", "SH600028"],
                    "name": ["隆基绿能", "中国石化"],
                    "avg_score": [-0.001, -0.01],
                    "close": [18.9, 5.9],
                    "buy_low": [18.2, 5.8],
                    "buy_high": [18.8, 5.9],
                    "breakout_price": [19.6, 6.6],
                    "stop_loss": [17.5, 5.7],
                    "take_profit_1": [20.5, 6.2],
                    "take_profit_2": [22.2, 6.4],
                    "action_plan": ["prefer_pullback_entry", "wait_for_breakout_confirmation"],
                    "signal_reason": ["score_0.0010; holding_above_ma10", "score_-0.0100; below_short_ma_wait_breakout"],
                    "entry_zone_hit": [False, False],
                    "breakout_hit": [False, False],
                    "stop_loss_hit_2d": [False, False],
                    "take_profit_1_hit_2d": [False, False],
                    "take_profit_2_hit_2d": [False, False],
                    "validation_status": ["pending_future_data", "pending_future_data"],
                    "validation_note": ["下一交易日数据暂不可用", "下一交易日数据暂不可用"],
                    "price_source": ["AkShare 同步原始日线", "AkShare 同步原始日线"],
                }
            )
        )
        cli._attach_feed_context = Mock(side_effect=lambda plan, as_of_date: plan)
        cli.score_bucket_sheet = Mock(
            return_value=pd.DataFrame(
                {
                    "score_bucket": ["-0.0050 ~ 0.0000", "-0.0200 ~ -0.0050"],
                    "signal_count": [2, 2],
                    "evaluable_count": [2, 2],
                    "hit_rate": [0.5, 0.0],
                    "direction_rate": [0.5, 1.0],
                    "avg_weekly_return": [0.01, -0.001],
                    "median_weekly_return": [0.01, -0.001],
                }
            )
        )

        df = cli.recommendation_sheet(limit=5, date="2026-03-25")
        self.assertEqual(df["instrument"].tolist(), ["SH601012"])
        self.assertEqual(df.iloc[0]["bucket_reliable"], "是")

    def test_recommendation_sheet_falls_back_to_unfiltered_candidates_for_daily_testing(self) -> None:
        with TemporaryDirectory() as tmpdir:
            analysis_dir = Path(tmpdir) / "analysis"
            selection_dir = analysis_dir / "selection_20260402_102059"
            selection_dir.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                columns=["datetime", "instrument", "avg_score", "pos_ratio", "model_count"]
            ).to_csv(selection_dir / "2026-04-01_filter_ret.csv", index=False, encoding="utf-8-sig")
            pd.DataFrame(
                [
                    {
                        "datetime": "2026-04-01",
                        "instrument": "SH688223",
                        "avg_score": 0.0421,
                        "pos_ratio": 1.0,
                        "model_count": 1,
                    },
                    {
                        "datetime": "2026-04-01",
                        "instrument": "SZ000630",
                        "avg_score": 0.0365,
                        "pos_ratio": 1.0,
                        "model_count": 1,
                    },
                ]
            ).to_csv(selection_dir / "2026-04-01_ret.csv", index=False, encoding="utf-8-sig")

            cli = ModelCLI(
                AppConfig(
                    analysis_folder=str(analysis_dir),
                    filtered_candidate_fallback_to_raw=True,
                    score_bucket_filter_enabled=True,
                    score_bucket_fallback_to_unfiltered=True,
                    confidence_filter_enabled=True,
                    confidence_min_level="高",
                    confidence_filter_fallback_to_unfiltered=True,
                    max_price=30.0,
                )
            )
            cli._get_entry_price_history = Mock(
                return_value=pd.DataFrame(
                    {
                        "datetime": list(pd.date_range("2026-03-19", periods=10, freq="B")) * 2,
                        "instrument": ["SH688223"] * 10 + ["SZ000630"] * 10,
                        "close": [6.5, 6.6, 6.7, 6.8, 6.75, 6.7, 6.72, 6.74, 6.78, 6.82]
                        + [5.6, 5.7, 5.8, 5.85, 5.9, 5.88, 5.9, 5.92, 5.95, 5.98],
                        "open": [6.4] * 10 + [5.5] * 10,
                        "high": [6.9] * 10 + [6.1] * 10,
                        "low": [6.3] * 10 + [5.4] * 10,
                    }
                )
            )
            cli._attach_feed_context = Mock(side_effect=lambda plan, as_of_date: plan)
            cli._attach_score_bucket_context = Mock(side_effect=lambda plan, as_of_date, filtered, max_price: plan.assign(bucket_reliable="否"))

            df = cli.recommendation_sheet(
                limit=5,
                date="2026-04-01",
                selection_dir=str(selection_dir),
                filtered=True,
                max_price=30.0,
            )

            self.assertEqual(df["instrument"].tolist(), ["SH688223", "SZ000630"])
            self.assertTrue((df["confidence_level"] == "中").all())

    def test_weekly_recommendation_sheet_builds_recent_comparison_rows(self) -> None:
        with TemporaryDirectory() as tmpdir:
            analysis_dir = Path(tmpdir) / "analysis"
            analysis_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {"信号日期": "2026-03-19", "排名": 1, "股票代码": "SH600000", "股票名称": "浦发银行", "平均分": 0.03, "收盘价": 10.0},
                    {"信号日期": "2026-03-20", "排名": 1, "股票代码": "SH600028", "股票名称": "中国石化", "平均分": 0.02, "收盘价": 6.0},
                ]
            ).to_csv(analysis_dir / "recommendations_2026-03-20_filtered_maxprice30.csv", index=False, encoding="utf-8-sig")
            pd.DataFrame(
                [
                    {"信号日期": "2026-03-23", "排名": 1, "股票代码": "SH601012", "股票名称": "隆基绿能", "平均分": -0.01, "收盘价": 18.8},
                ]
            ).to_csv(analysis_dir / "recommendations_2026-03-23_filtered_maxprice30.csv", index=False, encoding="utf-8-sig")

            cli = ModelCLI(AppConfig(analysis_folder=str(analysis_dir), max_price=30.0))
            cli._lookup_raw_daily_bar = Mock(
                side_effect=lambda instrument, date_str: {
                    ("SH600000", "2026-03-23"): {"close": 10.5},
                    ("SH600028", "2026-03-23"): {"close": 5.8},
                    ("SH601012", "2026-03-23"): {"close": 18.8},
                }.get((instrument, date_str))
            )

            sheet = cli.weekly_recommendation_sheet(end_date="2026-03-23", trading_days=5)

            self.assertEqual(len(sheet), 3)
            self.assertEqual(sheet.iloc[0]["instrument"], "SH600000")
            self.assertEqual(sheet.iloc[0]["recommendation_hit"], "是")
            self.assertEqual(sheet.iloc[1]["recommendation_hit"], "否")
            self.assertEqual(sheet.iloc[2]["week_result"], "本周最后一个交易日信号")

    def test_weekly_recommendation_sheet_reads_validated_daily_prices(self) -> None:
        with TemporaryDirectory() as tmpdir:
            analysis_dir = Path(tmpdir) / "analysis"
            analysis_dir.mkdir(parents=True, exist_ok=True)
            sync_dir = Path(tmpdir) / "sync"
            price_dir = sync_dir / "gold" / "market" / "validated_daily"
            price_dir.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {"信号日期": "2026-03-20", "排名": 1, "股票代码": "SH601012", "股票名称": "隆基绿能", "平均分": 0.02, "收盘价": 18.99},
                ]
            ).to_csv(analysis_dir / "recommendations_2026-03-20_filtered_maxprice30.csv", index=False, encoding="utf-8-sig")
            pd.DataFrame(
                [
                    {"信号日期": "2026-03-23", "排名": 1, "股票代码": "SH601012", "股票名称": "隆基绿能", "平均分": -0.01, "收盘价": 18.81},
                ]
            ).to_csv(analysis_dir / "recommendations_2026-03-23_filtered_maxprice30.csv", index=False, encoding="utf-8-sig")
            pd.DataFrame(
                [
                    {"date": "2026-03-20", "symbol": "SH601012", "name": "隆基绿能", "open": 18.50, "close": 18.99, "high": 19.60, "low": 18.42, "volume": 1, "factor": 1.0},
                    {"date": "2026-03-23", "symbol": "SH601012", "name": "隆基绿能", "open": 19.05, "close": 18.81, "high": 19.40, "low": 18.69, "volume": 1, "factor": 1.0},
                ]
            ).to_csv(price_dir / "SH601012.csv", index=False)

            cli = ModelCLI(AppConfig(analysis_folder=str(analysis_dir), sync_dir=str(sync_dir), max_price=30.0))
            sheet = cli.weekly_recommendation_sheet(end_date="2026-03-23", trading_days=5)

            prior_signal = sheet[sheet["signal_date"].astype(str) == "2026-03-20"].iloc[0]
            self.assertAlmostEqual(float(prior_signal["week_end_close"]), 18.81, places=2)
            self.assertAlmostEqual(float(prior_signal["weekly_return"]), (18.81 / 18.99) - 1.0, places=6)

    def test_score_bucket_sheet_groups_recent_signals(self) -> None:
        cli = ModelCLI(AppConfig())
        cli.weekly_recommendation_sheet = Mock(
            return_value=pd.DataFrame(
                {
                    "signal_date": ["2026-03-20", "2026-03-20", "2026-03-21", "2026-03-22"],
                    "instrument": ["A", "B", "C", "D"],
                    "avg_score": [0.021, 0.012, -0.003, -0.03],
                    "weekly_return": [0.03, 0.01, -0.02, -0.01],
                    "recommendation_hit": ["是", "是", "否", "否"],
                    "score_direction_match": ["是", "是", "是", "是"],
                }
            )
        )

        summary = cli.score_bucket_sheet(end_date="2026-03-22", trading_days=20)

        self.assertEqual(summary["signal_count"].sum(), 4)
        positive_bucket = summary[summary["score_bucket"] == "0.0050 ~ 0.0200"].iloc[0]
        self.assertEqual(int(positive_bucket["signal_count"]), 1)
        self.assertAlmostEqual(float(positive_bucket["hit_rate"]), 1.0, places=6)
        strong_positive = summary[summary["score_bucket"] == "大于等于 0.0200"].iloc[0]
        self.assertAlmostEqual(float(strong_positive["avg_weekly_return"]), 0.03, places=6)

    def test_score_threshold_comparison_sheet_compares_multiple_cutoffs(self) -> None:
        cli = ModelCLI(AppConfig(score_bucket_min_evaluable_count=2, score_bucket_min_avg_weekly_return=0.0))
        cli.score_bucket_sheet = Mock(
            return_value=pd.DataFrame(
                {
                    "score_bucket": ["-0.0050 ~ 0.0000", "-0.0200 ~ -0.0050"],
                    "signal_count": [2, 2],
                    "evaluable_count": [2, 2],
                    "hit_rate": [0.50, 0.60],
                    "direction_rate": [0.50, 0.80],
                    "avg_weekly_return": [0.01, -0.01],
                    "median_weekly_return": [0.01, -0.01],
                }
            )
        )
        cli.weekly_recommendation_sheet = Mock(
            return_value=pd.DataFrame(
                {
                    "avg_score": [-0.001, -0.010, -0.002],
                    "weekly_return": [0.01, -0.01, 0.02],
                    "recommendation_hit": ["是", "否", "是"],
                    "score_direction_match": ["是", "是", "是"],
                }
            )
        )

        comparison = cli.score_threshold_comparison_sheet(end_date="2026-03-25", trading_days=20, thresholds=[0.50, 0.60])

        self.assertEqual(len(comparison), 2)
        self.assertEqual(comparison.iloc[0]["reliable_bucket_count"], 1)
        self.assertEqual(comparison.iloc[0]["kept_signal_count"], 2)
        self.assertEqual(comparison.iloc[1]["reliable_bucket_count"], 0)

    def test_score_threshold_comparison_report_contains_final_and_candidate_views(self) -> None:
        cli = ModelCLI(AppConfig())
        cli.weekly_recommendation_sheet = Mock(
            side_effect=[
                pd.DataFrame({"weekly_return": [0.01, -0.01, 0.02]}),
                pd.DataFrame({"weekly_return": [0.01]}),
            ]
        )
        cli.score_threshold_comparison_sheet = Mock(
            side_effect=[
                pd.DataFrame(
                    {
                        "hit_rate_threshold": [0.50],
                        "reliable_bucket_count": [1],
                        "reliable_buckets": ["-0.0050 ~ 0.0000"],
                        "kept_signal_count": [1],
                        "kept_signal_ratio": [1.0],
                        "kept_hit_rate": [1.0],
                        "kept_avg_weekly_return": [0.02],
                    }
                ),
                pd.DataFrame(
                    {
                        "hit_rate_threshold": [0.50],
                        "reliable_bucket_count": [2],
                        "reliable_buckets": ["-0.0050 ~ 0.0000、0.0000 ~ 0.0050"],
                        "kept_signal_count": [3],
                        "kept_signal_ratio": [1.0],
                        "kept_hit_rate": [0.67],
                        "kept_avg_weekly_return": [0.01],
                    }
                ),
            ]
        )

        report = cli.score_threshold_comparison_report(end_date="2026-03-25", trading_days=60)

        self.assertIn("最终推荐视角", report)
        self.assertIn("候选样本视角", report)
        self.assertIn("最终推荐样本数", report)
        self.assertIn("候选样本数", report)

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
