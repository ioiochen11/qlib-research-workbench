from unittest import TestCase
from tempfile import TemporaryDirectory
from unittest.mock import Mock

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
