from unittest import TestCase

from qlib_assistant_refactor.config import AppConfig
from qlib_assistant_refactor.task_factory import (
    build_default_segments,
    build_experiment_name,
    build_task_template,
)
from qlib_assistant_refactor.train_cli import TrainCLI


class TaskFactoryTests(TestCase):
    def test_build_default_segments_orders_windows(self) -> None:
        config = AppConfig(
            train_window_years=3,
            valid_window_months=6,
            test_window_months=1,
        )
        segments = build_default_segments(config, latest_date="2026-03-19")
        self.assertLess(segments.train[0], segments.train[1])
        self.assertLess(segments.train[1], segments.valid[0])
        self.assertLess(segments.valid[1], segments.test[0])

    def test_build_task_template_uses_config_fields(self) -> None:
        config = AppConfig(
            model_name="Linear",
            dataset_name="Alpha158",
            stock_pool="csi300",
        )
        task = build_task_template(config, latest_date="2026-03-19")
        self.assertEqual(task["model"]["class"], "LinearModel")
        self.assertEqual(task["dataset"]["kwargs"]["handler"]["class"], "Alpha158")
        self.assertEqual(task["dataset"]["kwargs"]["handler"]["kwargs"]["instruments"], "csi300")

    def test_build_experiment_name_contains_key_fields(self) -> None:
        config = AppConfig(
            pfx_name="EXP",
            sfx_name="daily",
            model_name="Linear",
            dataset_name="Alpha158",
            stock_pool="csi300",
            rolling_type="expanding",
            step=60,
        )
        name = build_experiment_name(config, "smoke_20260320")
        self.assertIn("EXP_Linear_Alpha158_csi300_expanding_step60_daily_smoke_20260320", name)

    def test_clip_task_to_latest_caps_future_test_end(self) -> None:
        import pandas as pd

        config = AppConfig()
        cli = TrainCLI(config)
        task = {
            "dataset": {
                "kwargs": {
                    "handler": {"kwargs": {"end_time": "2026-05-22"}},
                    "segments": {
                        "train": ("2022-01-01", "2024-12-31"),
                        "valid": ("2025-01-01", "2025-12-31"),
                        "test": ("2026-02-24", "2026-05-22"),
                    },
                }
            }
        }
        clipped = cli._clip_task_to_latest(task, "2026-03-19")
        self.assertEqual(clipped["dataset"]["kwargs"]["segments"]["test"][1], pd.Timestamp("2026-03-19"))
        self.assertEqual(clipped["dataset"]["kwargs"]["handler"]["kwargs"]["end_time"], "2026-03-19")
