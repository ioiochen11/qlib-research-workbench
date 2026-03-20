from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from dateutil.relativedelta import relativedelta

from .config import AppConfig


MODEL_CONFIG_MAP: Dict[str, dict] = {
    "linear": {
        "class": "LinearModel",
        "module_path": "qlib.contrib.model.linear",
        "kwargs": {"estimator": "ols", "fit_intercept": False},
    },
    "lightgbm": {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": {},
    },
    "lgbm": {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": {},
    },
    "xgboost": {
        "class": "XGBModel",
        "module_path": "qlib.contrib.model.xgboost",
        "kwargs": {},
    },
}


RECORD_CONFIG: List[dict] = [
    {
        "class": "SignalRecord",
        "module_path": "qlib.workflow.record_temp",
        "kwargs": {"dataset": "<DATASET>", "model": "<MODEL>"},
    },
    {
        "class": "SigAnaRecord",
        "module_path": "qlib.workflow.record_temp",
    },
]


@dataclass(frozen=True)
class SegmentSet:
    train: tuple[str, str]
    valid: tuple[str, str]
    test: tuple[str, str]

    def as_dict(self) -> dict:
        return {
            "train": self.train,
            "valid": self.valid,
            "test": self.test,
        }


def latest_trade_date(provider_uri: str) -> str:
    day_file = Path(provider_uri).expanduser() / "calendars" / "day.txt"
    lines = day_file.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise RuntimeError(f"No trading calendar data found in {day_file}")
    return lines[-1].strip()


def build_default_segments(config: AppConfig, latest_date: str | None = None) -> SegmentSet:
    import pandas as pd

    latest = pd.Timestamp(latest_date or latest_trade_date(config.provider_uri))
    test_end = latest
    test_start = test_end - relativedelta(months=config.test_window_months) + relativedelta(days=1)
    valid_end = test_start - relativedelta(days=1)
    valid_start = valid_end - relativedelta(months=config.valid_window_months) + relativedelta(days=1)
    train_end = valid_start - relativedelta(days=1)
    train_start = train_end - relativedelta(years=config.train_window_years) + relativedelta(days=1)

    fmt = "%Y-%m-%d"
    return SegmentSet(
        train=(train_start.strftime(fmt), train_end.strftime(fmt)),
        valid=(valid_start.strftime(fmt), valid_end.strftime(fmt)),
        test=(test_start.strftime(fmt), test_end.strftime(fmt)),
    )


def build_task_template(config: AppConfig, latest_date: str | None = None) -> dict:
    model_key = config.model_name.lower()
    if model_key not in MODEL_CONFIG_MAP:
        raise ValueError(f"Unsupported model_name: {config.model_name}")

    if config.dataset_name not in {"Alpha158", "Alpha360"}:
        raise ValueError(f"Unsupported dataset_name: {config.dataset_name}")

    segments = build_default_segments(config, latest_date=latest_date)
    end_time = segments.test[1]

    task = {
        "model": deepcopy(MODEL_CONFIG_MAP[model_key]),
        "dataset": {
            "class": "DatasetH",
            "module_path": "qlib.data.dataset",
            "kwargs": {
                "handler": {
                    "class": config.dataset_name,
                    "module_path": "qlib.contrib.data.handler",
                    "kwargs": {
                        "start_time": segments.train[0],
                        "end_time": end_time,
                        "fit_start_time": segments.train[0],
                        "fit_end_time": segments.train[1],
                        "instruments": config.stock_pool,
                    },
                },
                "segments": segments.as_dict(),
            },
        },
        "record": deepcopy(RECORD_CONFIG),
    }
    return task


def build_experiment_name(config: AppConfig, suffix: str) -> str:
    return (
        f"{config.pfx_name}_{config.model_name}_{config.dataset_name}_"
        f"{config.stock_pool}_{config.rolling_type}_step{config.step}_{config.sfx_name}_{suffix}"
    )
