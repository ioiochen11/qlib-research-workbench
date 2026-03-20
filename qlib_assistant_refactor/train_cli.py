from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from .config import AppConfig, resolve_model_name, resolved_model_label
from .qlib_env import ensure_qlib, latest_local_data_date
from .task_factory import build_experiment_name, build_task_template, latest_trade_date


def my_enhanced_handler_mod(task: dict, rg: Any) -> None:
    from qlib.workflow.task.gen import handler_mod as default_handler_mod

    default_handler_mod(task, rg)
    train_start, train_end = task["dataset"]["kwargs"]["segments"]["train"]
    h_kwargs = task["dataset"]["kwargs"]["handler"]["kwargs"]
    h_kwargs["fit_start_time"] = train_start
    h_kwargs["fit_end_time"] = train_end


class TrainCLI:
    """Minimal training layer compatible with the refactored app."""

    def __init__(self, config: AppConfig):
        self.config = config
        self._qlib_ready = False

    def plan(self, limit: int = 5) -> dict[str, object]:
        tasks = self.gen(limit=limit)
        preview = []
        for task in tasks[:limit]:
            preview.append(task["dataset"]["kwargs"]["segments"])
        return {
            "experiment_name": self._build_experiment_name("plan"),
            "resolved_model_name": resolved_model_label(self.config.model_name),
            "task_count": len(tasks),
            "preview": preview,
        }

    def gen(self, limit: Optional[int] = None) -> List[dict]:
        self._ensure_qlib()
        task_template = build_task_template(self.config)

        if self.config.rolling_type in {"expanding", "sliding"}:
            from qlib.workflow.task.gen import RollingGen, task_generator

            rolling_gen = RollingGen(
                step=self.config.step,
                rtype=self.config.rolling_type,
                ds_extra_mod_func=my_enhanced_handler_mod,
            )
            tasks = task_generator(tasks=task_template, generators=rolling_gen)
        else:
            tasks = [task_template]

        latest_local_date = latest_local_data_date(self.config)
        tasks = [self._clip_task_to_latest(task, latest_local_date) for task in tasks]
        tasks = [task for task in tasks if task is not None]

        if limit is not None:
            tasks = tasks[:limit]
        return tasks

    def smoke(self) -> dict[str, object]:
        self._ensure_qlib()
        from qlib.model.trainer import TrainerR

        tasks = self.gen(limit=1)
        if not tasks:
            raise RuntimeError("No training task generated")

        experiment_name = self._build_experiment_name("smoke")
        trainer = TrainerR(experiment_name=experiment_name)
        recs = trainer.train(tasks)
        recs = trainer.end_train(recs)
        rec = recs[0]

        return {
            "experiment_name": experiment_name,
            "resolved_model_name": resolved_model_label(self.config.model_name),
            "recorder_id": rec.info["id"],
            "artifacts": sorted(rec.list_artifacts()),
            "train_segment": tasks[0]["dataset"]["kwargs"]["segments"]["train"],
            "valid_segment": tasks[0]["dataset"]["kwargs"]["segments"]["valid"],
            "test_segment": tasks[0]["dataset"]["kwargs"]["segments"]["test"],
        }

    def start(self, limit: Optional[int] = None) -> dict[str, object]:
        self._ensure_qlib()
        from qlib.model.trainer import TrainerR

        tasks = self.gen(limit=limit)
        experiment_name = self._build_experiment_name("start")
        trainer = TrainerR(experiment_name=experiment_name)
        recs = trainer.train(tasks)
        recs = trainer.end_train(recs)
        return {
            "experiment_name": experiment_name,
            "resolved_model_name": resolved_model_label(self.config.model_name),
            "recorders": [rec.info["id"] for rec in recs],
            "task_count": len(tasks),
        }

    def list_experiments(self) -> dict[str, object]:
        self._ensure_qlib()
        from qlib.workflow import R

        exps = R.list_experiments()
        return {
            "count": len(exps),
            "names": sorted(exps.keys()),
        }

    def _clip_task_to_latest(self, task: dict, latest_local_date: str) -> Optional[dict]:
        import pandas as pd

        latest_ts = pd.Timestamp(latest_local_date)
        segs = task["dataset"]["kwargs"]["segments"]
        test_start, test_end = segs["test"]
        if pd.Timestamp(test_start) > latest_ts:
            return None
        if pd.Timestamp(test_end) > latest_ts:
            segs["test"] = (test_start, latest_ts)
            task["dataset"]["kwargs"]["handler"]["kwargs"]["end_time"] = latest_local_date
        return task

    def _build_experiment_name(self, mode: str) -> str:
        time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        return build_experiment_name(self.config, f"{mode}_{time_str}")

    def _ensure_qlib(self) -> None:
        if self._qlib_ready:
            return

        try:
            import qlib  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "qlib is not installed in the current environment. "
                "Use .venv/bin/pip install pyqlib first."
            ) from exc

        ensure_qlib(self.config)
        self._qlib_ready = True
