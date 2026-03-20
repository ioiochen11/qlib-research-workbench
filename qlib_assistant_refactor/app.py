from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from pprint import pformat
import shutil
from typing import Any

from .config import AppConfig
from .data_cli import DataCLI
from .data_service import DataService
from .model_cli import ModelCLI
from .train_cli import TrainCLI


class RollingTrader:
    """Minimal refactor of the original roll.py focused on the data flow."""

    def __init__(self, config_path: str = "config.yaml", **overrides: Any):
        self.config_path = config_path
        self.config = self._load_config(config_path, overrides)
        self.params = asdict(self.config)
        self.data = DataCLI(self.config, service=DataService(self.config))
        self.train = TrainCLI(self.config)
        self.model = ModelCLI(self.config)

    def _load_config(self, config_path: str, overrides: dict[str, Any]) -> AppConfig:
        config = AppConfig.from_yaml(config_path)
        raw = {key: getattr(config, key) for key in config.__dataclass_fields__}
        raw.update({key: value for key, value in overrides.items() if value is not None})
        return AppConfig(**raw)

    def show_config(self) -> str:
        return pformat(self.params, sort_dicts=True)

    def ensure_predict_dates(self) -> list[dict[str, str]]:
        latest = self.data.service.read_local_calendar_date()
        if latest is None:
            return []
        return [{"start": latest, "end": latest}]

    def daily_run(self) -> dict[str, object]:
        refresh_info = None
        if self.config.stock_pool == "sse180" or self.config.sync_universe == "sse180":
            refresh_info = self.data.refresh_sse180_universe()

        before_date = self.data.service.read_local_calendar_date()
        sync_info = self.data.sync_akshare(start_date=before_date, end_date=None, limit=None)
        latest_date = str(self.data.service.read_local_calendar_date() or sync_info["end_date"])
        train_info = self.train.start(limit=None)
        selection_dir = self.model.selection_report()
        csv_path = self.model.save_recommendation_sheet(
            limit=30,
            date=latest_date,
            selection_dir=str(selection_dir),
            filtered=True,
            max_price=self.config.max_price,
        )
        markdown_path = self.model.save_recommendation_report(
            limit=30,
            date=latest_date,
            selection_dir=str(selection_dir),
            filtered=True,
            max_price=self.config.max_price,
        )
        html_path = self.model.save_recommendation_html(
            limit=30,
            date=latest_date,
            selection_dir=str(selection_dir),
            filtered=True,
            max_price=self.config.max_price,
        )
        spotlight_md_path = self.model.save_recommendation_spotlight(
            limit=3,
            date=latest_date,
            selection_dir=str(selection_dir),
            filtered=True,
            max_price=self.config.max_price,
        )
        spotlight_html_path = self.model.save_recommendation_spotlight_html(
            limit=3,
            date=latest_date,
            selection_dir=str(selection_dir),
            filtered=True,
            max_price=self.config.max_price,
        )
        latest_paths = self._write_latest_artifacts(
            csv_path=csv_path,
            markdown_path=markdown_path,
            html_path=html_path,
            spotlight_markdown_path=spotlight_md_path,
            spotlight_html_path=spotlight_html_path,
        )
        return {
            "refresh_info": refresh_info,
            "sync_info": sync_info,
            "train_info": train_info,
            "selection_dir": str(selection_dir),
            "recommendations_csv": str(csv_path),
            "recommendation_report_md": str(markdown_path),
            "recommendation_report_html": str(html_path),
            "recommendation_spotlight_md": str(spotlight_md_path),
            "recommendation_spotlight_html": str(spotlight_html_path),
            **latest_paths,
        }

    def _write_latest_artifacts(
        self,
        csv_path: Path,
        markdown_path: Path,
        html_path: Path,
        spotlight_markdown_path: Path,
        spotlight_html_path: Path,
    ) -> dict[str, str]:
        analysis_dir = Path(self.config.analysis_folder).expanduser()
        analysis_dir.mkdir(parents=True, exist_ok=True)
        latest_csv = analysis_dir / "latest_recommendations.csv"
        latest_md = analysis_dir / "latest_recommendation_report.md"
        latest_html = analysis_dir / "latest_recommendation_report.html"
        latest_spotlight_md = analysis_dir / "latest_recommendation_spotlight.md"
        latest_spotlight_html = analysis_dir / "latest_recommendation_spotlight.html"
        shutil.copy2(csv_path, latest_csv)
        shutil.copy2(markdown_path, latest_md)
        shutil.copy2(html_path, latest_html)
        shutil.copy2(spotlight_markdown_path, latest_spotlight_md)
        shutil.copy2(spotlight_html_path, latest_spotlight_html)
        return {
            "latest_recommendations_csv": str(latest_csv),
            "latest_recommendation_report_md": str(latest_md),
            "latest_recommendation_report_html": str(latest_html),
            "latest_recommendation_spotlight_md": str(latest_spotlight_md),
            "latest_recommendation_spotlight_html": str(latest_spotlight_html),
        }

    @property
    def region(self) -> str:
        return self.config.region

    @property
    def provider_uri(self) -> str:
        return self.config.provider_uri
