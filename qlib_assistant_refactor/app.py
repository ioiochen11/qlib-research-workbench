from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from pprint import pformat
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

    @property
    def region(self) -> str:
        return self.config.region

    @property
    def provider_uri(self) -> str:
        return self.config.provider_uri
