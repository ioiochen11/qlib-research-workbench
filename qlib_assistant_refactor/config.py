from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class MirrorConfig:
    name: str
    prefix: str = ""

    def build_url(self, asset_url: str) -> str:
        return f"{self.prefix}{asset_url}"


@dataclass(frozen=True)
class AppConfig:
    region: str = "cn"
    provider_uri: str = "~/.qlib/qlib_data/cn_data"
    download_output: str = "~/tmp/qlib_bin.tar.gz"
    extract_dir: str = "~/.qlib/qlib_data/cn_data"
    uri_folder: str = "~/.qlibAssistant/mlruns"
    pfx_name: str = "p"
    sfx_name: str = "s"
    model_name: str = "Linear"
    dataset_name: str = "Alpha158"
    stock_pool: str = "csi300"
    step: int = 60
    rolling_type: str = "expanding"
    train_window_years: int = 3
    valid_window_months: int = 6
    test_window_months: int = 1
    analysis_folder: str = "~/.qlibAssistant/analysis"
    backup_folder: str = "~/model_pkl"
    model_filter: Optional[List[str]] = None
    rec_filter: Optional[List[Dict[str, float]]] = None
    asset_url: str = (
        "https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz"
    )
    mirrors: List[MirrorConfig] = field(
        default_factory=lambda: [
            MirrorConfig(name="gh-proxy", prefix="https://gh-proxy.org/"),
            MirrorConfig(name="direct", prefix=""),
        ]
    )

    @classmethod
    def from_yaml(cls, path: str | Path = "config.yaml") -> "AppConfig":
        config_path = Path(path)
        if not config_path.exists():
            return cls()

        data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        mirrors = [
            MirrorConfig(name=item["name"], prefix=item.get("prefix", ""))
            for item in data.get("mirrors", [])
        ]
        if not mirrors:
            mirrors = cls().mirrors

        merged: Dict[str, Any] = {
            "region": data.get("region", cls.region),
            "provider_uri": data.get("provider_uri", cls.provider_uri),
            "download_output": data.get("download_output", cls.download_output),
            "extract_dir": data.get("extract_dir", cls.extract_dir),
            "uri_folder": data.get("uri_folder", cls.uri_folder),
            "pfx_name": data.get("pfx_name", cls.pfx_name),
            "sfx_name": data.get("sfx_name", cls.sfx_name),
            "model_name": data.get("model_name", cls.model_name),
            "dataset_name": data.get("dataset_name", cls.dataset_name),
            "stock_pool": data.get("stock_pool", cls.stock_pool),
            "step": data.get("step", cls.step),
            "rolling_type": data.get("rolling_type", cls.rolling_type),
            "train_window_years": data.get("train_window_years", cls.train_window_years),
            "valid_window_months": data.get("valid_window_months", cls.valid_window_months),
            "test_window_months": data.get("test_window_months", cls.test_window_months),
            "analysis_folder": data.get("analysis_folder", cls.analysis_folder),
            "backup_folder": data.get("backup_folder", cls.backup_folder),
            "model_filter": data.get("model_filter", cls.model_filter),
            "rec_filter": data.get("rec_filter", cls.rec_filter),
            "asset_url": data.get("asset_url", cls.asset_url),
            "mirrors": mirrors,
        }
        return cls(**merged)
