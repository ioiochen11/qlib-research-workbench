from __future__ import annotations

from dataclasses import dataclass, field
from importlib.util import find_spec
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
    sync_dir: str = "~/.qlibAssistant/daily_sync"
    sync_universe: str = "csi300"
    sync_adjust: str = "qfq"
    benchmark_symbol: str = "SH000300"
    uri_folder: str = "~/.qlibAssistant/mlruns"
    pfx_name: str = "p"
    sfx_name: str = "s"
    model_name: str = "auto"
    dataset_name: str = "Alpha158"
    stock_pool: str = "csi300"
    max_price: float = 30.0
    model_kwargs: Optional[Dict[str, Any]] = None
    step: int = 60
    rolling_type: str = "expanding"
    train_window_years: int = 2
    valid_window_months: int = 4
    test_window_months: int = 1
    analysis_folder: str = "~/.qlibAssistant/analysis"
    backup_folder: str = "~/model_pkl"
    model_filter: Optional[List[str]] = None
    rec_filter: Optional[List[Dict[str, float]]] = None
    market_close_cutoff: str = "15:30"
    required_feeds: List[str] = field(default_factory=lambda: ["market", "fundamentals", "events"])
    max_feed_age_hours: Dict[str, int] = field(
        default_factory=lambda: {
            "market": 8,
            "fundamentals": 24,
            "events": 24,
        }
    )
    strict_report_gate: bool = True
    source_priority: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "market": ["akshare", "eastmoney"],
            "fundamentals": ["eastmoney_individual", "eastmoney_yjbb", "eastmoney_yjyg", "eastmoney_yjkb"],
            "events": ["eastmoney_notice", "eastmoney_news"],
        }
    )
    min_universe_coverage: float = 0.9
    price_conflict_tolerance: float = 0.03
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
        defaults = cls()
        mirrors = [
            MirrorConfig(name=item["name"], prefix=item.get("prefix", ""))
            for item in data.get("mirrors", [])
        ]
        if not mirrors:
            mirrors = defaults.mirrors

        merged: Dict[str, Any] = {
            "region": data.get("region", cls.region),
            "provider_uri": data.get("provider_uri", cls.provider_uri),
            "download_output": data.get("download_output", cls.download_output),
            "extract_dir": data.get("extract_dir", cls.extract_dir),
            "sync_dir": data.get("sync_dir", cls.sync_dir),
            "sync_universe": data.get("sync_universe", cls.sync_universe),
            "sync_adjust": data.get("sync_adjust", cls.sync_adjust),
            "benchmark_symbol": data.get("benchmark_symbol", cls.benchmark_symbol),
            "uri_folder": data.get("uri_folder", cls.uri_folder),
            "pfx_name": data.get("pfx_name", cls.pfx_name),
            "sfx_name": data.get("sfx_name", cls.sfx_name),
            "model_name": data.get("model_name", cls.model_name),
            "dataset_name": data.get("dataset_name", cls.dataset_name),
            "stock_pool": data.get("stock_pool", cls.stock_pool),
            "max_price": data.get("max_price", cls.max_price),
            "model_kwargs": data.get("model_kwargs", cls.model_kwargs),
            "step": data.get("step", cls.step),
            "rolling_type": data.get("rolling_type", cls.rolling_type),
            "train_window_years": data.get("train_window_years", cls.train_window_years),
            "valid_window_months": data.get("valid_window_months", cls.valid_window_months),
            "test_window_months": data.get("test_window_months", cls.test_window_months),
            "analysis_folder": data.get("analysis_folder", cls.analysis_folder),
            "backup_folder": data.get("backup_folder", cls.backup_folder),
            "model_filter": data.get("model_filter", cls.model_filter),
            "rec_filter": data.get("rec_filter", cls.rec_filter),
            "market_close_cutoff": data.get("market_close_cutoff", defaults.market_close_cutoff),
            "required_feeds": data.get("required_feeds", defaults.required_feeds),
            "max_feed_age_hours": data.get("max_feed_age_hours", defaults.max_feed_age_hours),
            "strict_report_gate": data.get("strict_report_gate", defaults.strict_report_gate),
            "source_priority": data.get("source_priority", defaults.source_priority),
            "min_universe_coverage": data.get("min_universe_coverage", defaults.min_universe_coverage),
            "price_conflict_tolerance": data.get("price_conflict_tolerance", defaults.price_conflict_tolerance),
            "asset_url": data.get("asset_url", cls.asset_url),
            "mirrors": mirrors,
        }
        return cls(**merged)


def model_module_available(model_key: str) -> bool:
    normalized = model_key.lower()
    if normalized in {"linear"}:
        return True
    if normalized in {"lightgbm", "lgbm"}:
        return find_spec("lightgbm") is not None
    if normalized == "xgboost":
        return find_spec("xgboost") is not None
    return False


def resolve_model_name(model_name: str) -> str:
    normalized = model_name.lower()
    if normalized == "auto":
        for candidate in ["lightgbm", "linear"]:
            if model_module_available(candidate):
                return candidate
        return "linear"
    return normalized


def resolved_model_label(model_name: str) -> str:
    normalized = resolve_model_name(model_name)
    mapping = {
        "linear": "Linear",
        "lightgbm": "LightGBM",
        "lgbm": "LightGBM",
        "xgboost": "XGBoost",
    }
    return mapping.get(normalized, model_name)
