from __future__ import annotations

from pathlib import Path

from .config import AppConfig


def provider_uri_path(config: AppConfig) -> Path:
    return Path(config.provider_uri).expanduser()


def provider_uri_str(config: AppConfig) -> str:
    return str(provider_uri_path(config))


def latest_local_data_date(config: AppConfig) -> str:
    day_file = provider_uri_path(config) / "calendars" / "day.txt"
    lines = day_file.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise RuntimeError(f"No local trade dates found in {day_file}")
    return lines[-1].strip()


def mlruns_path(config: AppConfig) -> Path:
    return Path(config.uri_folder).expanduser()


def mlruns_uri(config: AppConfig) -> str:
    return "file:" + str(mlruns_path(config))


def build_exp_manager(config: AppConfig) -> dict:
    return {
        "class": "MLflowExpManager",
        "module_path": "qlib.workflow.expm",
        "kwargs": {
            "uri": mlruns_uri(config),
            "default_exp_name": "default",
        },
    }


def ensure_qlib(config: AppConfig) -> None:
    import qlib

    qlib.init(
        provider_uri=provider_uri_str(config),
        region=config.region,
        exp_manager=build_exp_manager(config),
    )
