from __future__ import annotations

from pathlib import Path
from typing import Optional

from .config import AppConfig
from .data_service import DataService, ProbeResult
from .qlib_env import provider_uri_path


PROXY_PREFIX_MAP = {
    "A": "https://gh-proxy.org/",
    "B": "https://hk.gh-proxy.org/",
    "C": "https://cdn.gh-proxy.org/",
    "D": "https://edgeone.gh-proxy.org/",
}


class DataCLI:
    """Compatibility layer for the original qlibAssistant data subcommands."""

    def __init__(self, config: AppConfig, service: Optional[DataService] = None):
        self.config = config
        self.service = service or DataService(config)

    def need_update(self) -> bool:
        remote_date = self.service.remote_publish_date()
        local_date = self.service.read_local_calendar_date()
        return str(remote_date) != str(local_date)

    def status(self) -> dict[str, object]:
        local_date = self.service.read_local_calendar_date()
        remote_date = self.service.remote_publish_date()
        needs_update = str(local_date) != str(remote_date)
        return {
            "region": self.config.region,
            "provider_uri": str(provider_uri_path(self.config)),
            "local_calendar_date": local_date,
            "remote_publish_date": remote_date,
            "needs_update": needs_update,
        }

    def update(self, proxy: str = "A", force: bool = False) -> dict[str, object]:
        current = self.status()
        if not force and not current["needs_update"]:
            return {"updated": False, **current}

        selected = self._select_probe_result(proxy)
        downloaded = self.service.download(
            self.config.download_output,
            max_bytes=None,
            url=selected.url,
        )
        extracted = self.service.extract_archive(
            downloaded,
            self.config.extract_dir,
            strip_components=1,
        )
        verification = self.service.verify_local_dataset()

        return {
            "updated": True,
            "mirror_name": selected.mirror_name,
            "mirror_url": selected.url,
            "downloaded": str(downloaded),
            "extracted_to": str(extracted),
            **verification,
        }

    def verify(self) -> dict[str, object]:
        return self.service.verify_local_dataset()

    def _select_probe_result(self, proxy: str) -> ProbeResult:
        normalized = proxy.upper()
        if normalized in PROXY_PREFIX_MAP:
            prefix = PROXY_PREFIX_MAP[normalized]
        else:
            prefix = proxy

        results = self.service.probe()
        for result in results:
            if result.ok and result.url.startswith(prefix):
                return result
        for result in results:
            if result.ok:
                return result
        raise RuntimeError(f"No reachable mirror found for proxy {proxy!r}")
