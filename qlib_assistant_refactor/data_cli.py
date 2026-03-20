from __future__ import annotations

from pathlib import Path
from typing import Optional

from .akshare_sync import AkshareDailySync
from .config import AppConfig
from .data_service import DataService, ProbeResult
from .feed_sync import FeedSyncManager
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
        self.feed_sync = FeedSyncManager(config)

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

    def sync_akshare(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        summary = AkshareDailySync(self.config).sync(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        return {
            "csv_dir": str(summary.csv_dir),
            "qlib_dir": str(summary.qlib_dir),
            "start_date": summary.start_date,
            "end_date": summary.end_date,
            "symbol_count": summary.symbol_count,
            "written_csv": summary.written_csv,
            "dump_mode": summary.dump_mode,
            "calendar_count": summary.calendar_count,
        }

    def refresh_sse180_universe(self, as_of_date: str | None = None) -> dict[str, object]:
        summary = AkshareDailySync(self.config).refresh_sse180_universe(as_of_date=as_of_date)
        return {
            "universe_name": summary.universe_name,
            "source": summary.source,
            "instrument_count": summary.instrument_count,
            "instruments_path": str(summary.instruments_path),
            "cache_path": str(summary.cache_path),
        }

    def sync_market(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        summary = self.feed_sync.sync_market(start_date=start_date, end_date=end_date, limit=limit)
        return {
            "feed_type": summary.feed_type,
            "as_of_date": summary.as_of_date,
            "output_path": str(summary.output_path),
            "manifest_path": str(summary.manifest_path),
            "record_count": summary.record_count,
            "coverage_ratio": summary.coverage_ratio,
            "eligible_for_daily_run": summary.eligible_for_daily_run,
            "validation_status": summary.validation_status,
            "validation_errors": "|".join(summary.validation_errors),
        }

    def sync_fundamentals(
        self,
        as_of_date: str | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        summary = self.feed_sync.sync_fundamentals(as_of_date=as_of_date, limit=limit)
        return {
            "feed_type": summary.feed_type,
            "as_of_date": summary.as_of_date,
            "output_path": str(summary.output_path),
            "manifest_path": str(summary.manifest_path),
            "record_count": summary.record_count,
            "coverage_ratio": summary.coverage_ratio,
            "eligible_for_daily_run": summary.eligible_for_daily_run,
            "validation_status": summary.validation_status,
            "validation_errors": "|".join(summary.validation_errors),
        }

    def sync_events(
        self,
        as_of_date: str | None = None,
        lookback_days: int = 3,
        limit: int | None = None,
    ) -> dict[str, object]:
        summary = self.feed_sync.sync_events(as_of_date=as_of_date, lookback_days=lookback_days, limit=limit)
        return {
            "feed_type": summary.feed_type,
            "as_of_date": summary.as_of_date,
            "output_path": str(summary.output_path),
            "manifest_path": str(summary.manifest_path),
            "record_count": summary.record_count,
            "coverage_ratio": summary.coverage_ratio,
            "eligible_for_daily_run": summary.eligible_for_daily_run,
            "validation_status": summary.validation_status,
            "validation_errors": "|".join(summary.validation_errors),
        }

    def verify_freshness(self, as_of_date: str | None = None) -> dict[str, object]:
        summary = self.feed_sync.verify_freshness(as_of_date=as_of_date)
        return {
            "as_of_date": summary.as_of_date,
            "eligible_for_daily_run": summary.eligible_for_daily_run,
            "validation_status": summary.validation_status,
            "validation_errors": "|".join(summary.validation_errors),
            "manifest_path": str(summary.manifest_path),
            "manifest_paths": "|".join(str(path) for path in summary.manifest_paths),
            "fetched_at": summary.fetched_at,
        }

    def show_manifest(self, as_of_date: str | None = None) -> dict[str, object]:
        return self.feed_sync.show_manifest(as_of_date=as_of_date)

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
