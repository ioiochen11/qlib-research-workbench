from __future__ import annotations

from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable, Optional
import tarfile

import requests

from .config import AppConfig, MirrorConfig
from .qlib_env import provider_uri_path


DEFAULT_TIMEOUT = (10, 30)


@dataclass(frozen=True)
class ProbeResult:
    mirror_name: str
    url: str
    ok: bool
    status_code: Optional[int] = None
    content_length: Optional[int] = None
    last_modified: Optional[str] = None
    error: Optional[str] = None


class DataService:
    def __init__(self, config: AppConfig):
        self.config = config

    def iter_candidate_urls(self) -> Iterable[tuple[MirrorConfig, str]]:
        for mirror in self.config.mirrors:
            yield mirror, mirror.build_url(self.config.asset_url)

    def probe(self) -> list[ProbeResult]:
        results: list[ProbeResult] = []
        session = requests.Session()
        for mirror, url in self.iter_candidate_urls():
            try:
                response = session.head(url, allow_redirects=True, timeout=DEFAULT_TIMEOUT)
                results.append(
                    ProbeResult(
                        mirror_name=mirror.name,
                        url=response.url,
                        ok=response.ok,
                        status_code=response.status_code,
                        content_length=self._parse_content_length(
                            response.headers.get("content-length")
                        ),
                        last_modified=response.headers.get("last-modified"),
                    )
                )
            except requests.RequestException as exc:
                results.append(
                    ProbeResult(
                        mirror_name=mirror.name,
                        url=url,
                        ok=False,
                        error=str(exc),
                    )
                )
        return results

    def choose_first_available(self) -> ProbeResult:
        results = self.probe()
        for result in results:
            if result.ok:
                return result
        raise RuntimeError("No reachable mirror found")

    def peek_remote_bytes(self, byte_count: int = 128) -> bytes:
        chosen = self.choose_first_available()
        headers = {"Range": f"bytes=0-{max(0, byte_count - 1)}"}
        with requests.get(chosen.url, headers=headers, stream=True, timeout=DEFAULT_TIMEOUT) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=byte_count):
                if chunk:
                    return chunk[:byte_count]
        return b""

    def download(
        self,
        output_path: str | Path,
        max_bytes: int | None = None,
        url: str | None = None,
    ) -> Path:
        chosen_url = url or self.choose_first_available().url
        output = Path(output_path).expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)

        with requests.get(chosen_url, stream=True, timeout=DEFAULT_TIMEOUT) as resp:
            resp.raise_for_status()
            written = 0
            with output.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        if max_bytes is not None:
                            remaining = max_bytes - written
                            if remaining <= 0:
                                break
                            chunk = chunk[:remaining]
                        fh.write(chunk)
                        written += len(chunk)
                        if max_bytes is not None and written >= max_bytes:
                            break
        return output

    def read_local_calendar_date(self) -> Optional[str]:
        day_file = provider_uri_path(self.config) / "calendars" / "day.txt"
        if not day_file.exists():
            return None
        lines = day_file.read_text(encoding="utf-8").splitlines()
        return lines[-1].strip() if lines else None

    def remote_publish_date(self) -> Optional[str]:
        chosen = self.choose_first_available()
        if not chosen.last_modified:
            return None
        return parsedate_to_datetime(chosen.last_modified).date().isoformat()

    def extract_archive(
        self,
        archive_path: str | Path,
        target_dir: str | Path,
        strip_components: int = 0,
    ) -> Path:
        archive = Path(archive_path).expanduser()
        target = Path(target_dir).expanduser()
        target.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive, "r:gz") as tf:
            if strip_components <= 0:
                tf.extractall(path=target)
                return target

            members = []
            for member in tf.getmembers():
                parts = Path(member.name).parts
                if len(parts) <= strip_components:
                    continue
                member.name = str(Path(*parts[strip_components:]))
                members.append(member)
            tf.extractall(path=target, members=members)
        return target

    def verify_local_dataset(self) -> dict[str, object]:
        base = provider_uri_path(self.config)
        day_file = base / "calendars" / "day.txt"
        features_dir = base / "features"
        instruments_dir = base / "instruments"
        return {
            "base_exists": base.exists(),
            "day_file_exists": day_file.exists(),
            "latest_calendar_date": self.read_local_calendar_date(),
            "features_exists": features_dir.exists(),
            "instruments_exists": instruments_dir.exists(),
        }

    @staticmethod
    def _parse_content_length(raw_value: Optional[str]) -> Optional[int]:
        if raw_value is None:
            return None
        try:
            return int(raw_value)
        except ValueError:
            return None
