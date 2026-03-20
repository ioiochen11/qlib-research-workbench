from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import tarfile


@dataclass(frozen=True)
class BackupInfo:
    path: Path
    size_bytes: int


class BackupManager:
    def __init__(self, source_dir: str | Path, backup_dir: str | Path, target_parent: str | Path):
        self.source_dir = Path(source_dir).expanduser()
        self.backup_dir = Path(backup_dir).expanduser()
        self.target_parent = Path(target_parent).expanduser()

    def list_backups(self) -> list[BackupInfo]:
        if not self.backup_dir.exists():
            return []
        infos = []
        for path in sorted(self.backup_dir.glob("mlruns_*.tar.gz")):
            infos.append(BackupInfo(path=path, size_bytes=path.stat().st_size))
        return infos

    def backup(self, stamp: str | None = None) -> Path:
        if not self.source_dir.exists():
            raise RuntimeError(f"Source dir does not exist: {self.source_dir}")

        self.backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = stamp or datetime.now().strftime("%Y-%m-%d_%H%M%S")
        archive_path = self.backup_dir / f"mlruns_{timestamp}.tar.gz"
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(self.source_dir, arcname=self.source_dir.name)
        return archive_path

    def restore(self, archive_name: str | None = None, restore_all: bool = False) -> list[Path]:
        archives = self.list_backups()
        if not archives:
            raise RuntimeError(f"No backups found in {self.backup_dir}")

        if restore_all:
            selected = [info.path for info in archives]
        elif archive_name:
            selected = [self.backup_dir / archive_name]
            if not selected[0].exists():
                raise RuntimeError(f"Backup archive not found: {selected[0]}")
        else:
            selected = [archives[-1].path]

        self.target_parent.mkdir(parents=True, exist_ok=True)
        restored = []
        for archive in selected:
            with tarfile.open(archive, "r:gz") as tar:
                self._safe_extract(tar, self.target_parent)
            restored.append(archive)
        return restored

    def _safe_extract(self, tar: tarfile.TarFile, target: Path) -> None:
        target_resolved = target.resolve()
        for member in tar.getmembers():
            member_path = (target / member.name).resolve()
            if not str(member_path).startswith(str(target_resolved)):
                raise RuntimeError(f"Unsafe path in archive: {member.name}")
        tar.extractall(path=target)
