from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from qlib_assistant_refactor.backup_utils import BackupManager


class BackupUtilsTests(TestCase):
    def test_backup_creates_archive(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "mlruns"
            source.mkdir()
            (source / "meta.yaml").write_text("test: 1\n", encoding="utf-8")
            backup_dir = root / "backup"
            target_parent = root / "restore"
            manager = BackupManager(source_dir=source, backup_dir=backup_dir, target_parent=target_parent)

            archive = manager.backup(stamp="2026-03-19")
            self.assertTrue(archive.exists())
            self.assertEqual(archive.name, "mlruns_2026-03-19.tar.gz")

    def test_restore_extracts_latest_archive(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "mlruns"
            source.mkdir()
            (source / "meta.yaml").write_text("test: 1\n", encoding="utf-8")
            backup_dir = root / "backup"
            target_parent = root / "restore"
            manager = BackupManager(source_dir=source, backup_dir=backup_dir, target_parent=target_parent)

            manager.backup(stamp="2026-03-19")
            restored = manager.restore()
            self.assertEqual(len(restored), 1)
            self.assertTrue((target_parent / "mlruns" / "meta.yaml").exists())
