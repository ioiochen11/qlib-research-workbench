from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch
import tarfile

from qlib_assistant_refactor.config import AppConfig
from qlib_assistant_refactor.data_service import DataService


class DataServiceTests(TestCase):
    def test_read_local_calendar_date_returns_last_line(self) -> None:
        with TemporaryDirectory() as tmpdir:
            day_file = Path(tmpdir) / "calendars" / "day.txt"
            day_file.parent.mkdir(parents=True, exist_ok=True)
            day_file.write_text("2026-03-18\n2026-03-19\n", encoding="utf-8")

            service = DataService(AppConfig(provider_uri=tmpdir))
            self.assertEqual(service.read_local_calendar_date(), "2026-03-19")

    def test_download_respects_max_bytes(self) -> None:
        service = DataService(AppConfig())
        fake_response = Mock()
        fake_response.raise_for_status = Mock()
        fake_response.iter_content = Mock(
            return_value=[b"a" * 6, b"b" * 6]
        )
        fake_response.__enter__ = Mock(return_value=fake_response)
        fake_response.__exit__ = Mock(return_value=False)

        with TemporaryDirectory() as tmpdir, \
            patch.object(service, "choose_first_available", return_value=Mock(url="https://example.test/file")), \
            patch("qlib_assistant_refactor.data_service.requests.get", return_value=fake_response):
            output = Path(tmpdir) / "sample.bin"
            service.download(output, max_bytes=8)
            self.assertEqual(output.read_bytes(), b"aaaaaabb")

    def test_download_uses_explicit_url(self) -> None:
        service = DataService(AppConfig())
        fake_response = Mock()
        fake_response.raise_for_status = Mock()
        fake_response.iter_content = Mock(return_value=[b"abc"])
        fake_response.__enter__ = Mock(return_value=fake_response)
        fake_response.__exit__ = Mock(return_value=False)

        with TemporaryDirectory() as tmpdir, patch(
            "qlib_assistant_refactor.data_service.requests.get", return_value=fake_response
        ) as mock_get:
            output = Path(tmpdir) / "sample.bin"
            service.download(output, url="https://example.test/file")
            mock_get.assert_called_once()
            self.assertEqual(mock_get.call_args.args[0], "https://example.test/file")

    def test_extract_archive_supports_strip_components(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_dir = root / "src"
            nested = source_dir / "qlib_bin" / "calendars"
            nested.mkdir(parents=True, exist_ok=True)
            (nested / "day.txt").write_text("2026-03-19\n", encoding="utf-8")

            archive = root / "sample.tar.gz"
            with tarfile.open(archive, "w:gz") as tf:
                tf.add(source_dir / "qlib_bin", arcname="qlib_bin")

            target = root / "out"
            service = DataService(AppConfig())
            service.extract_archive(archive, target, strip_components=1)

            self.assertTrue((target / "calendars" / "day.txt").exists())
