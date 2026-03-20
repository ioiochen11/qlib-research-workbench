from unittest import TestCase
from unittest.mock import Mock

from qlib_assistant_refactor.config import AppConfig
from qlib_assistant_refactor.data_cli import DataCLI
from qlib_assistant_refactor.data_service import ProbeResult


class DataCLITests(TestCase):
    def test_need_update_false_when_dates_match(self) -> None:
        service = Mock()
        service.remote_publish_date.return_value = "2026-03-19"
        service.read_local_calendar_date.return_value = "2026-03-19"
        cli = DataCLI(AppConfig(), service=service)
        self.assertFalse(cli.need_update())

    def test_select_proxy_prefers_requested_prefix(self) -> None:
        service = Mock()
        service.probe.return_value = [
            ProbeResult(
                mirror_name="gh-proxy",
                url="https://gh-proxy.org/example",
                ok=True,
            ),
            ProbeResult(
                mirror_name="hk-proxy",
                url="https://hk.gh-proxy.org/example",
                ok=True,
            ),
        ]
        cli = DataCLI(AppConfig(), service=service)
        selected = cli._select_probe_result("B")
        self.assertEqual(selected.url, "https://hk.gh-proxy.org/example")

    def test_update_uses_selected_url(self) -> None:
        service = Mock()
        service.remote_publish_date.return_value = "2026-03-20"
        service.read_local_calendar_date.return_value = "2026-03-19"
        service.probe.return_value = [
            ProbeResult(
                mirror_name="gh-proxy",
                url="https://gh-proxy.org/example",
                ok=True,
            ),
        ]
        service.download.return_value = "/tmp/qlib_bin.tar.gz"
        service.extract_archive.return_value = "/tmp/cn_data"
        service.verify_local_dataset.return_value = {"base_exists": True}

        cli = DataCLI(AppConfig(), service=service)
        result = cli.update(proxy="A")

        service.download.assert_called_once_with(
            cli.config.download_output,
            max_bytes=None,
            url="https://gh-proxy.org/example",
        )
        self.assertTrue(result["updated"])

    def test_sync_akshare_returns_summary(self) -> None:
        service = Mock()
        cli = DataCLI(AppConfig(), service=service)
        fake_summary = {
            "csv_dir": "/tmp/csv",
            "qlib_dir": "/tmp/qlib",
            "start_date": "2026-03-19",
            "end_date": "2026-03-20",
            "symbol_count": 3,
            "written_csv": 3,
            "dump_mode": "update",
            "calendar_count": 2,
        }
        with self.subTest("patched sync"):
            from unittest.mock import patch

            with patch("qlib_assistant_refactor.data_cli.AkshareDailySync") as mock_sync_cls:
                mock_sync = mock_sync_cls.return_value
                mock_sync.sync.return_value = type("Summary", (), fake_summary)()
                result = cli.sync_akshare(limit=3)

        self.assertEqual(result["written_csv"], 3)

    def test_refresh_sse180_universe_returns_summary(self) -> None:
        service = Mock()
        cli = DataCLI(AppConfig(), service=service)
        fake_summary = {
            "universe_name": "sse180",
            "source": "akshare_csindex",
            "instrument_count": 180,
            "instruments_path": "/tmp/sse180.txt",
            "cache_path": "/tmp/sse180.csv",
        }
        with self.subTest("patched refresh"):
            from unittest.mock import patch

            with patch("qlib_assistant_refactor.data_cli.AkshareDailySync") as mock_sync_cls:
                mock_sync = mock_sync_cls.return_value
                mock_sync.refresh_sse180_universe.return_value = type("Summary", (), fake_summary)()
                result = cli.refresh_sse180_universe(as_of_date="2026-03-20")

        self.assertEqual(result["instrument_count"], 180)
        self.assertEqual(result["source"], "akshare_csindex")
