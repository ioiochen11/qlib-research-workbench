from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from qlib_assistant_refactor.app import RollingTrader
from qlib_assistant_refactor.config import MirrorConfig


class RollingTraderTests(TestCase):
    def test_config_merging_priority(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        'region: "us"',
                        'provider_uri: "/tmp/provider"',
                    ]
                ),
                encoding="utf-8",
            )

            trader = RollingTrader(config_path=str(config_path), region="cn")
            self.assertEqual(trader.region, "cn")
            self.assertEqual(trader.provider_uri, "/tmp/provider")
            self.assertIsInstance(trader.config.mirrors[0], MirrorConfig)

    def test_ensure_predict_dates_uses_local_calendar(self) -> None:
        with TemporaryDirectory() as tmpdir:
            day_file = Path(tmpdir) / "calendars" / "day.txt"
            day_file.parent.mkdir(parents=True, exist_ok=True)
            day_file.write_text("2026-03-18\n2026-03-19\n", encoding="utf-8")

            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(f'provider_uri: "{tmpdir}"\n', encoding="utf-8")

            trader = RollingTrader(config_path=str(config_path))
            self.assertEqual(
                trader.ensure_predict_dates(),
                [{"start": "2026-03-19", "end": "2026-03-19"}],
            )
