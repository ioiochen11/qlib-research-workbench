from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock

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

    def test_daily_run_creates_latest_artifacts_after_success(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            analysis_dir = Path(tmpdir) / "analysis"
            provider_uri = Path(tmpdir) / "provider"
            (provider_uri / "calendars").mkdir(parents=True, exist_ok=True)
            (provider_uri / "calendars" / "day.txt").write_text("2026-03-19\n2026-03-20\n", encoding="utf-8")
            config_path.write_text(
                "\n".join(
                    [
                        f'provider_uri: "{provider_uri}"',
                        f'analysis_folder: "{analysis_dir}"',
                        'stock_pool: "sse180"',
                        'sync_universe: "sse180"',
                        'max_price: 30',
                    ]
                ),
                encoding="utf-8",
            )

            trader = RollingTrader(config_path=str(config_path))
            trader.data.refresh_sse180_universe = Mock(return_value={"instrument_count": 180})
            trader.data.sync_akshare = Mock(return_value={"written_csv": 180, "end_date": "2026-03-20"})
            trader.data.service.read_local_calendar_date = Mock(side_effect=["2026-03-19", "2026-03-20"])
            trader.train.start = Mock(return_value={"task_count": 1})
            selection_dir = analysis_dir / "selection_demo"
            selection_dir.mkdir(parents=True, exist_ok=True)
            trader.model.selection_report = Mock(return_value=selection_dir)

            csv_path = analysis_dir / "recommendations_2026-03-20_filtered_maxprice30.csv"
            md_path = analysis_dir / "recommendation_report_2026-03-20_filtered_maxprice30.md"
            html_path = analysis_dir / "recommendation_report_2026-03-20_filtered_maxprice30.html"
            spotlight_md_path = analysis_dir / "recommendation_spotlight_2026-03-20_filtered_maxprice30.md"
            spotlight_html_path = analysis_dir / "recommendation_spotlight_2026-03-20_filtered_maxprice30.html"
            csv_path.write_text("股票代码\nSH600000\n", encoding="utf-8")
            md_path.write_text("# 推荐验证日报\n", encoding="utf-8")
            html_path.write_text("<html>日报</html>", encoding="utf-8")
            spotlight_md_path.write_text("# 前三候选解读\n", encoding="utf-8")
            spotlight_html_path.write_text("<html>解读</html>", encoding="utf-8")
            trader.model.save_recommendation_sheet = Mock(return_value=csv_path)
            trader.model.save_recommendation_report = Mock(return_value=md_path)
            trader.model.save_recommendation_html = Mock(return_value=html_path)
            trader.model.save_recommendation_spotlight = Mock(return_value=spotlight_md_path)
            trader.model.save_recommendation_spotlight_html = Mock(return_value=spotlight_html_path)

            result = trader.daily_run()

            self.assertEqual(result["selection_dir"], str(selection_dir))
            self.assertTrue((analysis_dir / "latest_recommendations.csv").exists())
            self.assertTrue((analysis_dir / "latest_recommendation_report.md").exists())
            self.assertTrue((analysis_dir / "latest_recommendation_report.html").exists())
            self.assertTrue((analysis_dir / "latest_recommendation_spotlight.md").exists())
            self.assertTrue((analysis_dir / "latest_recommendation_spotlight.html").exists())
