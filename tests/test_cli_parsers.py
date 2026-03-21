from io import StringIO
from unittest import TestCase
from unittest.mock import patch

from qlib_assistant_refactor.cli import build_parser as build_data_parser, main as data_main
from qlib_assistant_refactor.roll_cli import build_parser as build_roll_parser, main as roll_main


class CLIParserTests(TestCase):
    def test_data_cli_accepts_probe(self) -> None:
        parser = build_data_parser()
        args = parser.parse_args(["probe"])
        self.assertEqual(args.command, "probe")

    def test_data_cli_accepts_extract(self) -> None:
        parser = build_data_parser()
        args = parser.parse_args(["extract", "--archive", "a.tar.gz", "--target-dir", "/tmp/x"])
        self.assertEqual(args.command, "extract")
        self.assertEqual(args.archive, "a.tar.gz")

    def test_data_cli_accepts_sync_akshare(self) -> None:
        parser = build_data_parser()
        args = parser.parse_args(["sync-akshare", "--limit", "5"])
        self.assertEqual(args.command, "sync-akshare")
        self.assertEqual(args.limit, 5)

    def test_roll_cli_accepts_model_backtest(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "backtest"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "backtest")

    def test_roll_cli_accepts_model_entry_plan(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "entry-plan", "--limit", "5", "--raw", "--max-price", "30"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "entry-plan")
        self.assertEqual(args.limit, 5)
        self.assertTrue(args.raw)
        self.assertEqual(args.max_price, 30)

    def test_roll_cli_accepts_model_save_entry_plan(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "save-entry-plan", "--limit", "5"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "save-entry-plan")
        self.assertEqual(args.limit, 5)

    def test_roll_cli_accepts_model_recommendations(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "recommendations", "--limit", "5"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "recommendations")
        self.assertEqual(args.limit, 5)

    def test_roll_cli_accepts_model_save_recommendations(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "save-recommendations", "--limit", "5", "--raw", "--max-price", "30"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "save-recommendations")
        self.assertEqual(args.limit, 5)
        self.assertTrue(args.raw)
        self.assertEqual(args.max_price, 30)

    def test_roll_cli_accepts_model_recommendation_report(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "recommendation-report", "--limit", "5"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "recommendation-report")
        self.assertEqual(args.limit, 5)

    def test_roll_cli_accepts_model_save_recommendation_report(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "save-recommendation-report", "--limit", "5", "--raw"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "save-recommendation-report")
        self.assertEqual(args.limit, 5)
        self.assertTrue(args.raw)

    def test_roll_cli_accepts_model_recommendation_html(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "recommendation-html", "--limit", "5"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "recommendation-html")
        self.assertEqual(args.limit, 5)

    def test_roll_cli_accepts_model_save_recommendation_html(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "save-recommendation-html", "--limit", "5", "--raw", "--max-price", "30"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "save-recommendation-html")
        self.assertEqual(args.limit, 5)
        self.assertTrue(args.raw)
        self.assertEqual(args.max_price, 30)

    def test_roll_cli_accepts_model_recommendation_spotlight(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "recommendation-spotlight", "--limit", "3"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "recommendation-spotlight")
        self.assertEqual(args.limit, 3)

    def test_roll_cli_accepts_model_save_recommendation_spotlight_html(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "save-recommendation-spotlight-html", "--limit", "3", "--raw"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "save-recommendation-spotlight-html")
        self.assertEqual(args.limit, 3)
        self.assertTrue(args.raw)

    def test_roll_cli_accepts_train_plan_limit(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["train", "plan", "--limit", "3"])
        self.assertEqual(args.command, "train")
        self.assertEqual(args.train_command, "plan")
        self.assertEqual(args.limit, 3)

    def test_roll_cli_accepts_data_sync_akshare(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["data", "sync-akshare", "--limit", "8"])
        self.assertEqual(args.command, "data")
        self.assertEqual(args.data_command, "sync-akshare")
        self.assertEqual(args.limit, 8)

    def test_roll_cli_accepts_data_sync_market(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["data", "sync-market", "--end-date", "2026-03-20"])
        self.assertEqual(args.command, "data")
        self.assertEqual(args.data_command, "sync-market")
        self.assertEqual(args.end_date, "2026-03-20")

    def test_roll_cli_accepts_data_sync_fundamentals(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["data", "sync-fundamentals", "--date", "2026-03-20"])
        self.assertEqual(args.command, "data")
        self.assertEqual(args.data_command, "sync-fundamentals")
        self.assertEqual(args.date, "2026-03-20")

    def test_roll_cli_accepts_data_verify_freshness(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["data", "verify-freshness", "--date", "2026-03-20"])
        self.assertEqual(args.command, "data")
        self.assertEqual(args.data_command, "verify-freshness")
        self.assertEqual(args.date, "2026-03-20")

    def test_roll_cli_accepts_refresh_sse180(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["data", "refresh-sse180", "--as-of-date", "2026-03-20"])
        self.assertEqual(args.command, "data")
        self.assertEqual(args.data_command, "refresh-sse180")
        self.assertEqual(args.as_of_date, "2026-03-20")

    def test_roll_cli_accepts_daily_run(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["daily-run"])
        self.assertEqual(args.command, "daily-run")

    def test_roll_cli_accepts_clawteam_runner(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["clawteam-runner", "--market-limit", "5"])
        self.assertEqual(args.command, "clawteam-runner")
        self.assertEqual(args.market_limit, 5)

    def test_data_main_accepts_argv(self) -> None:
        with patch("qlib_assistant_refactor.cli.AppConfig.from_yaml") as mock_from_yaml:
            config = mock_from_yaml.return_value
            with patch("qlib_assistant_refactor.cli.DataService") as mock_service_cls:
                service = mock_service_cls.return_value
                service.probe.return_value = []
                result = data_main(["probe"])

        self.assertEqual(result, 0)
        mock_from_yaml.assert_called_once_with("config.yaml")
        mock_service_cls.assert_called_once_with(config)

    def test_roll_main_accepts_argv(self) -> None:
        with patch("qlib_assistant_refactor.roll_cli.RollingTrader") as mock_app_cls:
            app = mock_app_cls.return_value
            app.data.status.return_value = {
                "local_calendar_date": "2026-03-19",
                "remote_publish_date": "2026-03-19",
                "needs_update": False,
            }
            with patch("sys.stdout", new_callable=StringIO) as stdout:
                result = roll_main(["data", "status"])

        self.assertEqual(result, 0)
        mock_app_cls.assert_called_once_with(config_path="config.yaml")
        self.assertIn("needs_update=False", stdout.getvalue())

    def test_roll_main_daily_run_accepts_argv(self) -> None:
        with patch("qlib_assistant_refactor.roll_cli.RollingTrader") as mock_app_cls:
            app = mock_app_cls.return_value
            app.daily_run.return_value = {
                "refresh_info": {"instrument_count": 180},
                "market_info": {"written_csv": 180},
                "fundamentals_info": {"record_count": 300},
                "events_info": {"record_count": 300},
                "freshness_info": {"eligible_for_daily_run": True},
                "sync_info": {"written_csv": 180},
                "train_info": {"task_count": 1},
                "manifest_dir": "/tmp/manifests/2026-03-20",
                "selection_dir": "/tmp/selection",
                "recommendations_csv": "/tmp/a.csv",
                "recommendation_report_md": "/tmp/a.md",
                "recommendation_report_html": "/tmp/a.html",
                "recommendation_spotlight_md": "/tmp/spot.md",
                "recommendation_spotlight_html": "/tmp/spot.html",
                "latest_recommendations_csv": "/tmp/latest.csv",
                "latest_recommendation_report_md": "/tmp/latest.md",
                "latest_recommendation_report_html": "/tmp/latest.html",
                "latest_recommendation_spotlight_md": "/tmp/latest-spot.md",
                "latest_recommendation_spotlight_html": "/tmp/latest-spot.html",
            }
            with patch("sys.stdout", new_callable=StringIO) as stdout:
                result = roll_main(["daily-run"])

        self.assertEqual(result, 0)
        self.assertIn("latest_recommendation_report_html=/tmp/latest.html", stdout.getvalue())
        self.assertIn("latest_recommendation_spotlight_html=/tmp/latest-spot.html", stdout.getvalue())

    def test_roll_main_daily_run_handles_skip(self) -> None:
        with patch("qlib_assistant_refactor.roll_cli.RollingTrader") as mock_app_cls:
            app = mock_app_cls.return_value
            app.daily_run.return_value = {
                "market_info": {"written_csv": 180},
                "fundamentals_info": {"record_count": 300},
                "events_info": {"record_count": 300},
                "freshness_info": {"eligible_for_daily_run": False},
                "daily_run_skipped": True,
                "skip_reason": "missing_manifest:events",
                "manifest_dir": "/tmp/manifests/2026-03-20",
                "selection_dir": None,
            }
            with patch("sys.stdout", new_callable=StringIO) as stdout:
                result = roll_main(["daily-run"])

        self.assertEqual(result, 0)
        self.assertIn("daily_run_skipped=True", stdout.getvalue())
        self.assertIn("manifest_dir=/tmp/manifests/2026-03-20", stdout.getvalue())

    def test_roll_main_clawteam_runner_accepts_argv(self) -> None:
        with patch("qlib_assistant_refactor.roll_cli.RollingTrader") as mock_app_cls:
            app = mock_app_cls.return_value
            app.config = object()
            with patch("qlib_assistant_refactor.roll_cli.ClawTeamDailyRunner") as mock_runner_cls:
                runner = mock_runner_cls.return_value
                runner.run.return_value = {
                    "team_name": "demo-team",
                    "run_dir": "/tmp/demo",
                    "summary_path": "/tmp/demo/summary.json",
                    "skipped": True,
                    "board_command": "clawteam board show demo-team",
                    "tasks": {
                        "market": {
                            "status": "completed",
                            "description": "行情完成",
                            "log_path": "/tmp/demo/logs/market.log",
                        }
                    },
                }
                with patch("sys.stdout", new_callable=StringIO) as stdout:
                    result = roll_main(["clawteam-runner", "--market-limit", "5"])

        self.assertEqual(result, 0)
        self.assertIn("team_name=demo-team", stdout.getvalue())
        self.assertIn("task_market_status=completed", stdout.getvalue())
