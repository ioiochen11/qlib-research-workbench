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

    def test_roll_cli_accepts_model_backtest(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["model", "backtest"])
        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "backtest")

    def test_roll_cli_accepts_train_plan_limit(self) -> None:
        parser = build_roll_parser()
        args = parser.parse_args(["train", "plan", "--limit", "3"])
        self.assertEqual(args.command, "train")
        self.assertEqual(args.train_command, "plan")
        self.assertEqual(args.limit, 3)

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
