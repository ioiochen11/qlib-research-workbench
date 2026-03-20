from __future__ import annotations

import argparse

from .app import RollingTrader
from .qlib_check import run_qlib_smoke


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal qlibAssistant-compatible entrypoint.")
    parser.add_argument("--config-path", default="config.yaml", help="Path to YAML config.")

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("show-config", help="Show merged config.")

    data_parser = subparsers.add_parser("data", help="Data subcommands.")
    data_subparsers = data_parser.add_subparsers(dest="data_command", required=True)
    data_subparsers.add_parser("status", help="Show local and remote dataset status.")
    data_subparsers.add_parser("need-update", help="Print whether data update is needed.")
    update_parser = data_subparsers.add_parser("update", help="Download and extract latest dataset.")
    update_parser.add_argument("--proxy", default="A", help="Proxy alias A/B/C/D or a full prefix URL.")
    update_parser.add_argument("--force", action="store_true", help="Force update even if local date matches.")
    data_subparsers.add_parser("verify", help="Verify local dataset structure.")
    data_subparsers.add_parser("qlib-check", help="Initialize qlib and read sample features.")

    train_parser = subparsers.add_parser("train", help="Training subcommands.")
    train_subparsers = train_parser.add_subparsers(dest="train_command", required=True)
    plan_parser = train_subparsers.add_parser("plan", help="Generate rolling tasks preview.")
    plan_parser.add_argument("--limit", type=int, default=5, help="How many tasks to preview.")
    train_subparsers.add_parser("smoke", help="Train one minimal task and save artifacts.")
    start_parser = train_subparsers.add_parser("start", help="Train generated tasks.")
    start_parser.add_argument("--limit", type=int, default=None, help="Optionally cap generated tasks.")
    train_subparsers.add_parser("list-experiments", help="List qlib experiments in the MLflow store.")

    model_parser = subparsers.add_parser("model", help="Model analysis subcommands.")
    model_subparsers = model_parser.add_subparsers(dest="model_command", required=True)
    ls_parser = model_subparsers.add_parser("ls", help="List model experiments and recorders.")
    ls_parser.add_argument("--all", action="store_true", help="Include recorder details.")
    top_parser = model_subparsers.add_parser("top", help="Show top scored instruments from saved predictions.")
    top_parser.add_argument("--limit", type=int, default=20, help="Top N rows to show.")
    top_parser.add_argument("--date", default=None, help="Prediction date in YYYY-MM-DD format.")
    save_top_parser = model_subparsers.add_parser("save-top", help="Save top predictions to CSV.")
    save_top_parser.add_argument("--limit", type=int, default=20, help="Top N rows to save.")
    save_top_parser.add_argument("--date", default=None, help="Prediction date in YYYY-MM-DD format.")
    report_parser = model_subparsers.add_parser("report", help="Build per-day selection report CSVs.")
    report_parser.add_argument("--output-dir", default=None, help="Optional output directory.")
    review_parser = model_subparsers.add_parser("review", help="Review a selection report directory.")
    review_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    backtest_parser = model_subparsers.add_parser("backtest", help="Backtest top-k baskets from a selection report.")
    backtest_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    model_subparsers.add_parser("list-backups", help="List archived mlruns backups.")
    model_subparsers.add_parser("backup", help="Archive the current mlruns directory.")
    restore_parser = model_subparsers.add_parser("restore", help="Restore archived mlruns backups.")
    restore_parser.add_argument("--archive-name", default=None, help="Specific archive file name to restore.")
    restore_parser.add_argument("--all", action="store_true", help="Restore all archives in the backup folder.")
    return parser


def print_kv(data: dict[str, object]) -> None:
    for key, value in data.items():
        print(f"{key}={value}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    app = RollingTrader(config_path=args.config_path)

    if args.command == "show-config":
        print(app.show_config())
        return 0

    if args.command == "data":
        if args.data_command == "status":
            print_kv(app.data.status())
            return 0
        if args.data_command == "need-update":
            print(app.data.need_update())
            return 0
        if args.data_command == "update":
            print_kv(app.data.update(proxy=args.proxy, force=args.force))
            return 0
        if args.data_command == "verify":
            print_kv(app.data.verify())
            return 0
        if args.data_command == "qlib-check":
            info = run_qlib_smoke(app.config)
            print_kv({k: info[k] for k in ["provider_uri", "latest_trade_date", "csi300_count", "sample_instrument"]})
            print("sample_features=")
            print(info["sample_features"])
            return 0

    if args.command == "train":
        if args.train_command == "plan":
            info = app.train.plan(limit=args.limit)
            print_kv({k: info[k] for k in ["experiment_name", "task_count"]})
            for idx, segs in enumerate(info["preview"], start=1):
                print(f"task_{idx}_segments={segs}")
            return 0
        if args.train_command == "smoke":
            print_kv(app.train.smoke())
            return 0
        if args.train_command == "start":
            print_kv(app.train.start(limit=args.limit))
            return 0
        if args.train_command == "list-experiments":
            info = app.train.list_experiments()
            print_kv(info)
            return 0

    if args.command == "model":
        if args.model_command == "ls":
            info = app.model.list_models(include_recorders=args.all)
            print(f"count={info['count']}")
            for item in info["items"]:
                print(item)
            return 0
        if args.model_command == "top":
            df = app.model.top_predictions(limit=args.limit, date=args.date)
            print(df.to_string(index=False))
            return 0
        if args.model_command == "save-top":
            output = app.model.save_top_predictions(limit=args.limit, date=args.date)
            print(f"saved={output}")
            return 0
        if args.model_command == "report":
            output = app.model.selection_report(output_dir=args.output_dir)
            print(f"saved={output}")
            return 0
        if args.model_command == "review":
            output = app.model.review_report(selection_dir=args.selection_dir)
            print(f"saved={output}")
            return 0
        if args.model_command == "backtest":
            output = app.model.backtest_report(selection_dir=args.selection_dir)
            print(f"saved={output}")
            return 0
        if args.model_command == "list-backups":
            info = app.model.list_backups()
            print(f"count={info['count']}")
            for item in info["items"]:
                print(item)
            return 0
        if args.model_command == "backup":
            output = app.model.backup_mlruns()
            print(f"saved={output}")
            return 0
        if args.model_command == "restore":
            restored = app.model.restore_mlruns(archive_name=args.archive_name, restore_all=args.all)
            for path in restored:
                print(f"restored={path}")
            return 0

    parser.error("Unknown command")
    return 2
