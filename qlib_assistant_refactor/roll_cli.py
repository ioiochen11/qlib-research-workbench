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
    refresh_sse180_parser = data_subparsers.add_parser("refresh-sse180", help="Fetch and update the SSE180 instrument universe file.")
    refresh_sse180_parser.add_argument("--as-of-date", default=None, help="Universe end date in YYYY-MM-DD format.")
    sync_parser = data_subparsers.add_parser("sync-akshare", help="Sync daily bars from AkShare into local qlib data.")
    sync_parser.add_argument("--start-date", default=None, help="Sync start date in YYYY-MM-DD format.")
    sync_parser.add_argument("--end-date", default=None, help="Sync end date in YYYY-MM-DD format.")
    sync_parser.add_argument("--limit", type=int, default=None, help="Only sync the first N symbols.")
    data_subparsers.add_parser("verify", help="Verify local dataset structure.")
    data_subparsers.add_parser("qlib-check", help="Initialize qlib and read sample features.")

    subparsers.add_parser("daily-run", help="Run the post-close daily pipeline for SSE180 low-price recommendations.")

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
    entry_parser = model_subparsers.add_parser("entry-plan", help="Build rule-based entry price plans for candidates.")
    entry_parser.add_argument("--limit", type=int, default=10, help="How many candidates to include.")
    entry_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    entry_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    entry_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    entry_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    save_entry_parser = model_subparsers.add_parser("save-entry-plan", help="Save rule-based entry price plans to CSV.")
    save_entry_parser.add_argument("--limit", type=int, default=10, help="How many candidates to include.")
    save_entry_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    save_entry_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    save_entry_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    save_entry_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    recommendations_parser = model_subparsers.add_parser(
        "recommendations",
        help="Show validation-friendly recommendation rows with entry levels and next-day checks.",
    )
    recommendations_parser.add_argument("--limit", type=int, default=10, help="How many candidates to include.")
    recommendations_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    recommendations_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    recommendations_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    recommendations_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    save_recommendations_parser = model_subparsers.add_parser(
        "save-recommendations",
        help="Save validation-friendly recommendation rows to CSV.",
    )
    save_recommendations_parser.add_argument("--limit", type=int, default=10, help="How many candidates to include.")
    save_recommendations_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    save_recommendations_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    save_recommendations_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    save_recommendations_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    report_recommendations_parser = model_subparsers.add_parser(
        "recommendation-report",
        help="Render a Markdown validation report for the current recommendation sheet.",
    )
    report_recommendations_parser.add_argument("--limit", type=int, default=10, help="How many candidates to include.")
    report_recommendations_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    report_recommendations_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    report_recommendations_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    report_recommendations_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    save_report_recommendations_parser = model_subparsers.add_parser(
        "save-recommendation-report",
        help="Save a Markdown validation report for the current recommendation sheet.",
    )
    save_report_recommendations_parser.add_argument("--limit", type=int, default=10, help="How many candidates to include.")
    save_report_recommendations_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    save_report_recommendations_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    save_report_recommendations_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    save_report_recommendations_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    html_recommendations_parser = model_subparsers.add_parser(
        "recommendation-html",
        help="Render an HTML validation report for the current recommendation sheet.",
    )
    html_recommendations_parser.add_argument("--limit", type=int, default=10, help="How many candidates to include.")
    html_recommendations_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    html_recommendations_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    html_recommendations_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    html_recommendations_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    save_html_recommendations_parser = model_subparsers.add_parser(
        "save-recommendation-html",
        help="Save an HTML validation report for the current recommendation sheet.",
    )
    save_html_recommendations_parser.add_argument("--limit", type=int, default=10, help="How many candidates to include.")
    save_html_recommendations_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    save_html_recommendations_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    save_html_recommendations_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    save_html_recommendations_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    spotlight_parser = model_subparsers.add_parser(
        "recommendation-spotlight",
        help="Render a focused Markdown interpretation for the top recommendation candidates.",
    )
    spotlight_parser.add_argument("--limit", type=int, default=3, help="How many candidates to include.")
    spotlight_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    spotlight_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    spotlight_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    spotlight_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    save_spotlight_parser = model_subparsers.add_parser(
        "save-recommendation-spotlight",
        help="Save a focused Markdown interpretation for the top recommendation candidates.",
    )
    save_spotlight_parser.add_argument("--limit", type=int, default=3, help="How many candidates to include.")
    save_spotlight_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    save_spotlight_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    save_spotlight_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    save_spotlight_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    spotlight_html_parser = model_subparsers.add_parser(
        "recommendation-spotlight-html",
        help="Render a focused HTML interpretation for the top recommendation candidates.",
    )
    spotlight_html_parser.add_argument("--limit", type=int, default=3, help="How many candidates to include.")
    spotlight_html_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    spotlight_html_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    spotlight_html_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    spotlight_html_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
    save_spotlight_html_parser = model_subparsers.add_parser(
        "save-recommendation-spotlight-html",
        help="Save a focused HTML interpretation for the top recommendation candidates.",
    )
    save_spotlight_html_parser.add_argument("--limit", type=int, default=3, help="How many candidates to include.")
    save_spotlight_html_parser.add_argument("--date", default=None, help="Target date in YYYY-MM-DD format.")
    save_spotlight_html_parser.add_argument("--selection-dir", default=None, help="Selection directory; defaults to latest.")
    save_spotlight_html_parser.add_argument("--raw", action="store_true", help="Use *_ret.csv instead of *_filter_ret.csv.")
    save_spotlight_html_parser.add_argument("--max-price", type=float, default=None, help="Only keep stocks with close price <= this value.")
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
        if args.data_command == "refresh-sse180":
            print_kv(app.data.refresh_sse180_universe(as_of_date=args.as_of_date))
            return 0
        if args.data_command == "sync-akshare":
            print_kv(app.data.sync_akshare(start_date=args.start_date, end_date=args.end_date, limit=args.limit))
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

    if args.command == "daily-run":
        info = app.daily_run()
        if info.get("refresh_info"):
            print_kv({f"refresh_{k}": v for k, v in info["refresh_info"].items()})
        print_kv({f"sync_{k}": v for k, v in info["sync_info"].items()})
        print_kv({f"train_{k}": v for k, v in info["train_info"].items()})
        print(f"selection_dir={info['selection_dir']}")
        print(f"recommendations_csv={info['recommendations_csv']}")
        print(f"recommendation_report_md={info['recommendation_report_md']}")
        print(f"recommendation_report_html={info['recommendation_report_html']}")
        print(f"recommendation_spotlight_md={info['recommendation_spotlight_md']}")
        print(f"recommendation_spotlight_html={info['recommendation_spotlight_html']}")
        print(f"latest_recommendations_csv={info['latest_recommendations_csv']}")
        print(f"latest_recommendation_report_md={info['latest_recommendation_report_md']}")
        print(f"latest_recommendation_report_html={info['latest_recommendation_report_html']}")
        print(f"latest_recommendation_spotlight_md={info['latest_recommendation_spotlight_md']}")
        print(f"latest_recommendation_spotlight_html={info['latest_recommendation_spotlight_html']}")
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
        if args.model_command == "entry-plan":
            df = app.model.entry_plan(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(df.to_string(index=False))
            return 0
        if args.model_command == "save-entry-plan":
            output = app.model.save_entry_plan(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(f"saved={output}")
            return 0
        if args.model_command == "recommendations":
            df = app.model.recommendation_sheet(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(df.to_string(index=False))
            return 0
        if args.model_command == "save-recommendations":
            output = app.model.save_recommendation_sheet(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(f"saved={output}")
            return 0
        if args.model_command == "recommendation-report":
            report = app.model.recommendation_report(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(report, end="")
            return 0
        if args.model_command == "save-recommendation-report":
            output = app.model.save_recommendation_report(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(f"saved={output}")
            return 0
        if args.model_command == "recommendation-html":
            report = app.model.recommendation_html(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(report, end="")
            return 0
        if args.model_command == "save-recommendation-html":
            output = app.model.save_recommendation_html(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(f"saved={output}")
            return 0
        if args.model_command == "recommendation-spotlight":
            report = app.model.recommendation_spotlight(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(report)
            return 0
        if args.model_command == "save-recommendation-spotlight":
            output = app.model.save_recommendation_spotlight(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(f"saved={output}")
            return 0
        if args.model_command == "recommendation-spotlight-html":
            report = app.model.recommendation_spotlight_html(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(report)
            return 0
        if args.model_command == "save-recommendation-spotlight-html":
            output = app.model.save_recommendation_spotlight_html(
                limit=args.limit,
                date=args.date,
                selection_dir=args.selection_dir,
                filtered=not args.raw,
                max_price=args.max_price,
            )
            print(f"saved={output}")
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
