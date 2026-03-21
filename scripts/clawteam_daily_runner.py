#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from qlib_assistant_refactor.clawteam_adapter import default_data_dir
from qlib_assistant_refactor.clawteam_runner import ClawTeamDailyRunner
from qlib_assistant_refactor.config import AppConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the post-close workflow with ClawTeam task tracking.")
    parser.add_argument("--config-path", default="config.yaml", help="Path to YAML config.")
    parser.add_argument("--repo", default=".", help="Repository path.")
    parser.add_argument("--data-dir", default=None, help="ClawTeam data dir. Defaults to .clawteam-workbench under repo.")
    parser.add_argument("--clawteam-bin", default=".venv-clawteam/bin/clawteam", help="Path to clawteam executable.")
    parser.add_argument("--team-name", default=None, help="Optional fixed team name.")
    parser.add_argument("--leader-name", default="post-close-runner", help="Leader / runner agent name.")
    parser.add_argument("--market-timeout", type=int, default=1800, help="Timeout for sync-market in seconds.")
    parser.add_argument("--feed-timeout", type=int, default=1800, help="Timeout for fundamentals/events/gate in seconds.")
    parser.add_argument("--train-timeout", type=int, default=7200, help="Timeout for train-start in seconds.")
    parser.add_argument("--report-timeout", type=int, default=1800, help="Timeout for export-reports in seconds.")
    parser.add_argument("--market-limit", type=int, default=None, help="Only sync the first N instruments for market.")
    parser.add_argument("--fundamentals-limit", type=int, default=None, help="Only sync the first N instruments for fundamentals.")
    parser.add_argument("--events-limit", type=int, default=None, help="Only sync the first N instruments for events.")
    parser.add_argument("--event-lookback-days", type=int, default=3, help="How many recent calendar days to include for events.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    repo_path = Path(args.repo).expanduser().resolve()
    config = AppConfig.from_yaml(args.config_path)
    data_dir = Path(args.data_dir).expanduser().resolve() if args.data_dir else default_data_dir(repo_path)
    runner = ClawTeamDailyRunner(
        config=config,
        repo_path=repo_path,
        clawteam_bin=args.clawteam_bin,
        data_dir=data_dir,
        team_name=args.team_name,
        leader_name=args.leader_name,
        market_timeout_seconds=args.market_timeout,
        feed_timeout_seconds=args.feed_timeout,
        train_timeout_seconds=args.train_timeout,
        report_timeout_seconds=args.report_timeout,
        market_limit=args.market_limit,
        fundamentals_limit=args.fundamentals_limit,
        events_limit=args.events_limit,
        event_lookback_days=args.event_lookback_days,
    )
    summary = runner.run()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
