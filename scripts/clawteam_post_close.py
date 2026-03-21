#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from qlib_assistant_refactor.clawteam_adapter import (
    build_leader_prompt,
    build_post_close_task_templates,
    build_worker_prompt,
    default_data_dir,
)


def run_json(clawteam_bin: Path, data_dir: Path, args: list[str]) -> dict:
    cmd = [str(clawteam_bin), "--data-dir", str(data_dir), "--json", *args]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def run_plain(clawteam_bin: Path, data_dir: Path, args: list[str]) -> None:
    cmd = [str(clawteam_bin), "--data-dir", str(data_dir), *args]
    subprocess.run(cmd, check=True)


def command_exists(command_parts: list[str]) -> bool:
    if not command_parts:
        return False
    binary = command_parts[0]
    if Path(binary).exists():
        return True
    return shutil.which(binary) is not None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap a minimal ClawTeam post-close workflow.")
    parser.add_argument("--team-name", default=None, help="ClawTeam team name. Defaults to qlib-post-close-YYYYMMDD-HHMMSS.")
    parser.add_argument("--leader-name", default="post-close-leader", help="Leader agent name.")
    parser.add_argument("--backend", default="subprocess", choices=["subprocess", "tmux"], help="ClawTeam spawn backend.")
    parser.add_argument(
        "--command",
        default="claude",
        help="Agent command used by clawteam spawn, for example 'claude' or 'codex'.",
    )
    parser.add_argument("--repo", default=".", help="Repository path.")
    parser.add_argument("--data-dir", default=None, help="ClawTeam data dir. Defaults to .clawteam-workbench under repo.")
    parser.add_argument(
        "--clawteam-bin",
        default=".venv-clawteam/bin/clawteam",
        help="Path to clawteam executable.",
    )
    parser.add_argument("--spawn-workers", action="store_true", help="Actually spawn leader and worker agents.")
    parser.add_argument("--workspace", action="store_true", help="Ask clawteam to create git worktree workspaces.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_path = Path(args.repo).expanduser().resolve()
    clawteam_bin = Path(args.clawteam_bin).expanduser().resolve()
    data_dir = Path(args.data_dir).expanduser().resolve() if args.data_dir else default_data_dir(repo_path)
    team_name = args.team_name or f"qlib-post-close-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    command_parts = shlex.split(args.command)

    if not clawteam_bin.exists():
        print(f"Missing clawteam executable: {clawteam_bin}", file=sys.stderr)
        print("Run `bash scripts/setup_clawteam_env.sh` first.", file=sys.stderr)
        return 1
    if args.backend == "tmux" and shutil.which("tmux") is None:
        print("tmux is not installed or not on PATH.", file=sys.stderr)
        return 1
    if args.spawn_workers and not command_exists(command_parts):
        print(f"Agent command is not available: {args.command}", file=sys.stderr)
        print("Install the agent CLI first or rerun without --spawn-workers.", file=sys.stderr)
        return 1

    data_dir.mkdir(parents=True, exist_ok=True)
    team_info = run_json(
        clawteam_bin,
        data_dir,
        ["team", "spawn-team", team_name, "-d", "qlib-research-workbench post-close pipeline", "-n", args.leader_name],
    )

    templates = build_post_close_task_templates()
    task_ids: dict[str, str] = {}
    task_payloads: list[dict[str, str]] = []
    for template in templates:
        blocked_by_ids = [task_ids[key] for key in template.blocked_by]
        create_args = [
            "task",
            "create",
            team_name,
            template.subject,
            "-d",
            template.description,
            "-o",
            template.agent_name,
        ]
        if blocked_by_ids:
            create_args.extend(["--blocked-by", ",".join(blocked_by_ids)])
        task_info = run_json(clawteam_bin, data_dir, create_args)
        task_ids[template.key] = task_info["id"]
        task_payloads.append(
            {
                "key": template.key,
                "task_id": task_info["id"],
                "subject": template.subject,
                "agent_name": template.agent_name,
                "run_script": template.run_script,
            }
        )

    if args.spawn_workers:
        leader_prompt = build_leader_prompt(team_name=team_name, repo_path=str(repo_path))
        leader_cmd = [
            "spawn",
            args.backend,
            *command_parts,
            "--team",
            team_name,
            "--agent-name",
            args.leader_name,
            "--repo",
            str(repo_path),
            "--task",
            leader_prompt,
        ]
        if args.workspace:
            leader_cmd.append("--workspace")
        run_plain(clawteam_bin, data_dir, leader_cmd)

        for template in templates:
            prompt = build_worker_prompt(
                team_name=team_name,
                task_id=task_ids[template.key],
                template=template,
                dependency_task_ids=[task_ids[key] for key in template.blocked_by],
                clawteam_bin=str(clawteam_bin),
                data_dir=str(data_dir),
                repo_path=str(repo_path),
            )
            spawn_args = [
                "spawn",
                args.backend,
                *command_parts,
                "--team",
                team_name,
                "--agent-name",
                template.agent_name,
                "--repo",
                str(repo_path),
                "--task",
                prompt,
            ]
            if args.workspace:
                spawn_args.append("--workspace")
            run_plain(clawteam_bin, data_dir, spawn_args)

    summary = {
        "team": team_name,
        "leader": team_info.get("leaderName", args.leader_name),
        "data_dir": str(data_dir),
        "repo": str(repo_path),
        "spawned": bool(args.spawn_workers),
        "backend": args.backend,
        "command": args.command,
        "tasks": task_payloads,
        "board_command": f"{clawteam_bin} --data-dir {data_dir} board show {team_name}",
        "wait_command": f"{clawteam_bin} --data-dir {data_dir} task wait {team_name}",
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
