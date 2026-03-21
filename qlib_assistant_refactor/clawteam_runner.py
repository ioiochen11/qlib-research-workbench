from __future__ import annotations

import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from .clawteam_adapter import ClawTaskTemplate, build_post_close_task_templates
from .config import AppConfig


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float
    timed_out: bool = False


@dataclass(frozen=True)
class TaskExecutionResult:
    key: str
    task_id: str
    status: str
    description: str
    log_path: str
    output: dict[str, str]
    returncode: int
    timed_out: bool = False


def parse_kv_output(text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _bool_text(value: str | None) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _truncate_description(value: str, max_length: int = 240) -> str:
    compact = " ".join(value.split())
    if len(compact) <= max_length:
        return compact
    return compact[: max_length - 3] + "..."


def _default_command_runner(
    *,
    repo_path: Path,
    script_path: Path,
    log_path: Path,
    timeout_seconds: int,
    args: list[str],
) -> CommandResult:
    started_at = time.monotonic()
    cmd = ["bash", str(script_path), *args]
    try:
        completed = subprocess.run(
            cmd,
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            "\n".join(
                [
                    f"command={' '.join(cmd)}",
                    f"returncode={completed.returncode}",
                    "",
                    "STDOUT",
                    completed.stdout,
                    "",
                    "STDERR",
                    completed.stderr,
                ]
            ),
            encoding="utf-8",
        )
        return CommandResult(
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            duration_seconds=time.monotonic() - started_at,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            "\n".join(
                [
                    f"command={' '.join(cmd)}",
                    "returncode=124",
                    f"timed_out_after={timeout_seconds}",
                    "",
                    "STDOUT",
                    stdout,
                    "",
                    "STDERR",
                    stderr,
                ]
            ),
            encoding="utf-8",
        )
        return CommandResult(
            returncode=124,
            stdout=stdout,
            stderr=stderr,
            duration_seconds=time.monotonic() - started_at,
            timed_out=True,
        )


class ClawTeamCLIClient:
    def __init__(self, clawteam_bin: Path, data_dir: Path):
        self.clawteam_bin = clawteam_bin
        self.data_dir = data_dir

    def _run(self, args: list[str], *, json_output: bool) -> dict[str, object] | str:
        cmd = [str(self.clawteam_bin), "--data-dir", str(self.data_dir)]
        if json_output:
            cmd.append("--json")
        cmd.extend(args)
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if json_output:
            return json.loads(result.stdout)
        return result.stdout

    def spawn_team(self, team_name: str, leader_name: str, description: str) -> dict[str, object]:
        return self._run(
            ["team", "spawn-team", team_name, "-d", description, "-n", leader_name],
            json_output=True,
        )

    def create_task(
        self,
        *,
        team_name: str,
        template: ClawTaskTemplate,
        blocked_by_ids: list[str],
    ) -> dict[str, object]:
        args = [
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
            args.extend(["--blocked-by", ",".join(blocked_by_ids)])
        return self._run(args, json_output=True)

    def update_task(
        self,
        *,
        team_name: str,
        task_id: str,
        status: str,
        owner: str,
        description: Optional[str] = None,
    ) -> None:
        args = ["task", "update", team_name, task_id, "-s", status, "-o", owner]
        if description is not None:
            args.extend(["-d", description])
        self._run(args, json_output=False)


class ClawTeamDailyRunner:
    def __init__(
        self,
        *,
        config: AppConfig,
        repo_path: str | Path,
        clawteam_bin: str | Path,
        data_dir: str | Path,
        team_name: Optional[str] = None,
        leader_name: str = "post-close-runner",
        market_timeout_seconds: int = 1800,
        feed_timeout_seconds: int = 1800,
        train_timeout_seconds: int = 7200,
        report_timeout_seconds: int = 1800,
        market_limit: Optional[int] = None,
        fundamentals_limit: Optional[int] = None,
        events_limit: Optional[int] = None,
        event_lookback_days: int = 3,
        client: Optional[ClawTeamCLIClient] = None,
        command_runner: Optional[Callable[..., CommandResult]] = None,
        task_templates: Optional[list[ClawTaskTemplate]] = None,
        now_fn: Optional[Callable[[], datetime]] = None,
    ):
        self.config = config
        self.repo_path = Path(repo_path).expanduser().resolve()
        self.clawteam_bin = Path(clawteam_bin).expanduser().resolve()
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.team_name = team_name
        self.leader_name = leader_name
        self.market_timeout_seconds = market_timeout_seconds
        self.feed_timeout_seconds = feed_timeout_seconds
        self.train_timeout_seconds = train_timeout_seconds
        self.report_timeout_seconds = report_timeout_seconds
        self.market_limit = market_limit
        self.fundamentals_limit = fundamentals_limit
        self.events_limit = events_limit
        self.event_lookback_days = event_lookback_days
        self.client = client or ClawTeamCLIClient(self.clawteam_bin, self.data_dir)
        self.command_runner = command_runner or _default_command_runner
        self.now_fn = now_fn or datetime.now
        include_refresh = config.stock_pool == "sse180" or config.sync_universe == "sse180"
        self.task_templates = task_templates or build_post_close_task_templates(
            include_refresh_sse180=include_refresh
        )

    def run(self) -> dict[str, object]:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        team_name = self.team_name or f"qlib-post-close-{self.now_fn().strftime('%Y%m%d-%H%M%S')}"
        run_dir = self.data_dir / "runs" / team_name
        logs_dir = run_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        self.client.spawn_team(
            team_name=team_name,
            leader_name=self.leader_name,
            description="qlib-research-workbench ClawTeam daily runner",
        )

        task_ids: dict[str, str] = {}
        for template in self.task_templates:
            blocked_by_ids = [task_ids[item] for item in template.blocked_by]
            task_info = self.client.create_task(
                team_name=team_name,
                template=template,
                blocked_by_ids=blocked_by_ids,
            )
            task_ids[template.key] = str(task_info["id"])

        results: dict[str, TaskExecutionResult] = {}

        refresh_template = self._template_for_key("refresh")
        if refresh_template is not None:
            refresh_result = self._run_single_task(
                team_name=team_name,
                task_id=task_ids["refresh"],
                template=refresh_template,
                logs_dir=logs_dir,
                timeout_seconds=self.feed_timeout_seconds,
                args=[],
            )
            results["refresh"] = refresh_result
            if refresh_result.status != "completed":
                self._block_pending_tasks(
                    team_name=team_name,
                    task_ids=task_ids,
                    keys_to_block=[item.key for item in self.task_templates if item.key != "refresh"],
                    reason=f"前置股票池刷新失败: {refresh_result.description}",
                )
                return self._write_summary(team_name, run_dir, task_ids, results, skipped=False)

        market_result = self._run_single_task(
            team_name=team_name,
            task_id=task_ids["market"],
            template=self._template_for_key("market"),
            logs_dir=logs_dir,
            timeout_seconds=self.market_timeout_seconds,
            args=self._market_args(),
        )
        results["market"] = market_result
        if market_result.status != "completed":
            self._block_pending_tasks(
                team_name=team_name,
                task_ids=task_ids,
                keys_to_block=self._remaining_keys(results),
                reason=f"行情同步失败: {market_result.description}",
            )
            return self._write_summary(team_name, run_dir, task_ids, results, skipped=False)

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                "fundamentals": executor.submit(
                    self._run_single_task,
                    team_name=team_name,
                    task_id=task_ids["fundamentals"],
                    template=self._template_for_key("fundamentals"),
                    logs_dir=logs_dir,
                    timeout_seconds=self.feed_timeout_seconds,
                    args=self._fundamentals_args(market_result.output),
                ),
                "events": executor.submit(
                    self._run_single_task,
                    team_name=team_name,
                    task_id=task_ids["events"],
                    template=self._template_for_key("events"),
                    logs_dir=logs_dir,
                    timeout_seconds=self.feed_timeout_seconds,
                    args=self._events_args(market_result.output),
                ),
            }
            for key, future in futures.items():
                results[key] = future.result()

        failed_feed = next((item for item in ["fundamentals", "events"] if results[item].status != "completed"), None)
        if failed_feed is not None:
            self._block_pending_tasks(
                team_name=team_name,
                task_ids=task_ids,
                keys_to_block=["gate", "train", "reports"],
                reason=f"{failed_feed} 任务失败: {results[failed_feed].description}",
            )
            return self._write_summary(team_name, run_dir, task_ids, results, skipped=False)

        gate_result = self._run_single_task(
            team_name=team_name,
            task_id=task_ids["gate"],
            template=self._template_for_key("gate"),
            logs_dir=logs_dir,
            timeout_seconds=self.feed_timeout_seconds,
            args=self._gate_args(market_result.output),
        )
        results["gate"] = gate_result
        gate_eligible = _bool_text(gate_result.output.get("eligible_for_daily_run"))
        if gate_result.status != "completed":
            self._block_pending_tasks(
                team_name=team_name,
                task_ids=task_ids,
                keys_to_block=["train", "reports"],
                reason=f"freshness gate 执行失败: {gate_result.description}",
            )
            return self._write_summary(team_name, run_dir, task_ids, results, skipped=False)
        if not gate_eligible:
            skip_reason = gate_result.output.get("validation_errors") or "freshness gate 未通过"
            self._block_pending_tasks(
                team_name=team_name,
                task_ids=task_ids,
                keys_to_block=["train", "reports"],
                reason=f"已跳过: {skip_reason}",
            )
            return self._write_summary(team_name, run_dir, task_ids, results, skipped=True)

        train_result = self._run_single_task(
            team_name=team_name,
            task_id=task_ids["train"],
            template=self._template_for_key("train"),
            logs_dir=logs_dir,
            timeout_seconds=self.train_timeout_seconds,
            args=[],
        )
        results["train"] = train_result
        if train_result.status != "completed":
            self._block_pending_tasks(
                team_name=team_name,
                task_ids=task_ids,
                keys_to_block=["reports"],
                reason=f"训练失败: {train_result.description}",
            )
            return self._write_summary(team_name, run_dir, task_ids, results, skipped=False)

        report_result = self._run_single_task(
            team_name=team_name,
            task_id=task_ids["reports"],
            template=self._template_for_key("reports"),
            logs_dir=logs_dir,
            timeout_seconds=self.report_timeout_seconds,
            args=[],
        )
        results["reports"] = report_result
        return self._write_summary(team_name, run_dir, task_ids, results, skipped=False)

    def _template_for_key(self, key: str) -> Optional[ClawTaskTemplate]:
        for item in self.task_templates:
            if item.key == key:
                return item
        return None

    def _run_single_task(
        self,
        *,
        team_name: str,
        task_id: str,
        template: ClawTaskTemplate | None,
        logs_dir: Path,
        timeout_seconds: int,
        args: list[str],
    ) -> TaskExecutionResult:
        if template is None:
            raise ValueError("template is required")
        log_path = logs_dir / f"{template.key}.log"
        self.client.update_task(
            team_name=team_name,
            task_id=task_id,
            status="in_progress",
            owner=template.agent_name,
            description=template.description,
        )
        result = self.command_runner(
            repo_path=self.repo_path,
            script_path=self.repo_path / template.run_script,
            log_path=log_path,
            timeout_seconds=timeout_seconds,
            args=args,
        )
        parsed = parse_kv_output(result.stdout)
        status = "completed" if result.returncode == 0 else "blocked"
        description = (
            self._success_description(template.key, parsed, result)
            if status == "completed"
            else self._failure_description(template.key, parsed, result)
        )
        self.client.update_task(
            team_name=team_name,
            task_id=task_id,
            status=status,
            owner=template.agent_name,
            description=description,
        )
        return TaskExecutionResult(
            key=template.key,
            task_id=task_id,
            status=status,
            description=description,
            log_path=str(log_path),
            output=parsed,
            returncode=result.returncode,
            timed_out=result.timed_out,
        )

    def _success_description(self, task_key: str, parsed: dict[str, str], result: CommandResult) -> str:
        if task_key == "refresh":
            return _truncate_description(
                f"股票池已刷新，instrument_count={parsed.get('instrument_count', 'unknown')}"
            )
        if task_key == "market":
            return _truncate_description(
                f"行情完成，as_of_date={parsed.get('as_of_date', 'unknown')}，written_csv={parsed.get('written_csv', 'unknown')}"
            )
        if task_key == "fundamentals":
            return _truncate_description(
                f"财报完成，record_count={parsed.get('record_count', 'unknown')}，coverage_ratio={parsed.get('coverage_ratio', 'unknown')}"
            )
        if task_key == "events":
            return _truncate_description(
                f"事件完成，record_count={parsed.get('record_count', 'unknown')}，coverage_ratio={parsed.get('coverage_ratio', 'unknown')}"
            )
        if task_key == "gate":
            return _truncate_description(
                f"gate 完成，eligible_for_daily_run={parsed.get('eligible_for_daily_run', 'unknown')}，validation_status={parsed.get('validation_status', 'unknown')}"
            )
        if task_key == "train":
            return _truncate_description(
                f"训练完成，task_count={parsed.get('task_count', 'unknown')}，elapsed_seconds={parsed.get('elapsed_seconds', f'{result.duration_seconds:.1f}')}"
            )
        if task_key == "reports":
            return _truncate_description(
                "日报导出完成"
                + (f"，selection_dir={parsed.get('selection_dir')}" if parsed.get("selection_dir") else "")
            )
        return "完成"

    def _failure_description(self, task_key: str, parsed: dict[str, str], result: CommandResult) -> str:
        if result.timed_out:
            return _truncate_description(f"{task_key} 超时，returncode={result.returncode}")
        if parsed.get("validation_errors"):
            return _truncate_description(parsed["validation_errors"])
        stderr_lines = result.stderr.strip().splitlines()
        if stderr_lines:
            return _truncate_description(stderr_lines[-1])
        stdout_lines = result.stdout.strip().splitlines()
        if stdout_lines:
            return _truncate_description(stdout_lines[-1])
        return _truncate_description(f"{task_key} 失败，returncode={result.returncode}")

    def _block_pending_tasks(
        self,
        *,
        team_name: str,
        task_ids: dict[str, str],
        keys_to_block: list[str],
        reason: str,
    ) -> None:
        for template in self.task_templates:
            if template.key not in keys_to_block:
                continue
            self.client.update_task(
                team_name=team_name,
                task_id=task_ids[template.key],
                status="blocked",
                owner=template.agent_name,
                description=_truncate_description(reason),
            )

    def _remaining_keys(self, results: dict[str, TaskExecutionResult]) -> list[str]:
        completed_keys = set(results)
        return [item.key for item in self.task_templates if item.key not in completed_keys]

    def _market_args(self) -> list[str]:
        args: list[str] = []
        if self.market_limit is not None:
            args.extend(["--limit", str(self.market_limit)])
        return args

    def _fundamentals_args(self, market_output: dict[str, str]) -> list[str]:
        args: list[str] = []
        if market_output.get("as_of_date"):
            args.extend(["--date", market_output["as_of_date"]])
        if self.fundamentals_limit is not None:
            args.extend(["--limit", str(self.fundamentals_limit)])
        return args

    def _events_args(self, market_output: dict[str, str]) -> list[str]:
        args: list[str] = []
        if market_output.get("as_of_date"):
            args.extend(["--date", market_output["as_of_date"]])
        args.extend(["--lookback-days", str(self.event_lookback_days)])
        if self.events_limit is not None:
            args.extend(["--limit", str(self.events_limit)])
        return args

    def _gate_args(self, market_output: dict[str, str]) -> list[str]:
        if market_output.get("as_of_date"):
            return ["--date", market_output["as_of_date"]]
        return []

    def _write_summary(
        self,
        team_name: str,
        run_dir: Path,
        task_ids: dict[str, str],
        results: dict[str, TaskExecutionResult],
        *,
        skipped: bool,
    ) -> dict[str, object]:
        summary = {
            "team_name": team_name,
            "data_dir": str(self.data_dir),
            "run_dir": str(run_dir),
            "skipped": skipped,
            "tasks": {key: asdict(value) for key, value in results.items()},
            "task_ids": task_ids,
            "board_command": f"{self.clawteam_bin} --data-dir {self.data_dir} board show {team_name}",
            "wait_command": f"{self.clawteam_bin} --data-dir {self.data_dir} task wait {team_name}",
        }
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        summary["summary_path"] = str(summary_path)
        return summary
