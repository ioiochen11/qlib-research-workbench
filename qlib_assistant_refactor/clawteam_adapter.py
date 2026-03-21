from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ClawTaskTemplate:
    key: str
    subject: str
    description: str
    agent_name: str
    run_script: str
    blocked_by: tuple[str, ...] = ()


def build_post_close_task_templates(*, include_refresh_sse180: bool = False) -> list[ClawTaskTemplate]:
    templates: list[ClawTaskTemplate] = []
    market_blocked_by: tuple[str, ...] = ()
    if include_refresh_sse180:
        templates.append(
            ClawTaskTemplate(
                key="refresh",
                subject="refresh-sse180",
                description="刷新上证 180 股票池文件",
                agent_name="universe-refresh",
                run_script="scripts/clawteam_refresh_sse180.sh",
            )
        )
        market_blocked_by = ("refresh",)

    templates.extend([
        ClawTaskTemplate(
            key="market",
            subject="sync-market",
            description="同步并校验收盘后行情数据",
            agent_name="market-sync",
            run_script="scripts/clawteam_market_sync.sh",
            blocked_by=market_blocked_by,
        ),
        ClawTaskTemplate(
            key="fundamentals",
            subject="sync-fundamentals",
            description="同步收盘后的财报与估值结构化数据",
            agent_name="fundamentals-sync",
            run_script="scripts/clawteam_fundamentals_sync.sh",
            blocked_by=("market",),
        ),
        ClawTaskTemplate(
            key="events",
            subject="sync-events",
            description="同步收盘后的公告和新闻结构化数据",
            agent_name="events-sync",
            run_script="scripts/clawteam_events_sync.sh",
            blocked_by=("market",),
        ),
        ClawTaskTemplate(
            key="gate",
            subject="verify-freshness",
            description="执行收盘后 freshness gate，确认今天是否允许正式出报",
            agent_name="freshness-gate",
            run_script="scripts/clawteam_verify_freshness.sh",
            blocked_by=("fundamentals", "events"),
        ),
        ClawTaskTemplate(
            key="train",
            subject="train-start",
            description="在 gate 通过后启动滚动训练",
            agent_name="train-runner",
            run_script="scripts/clawteam_train_start.sh",
            blocked_by=("gate",),
        ),
        ClawTaskTemplate(
            key="reports",
            subject="export-reports",
            description="生成推荐 CSV / Markdown / HTML 日报并刷新 latest 文件",
            agent_name="report-exporter",
            run_script="scripts/clawteam_export_reports.sh",
            blocked_by=("train",),
        ),
    ])
    return templates


def build_leader_prompt(team_name: str, repo_path: str) -> str:
    return (
        f"你是 {team_name} 的 leader，仓库路径是 {repo_path}。\n"
        "你的职责是监控任务看板、检查 worker 是否正确更新 task 状态，并在某个步骤失败时给出重试建议。\n"
        "优先使用 `clawteam task list`、`clawteam board show`、`clawteam inbox` 来观察团队状态。\n"
        "不要直接修改量化业务代码，除非用户明确要求。"
    )


def build_worker_prompt(
    *,
    team_name: str,
    task_id: str,
    template: ClawTaskTemplate,
    dependency_task_ids: list[str],
    clawteam_bin: str,
    data_dir: str,
    repo_path: str,
) -> str:
    dep_lines = ""
    if dependency_task_ids:
        dep_lines = (
            "在开始前，先确认这些依赖任务都已经完成："
            + ", ".join(dependency_task_ids)
            + "。\n"
            f"可以用 `{clawteam_bin} --data-dir {data_dir} task list {team_name}` 或 "
            f"`{clawteam_bin} --data-dir {data_dir} board show {team_name}` 检查。\n"
        )
    return (
        f"你是 {template.agent_name}，负责任务 {task_id}（{template.subject}）。\n"
        f"代码仓库在 {repo_path}。\n"
        f"{dep_lines}"
        "严格按下面步骤执行：\n"
        f"1. 先运行 `{clawteam_bin} --data-dir {data_dir} task update {team_name} {task_id} --status in_progress --owner {template.agent_name}`。\n"
        f"2. 进入仓库后运行 `bash {template.run_script}`。\n"
        f"3. 如果脚本成功，运行 `{clawteam_bin} --data-dir {data_dir} task update {team_name} {task_id} --status completed --owner {template.agent_name}`。\n"
        f"4. 如果脚本失败，运行 `{clawteam_bin} --data-dir {data_dir} task update {team_name} {task_id} --status blocked --owner {template.agent_name}`，并把失败原因写进 description。\n"
        "不要修改别的 task 状态。不要做与当前步骤无关的代码编辑。"
    )


def default_data_dir(repo_path: str | Path) -> Path:
    return Path(repo_path).expanduser().resolve() / ".clawteam-workbench"
