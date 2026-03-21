from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from qlib_assistant_refactor.clawteam_runner import ClawTeamDailyRunner, CommandResult
from qlib_assistant_refactor.config import AppConfig


class FakeClient:
    def __init__(self) -> None:
        self.created_tasks: list[tuple[str, str]] = []
        self.updates: list[dict[str, str]] = []
        self._counter = 0

    def spawn_team(self, team_name: str, leader_name: str, description: str) -> dict[str, object]:
        return {"teamName": team_name, "leaderName": leader_name}

    def create_task(self, *, team_name: str, template, blocked_by_ids: list[str]) -> dict[str, object]:
        self._counter += 1
        task_id = f"task-{self._counter}"
        self.created_tasks.append((template.key, task_id))
        return {"id": task_id}

    def update_task(self, *, team_name: str, task_id: str, status: str, owner: str, description: str | None = None) -> None:
        self.updates.append(
            {
                "team_name": team_name,
                "task_id": task_id,
                "status": status,
                "owner": owner,
                "description": description or "",
            }
        )


class ClawTeamDailyRunnerTests(TestCase):
    def test_runner_blocks_downstream_when_gate_not_eligible(self) -> None:
        outputs = {
            "clawteam_market_sync.sh": CommandResult(0, "as_of_date=2026-03-20\nwritten_csv=5\n", "", 1.0),
            "clawteam_fundamentals_sync.sh": CommandResult(0, "record_count=5\ncoverage_ratio=1.0\n", "", 1.0),
            "clawteam_events_sync.sh": CommandResult(0, "record_count=5\ncoverage_ratio=1.0\n", "", 1.0),
            "clawteam_verify_freshness.sh": CommandResult(
                0,
                "eligible_for_daily_run=False\nvalidation_status=failed\nvalidation_errors=stale_feed:market\n",
                "",
                1.0,
            ),
        }

        def fake_runner(*, repo_path, script_path, log_path, timeout_seconds, args):
            log_path.write_text("ok", encoding="utf-8")
            return outputs[script_path.name]

        with TemporaryDirectory() as tmpdir:
            client = FakeClient()
            runner = ClawTeamDailyRunner(
                config=AppConfig(),
                repo_path=tmpdir,
                clawteam_bin=Path(tmpdir) / "clawteam",
                data_dir=Path(tmpdir) / ".clawteam",
                team_name="daily-demo",
                client=client,
                command_runner=fake_runner,
                now_fn=lambda: datetime(2026, 3, 21, 16, 15, 0),
            )

            summary = runner.run()

            self.assertTrue(summary["skipped"])
            self.assertEqual(summary["tasks"]["gate"]["status"], "completed")
            blocked_descriptions = [item["description"] for item in client.updates if item["status"] == "blocked"]
            self.assertTrue(any("stale_feed:market" in item for item in blocked_descriptions))
            self.assertTrue(Path(summary["summary_path"]).exists())

    def test_runner_completes_all_steps_on_success(self) -> None:
        outputs = {
            "clawteam_market_sync.sh": CommandResult(0, "as_of_date=2026-03-20\nwritten_csv=5\n", "", 1.0),
            "clawteam_fundamentals_sync.sh": CommandResult(0, "record_count=5\ncoverage_ratio=1.0\n", "", 1.0),
            "clawteam_events_sync.sh": CommandResult(0, "record_count=5\ncoverage_ratio=1.0\n", "", 1.0),
            "clawteam_verify_freshness.sh": CommandResult(0, "eligible_for_daily_run=True\nvalidation_status=passed\n", "", 1.0),
            "clawteam_train_start.sh": CommandResult(0, "task_count=1\nelapsed_seconds=3.5\n", "", 3.5),
            "clawteam_export_reports.sh": CommandResult(0, "selection_dir=/tmp/demo\nlatest_html=/tmp/latest.html\n", "", 1.0),
        }

        def fake_runner(*, repo_path, script_path, log_path, timeout_seconds, args):
            log_path.write_text("ok", encoding="utf-8")
            return outputs[script_path.name]

        with TemporaryDirectory() as tmpdir:
            client = FakeClient()
            runner = ClawTeamDailyRunner(
                config=AppConfig(),
                repo_path=tmpdir,
                clawteam_bin=Path(tmpdir) / "clawteam",
                data_dir=Path(tmpdir) / ".clawteam",
                team_name="daily-success",
                client=client,
                command_runner=fake_runner,
                now_fn=lambda: datetime(2026, 3, 21, 16, 15, 0),
            )

            summary = runner.run()

            self.assertFalse(summary["skipped"])
            self.assertEqual(summary["tasks"]["train"]["status"], "completed")
            self.assertEqual(summary["tasks"]["reports"]["status"], "completed")
