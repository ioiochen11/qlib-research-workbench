from unittest import TestCase

from qlib_assistant_refactor.clawteam_adapter import (
    build_post_close_task_templates,
    build_worker_prompt,
)


class ClawTeamAdapterTests(TestCase):
    def test_build_post_close_task_templates_has_expected_order(self) -> None:
        templates = build_post_close_task_templates()
        self.assertEqual(
            [item.key for item in templates],
            ["market", "fundamentals", "events", "gate", "train", "reports"],
        )
        self.assertEqual(templates[1].blocked_by, ("market",))
        self.assertEqual(templates[-1].blocked_by, ("train",))

    def test_build_post_close_task_templates_can_include_refresh_task(self) -> None:
        templates = build_post_close_task_templates(include_refresh_sse180=True)
        self.assertEqual(templates[0].key, "refresh")
        self.assertEqual(templates[1].blocked_by, ("refresh",))

    def test_build_worker_prompt_mentions_task_update_and_script(self) -> None:
        template = build_post_close_task_templates()[0]
        prompt = build_worker_prompt(
            team_name="demo-team",
            task_id="abcd1234",
            template=template,
            dependency_task_ids=["root0001"],
            clawteam_bin="/tmp/clawteam",
            data_dir="/tmp/team-data",
            repo_path="/tmp/repo",
        )
        self.assertIn("abcd1234", prompt)
        self.assertIn("root0001", prompt)
        self.assertIn("scripts/clawteam_market_sync.sh", prompt)
        self.assertIn("task update demo-team abcd1234 --status in_progress", prompt)
