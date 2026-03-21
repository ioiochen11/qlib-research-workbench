# ClawTeam Integration

This repository can use `ClawTeam` as an orchestration layer while keeping `qlib-research-workbench` as the execution layer.

The intended split is:

- `ClawTeam`: team creation, task tracking, worker spawning, board view
- `qlib-research-workbench`: market sync, freshness gate, training, report export

## Daily Runner

For daily post-close automation, prefer the built-in runner:

```bash
.venv/bin/python roll.py clawteam-runner
```

This mode keeps `ClawTeam` responsible for:

- task board creation
- step status tracking
- per-task log files
- run summary snapshots

And it keeps this repository responsible for actually running the market / fundamentals / events / gate / train / report steps through the existing shell scripts.

Output paths:

- task logs: `.clawteam-workbench/runs/<team>/logs/`
- summary JSON: `.clawteam-workbench/runs/<team>/summary.json`

Smoke-test example:

```bash
.venv/bin/python roll.py clawteam-runner --team-name qlib-post-close-smoke --market-limit 5 --fundamentals-limit 5 --events-limit 5
```

In normal daily runs, omit the `--*-limit` arguments.

## Setup

Create a dedicated Python 3.11+ environment for ClawTeam:

```bash
bash scripts/setup_clawteam_env.sh
```

This creates:

- `.venv-clawteam/`

Your existing workbench runtime stays in:

- `.venv/`

## Bootstrap A Team

Create a post-close task board without spawning agents:

```bash
./.venv-clawteam/bin/python scripts/clawteam_post_close.py \
  --team-name qlib-post-close-smoke \
  --data-dir .tmp-clawteam
```

This creates a team with tasks for:

1. `sync-market`
2. `sync-fundamentals`
3. `sync-events`
4. `verify-freshness`
5. `train-start`
6. `export-reports`

## Spawn Workers

If you already have an agent CLI on PATH, you can ask ClawTeam to spawn workers.

Example with subprocess backend:

```bash
./.venv-clawteam/bin/python scripts/clawteam_post_close.py \
  --team-name qlib-post-close-live \
  --data-dir .clawteam-workbench \
  --backend subprocess \
  --command "claude" \
  --spawn-workers
```

Example with tmux backend:

```bash
./.venv-clawteam/bin/python scripts/clawteam_post_close.py \
  --team-name qlib-post-close-live \
  --data-dir .clawteam-workbench \
  --backend tmux \
  --command "claude" \
  --spawn-workers \
  --workspace
```

## Board Commands

Show a team board:

```bash
.venv-clawteam/bin/clawteam --data-dir .clawteam-workbench board show qlib-post-close-live
```

Wait for all tasks:

```bash
.venv-clawteam/bin/clawteam --data-dir .clawteam-workbench task wait qlib-post-close-live
```

## Worker Step Scripts

Each worker prompt uses one deterministic shell script:

- `scripts/clawteam_market_sync.sh`
- `scripts/clawteam_fundamentals_sync.sh`
- `scripts/clawteam_events_sync.sh`
- `scripts/clawteam_verify_freshness.sh`
- `scripts/clawteam_train_start.sh`
- `scripts/clawteam_export_reports.sh`

That keeps ClawTeam responsible for coordination while this repository keeps responsibility for business logic.
