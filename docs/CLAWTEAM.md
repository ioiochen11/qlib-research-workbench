# ClawTeam 集成说明

这个仓库可以把 `ClawTeam` 当作编排层来用，而把 `qlib-research-workbench` 继续当作业务执行层。

职责划分是：

- `ClawTeam`：team 创建、task 跟踪、worker 拉起、board 展示
- `qlib-research-workbench`：行情同步、freshness gate、训练、日报导出

## 日常 Runner

如果你是为了每天收盘后自动运行，优先用内置 runner：

```bash
.venv/bin/python roll.py clawteam-runner
```

这种模式里，`ClawTeam` 负责：

- 任务看板
- 步骤状态跟踪
- 每个任务的日志文件
- 每次运行的 summary 快照

而这个仓库继续负责通过现有 shell 脚本去执行真实的 `market / fundamentals / events / gate / train / report` 步骤。

输出路径：

- 任务日志：`.clawteam-workbench/runs/<team>/logs/`
- summary JSON：`.clawteam-workbench/runs/<team>/summary.json`

smoke test 示例：

```bash
.venv/bin/python roll.py clawteam-runner --team-name qlib-post-close-smoke --market-limit 5 --fundamentals-limit 5 --events-limit 5
```

正式日常运行时，不要带 `--*-limit`。

## 环境准备

给 `ClawTeam` 单独准备一个 Python 3.11+ 环境：

```bash
bash scripts/setup_clawteam_env.sh
```

这会创建：

- `.venv-clawteam/`

现有工作台运行时仍然放在：

- `.venv/`

## 创建任务板

如果你只想先创建一个收盘后任务板，不拉起 agent：

```bash
./.venv-clawteam/bin/python scripts/clawteam_post_close.py \
  --team-name qlib-post-close-smoke \
  --data-dir .tmp-clawteam
```

它会创建一个 team，并生成这些任务：

1. `sync-market`
2. `sync-fundamentals`
3. `sync-events`
4. `verify-freshness`
5. `train-start`
6. `export-reports`

## 拉起 Worker

如果你机器上已经有 agent CLI，可以让 ClawTeam 去拉起 worker。

`subprocess` 后端示例：

```bash
./.venv-clawteam/bin/python scripts/clawteam_post_close.py \
  --team-name qlib-post-close-live \
  --data-dir .clawteam-workbench \
  --backend subprocess \
  --command "claude" \
  --spawn-workers
```

`tmux` 后端示例：

```bash
./.venv-clawteam/bin/python scripts/clawteam_post_close.py \
  --team-name qlib-post-close-live \
  --data-dir .clawteam-workbench \
  --backend tmux \
  --command "claude" \
  --spawn-workers \
  --workspace
```

## 看板命令

查看任务看板：

```bash
.venv-clawteam/bin/clawteam --data-dir .clawteam-workbench board show qlib-post-close-live
```

等待所有任务结束：

```bash
.venv-clawteam/bin/clawteam --data-dir .clawteam-workbench task wait qlib-post-close-live
```

## Worker 步骤脚本

每个 worker prompt 都会对应一个确定性的 shell 脚本：

- `scripts/clawteam_market_sync.sh`
- `scripts/clawteam_fundamentals_sync.sh`
- `scripts/clawteam_events_sync.sh`
- `scripts/clawteam_verify_freshness.sh`
- `scripts/clawteam_train_start.sh`
- `scripts/clawteam_export_reports.sh`

这样可以让 ClawTeam 负责协调，而这个仓库继续负责业务逻辑本身。
