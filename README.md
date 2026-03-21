# qlib-research-workbench

[![CI](https://github.com/ioiochen11/qlib-research-workbench/actions/workflows/ci.yml/badge.svg)](https://github.com/ioiochen11/qlib-research-workbench/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Status](https://img.shields.io/badge/status-active-success)

一个面向 A 股研究流程的 Qlib 工作台，用来做收盘后数据校验、滚动训练、推荐日报生成，以及复盘和回测。

仓库首页现在以中文说明为主，更完整的中文文档见 [docs/README_CN.md](docs/README_CN.md)。

当前默认工作流是：

- 目标股票池默认是 `沪深300`
- 训练和推荐都只保留 `30 元以下` 的股票
- 收盘后同步 `market / fundamentals / events`
- freshness gate 不通过时，不覆盖已有 `latest_*` 日报
- 生成全中文的 `CSV / Markdown / HTML` 推荐日报，方便人工核对

这个项目最初是对 [`touhoufan2024/qlibAssistant`](https://github.com/touhoufan2024/qlibAssistant) 的一次聚焦重构，后来逐步扩展成一个更适合公开维护的研究型仓库，补上了 CLI、测试、文档、CI，以及收盘后多 feed 校验流程。

如果这个项目对你有帮助，欢迎点个 Star。

## 一眼看懂

- 收盘后多 feed 校验流水线，包含 `raw / gold / manifests`
- 本地 Qlib 数据工作流，不再依赖别人每天更新 release 数据包
- 基于 `Alpha158` 的滚动训练与推荐生成
- 面向人工核对的中文推荐日报，而不只是 notebook 输出
- 内置 freshness gate，脏数据或陈旧数据不会污染 `latest_*`
- 可选的 `ClawTeam` 收盘后 runner，带任务看板、步骤日志和 summary 快照

## 为什么做这个仓库

`qlibAssistant` 最有价值的点，是把 Qlib 从“一次性的 notebook”变成“每天可运行的研究流水线”。这个仓库保留了这个方向，但把它整理成一个更容易验证、扩展和公开维护的工作台。

它现在已经具备：

- 远端数据探测、下载、解压和本地校验
- 基于 AkShare 的日频同步，不用再等第三方包更新
- `market / fundamentals / events` 的收盘后多 feed 同步与校验
- freshness gate，保证脏数据不会进入正式日报
- 可选的上证 180 股票池刷新和本地缓存回退
- 本地 Qlib 初始化与特征读取 smoke check
- 带样本级 `<= 30 元` 过滤的滚动训练
- 预测聚合、每日选股报表、推荐价位计划
- 全中文的 CSV / Markdown / HTML 推荐日报
- `daily-run` 和 `clawteam-runner` 两套收盘后执行方式
- 复盘、TopK 回测、`mlruns` 备份恢复
- 单元测试、文档、Makefile 快捷命令和 GitHub Actions CI

## 工作流

```text
Post-close sync
  -> validate market / fundamentals / events
  -> write manifests and freshness gate result
  -> run rolling training
  -> aggregate predictions
  -> export Chinese CSV / Markdown / HTML reports
  -> keep dated archives and stable latest_* files
```

This makes the repo useful for two different jobs at once:

- daily candidate generation
- daily validation of whether the recommendation plan matched the next trade day's actual price behavior

## Quick Start

Install the lightweight dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Install Qlib-related dependencies when you want to run training or model workflows:

```bash
python3 -m pip install -r requirements-qlib.txt
```

Or install the package directly:

```bash
python3 -m pip install -e .
python3 -m pip install -e .[qlib]
```

## First Run

Probe the remote data source:

```bash
python3 -m qlib_assistant_refactor probe
```

Check local data status:

```bash
python3 roll.py data status
```

If you want to work on `上证180`, refresh that universe first; otherwise the default `沪深300` workflow can sync directly:

```bash
.venv/bin/python roll.py data refresh-sse180
.venv/bin/python roll.py data sync-akshare --start-date 2026-03-19 --end-date 2026-03-20
.venv/bin/python roll.py data sync-market --start-date 2026-03-19 --end-date 2026-03-20
.venv/bin/python roll.py data sync-fundamentals --date 2026-03-20
.venv/bin/python roll.py data sync-events --date 2026-03-20
.venv/bin/python roll.py data verify-freshness --date 2026-03-20
```

Validate that Qlib can read the extracted dataset:

```bash
.venv/bin/python roll.py data qlib-check
```

Run a minimal training job:

```bash
.venv/bin/python roll.py train smoke
```

Generate reports:

```bash
.venv/bin/python roll.py model report
.venv/bin/python roll.py model review
.venv/bin/python roll.py model backtest
```

Run the full post-close pipeline:

```bash
.venv/bin/python roll.py daily-run
.venv/bin/python roll.py clawteam-runner
```

## Common Commands

Data:

```bash
python3 -m qlib_assistant_refactor probe
python3 -m qlib_assistant_refactor status
python3 -m qlib_assistant_refactor verify
python3 -m qlib_assistant_refactor qlib-check
python3 roll.py data update --proxy A
python3 roll.py data refresh-sse180
python3 roll.py data sync-akshare
python3 roll.py data sync-market
python3 roll.py data sync-fundamentals
python3 roll.py data sync-events
python3 roll.py data verify-freshness
python3 roll.py data show-manifest --date 2026-03-20
```

Training:

```bash
.venv/bin/python roll.py train plan
.venv/bin/python roll.py train smoke
.venv/bin/python roll.py train start --limit 1
.venv/bin/python roll.py train list-experiments
```

Analysis:

```bash
.venv/bin/python roll.py model ls --all
.venv/bin/python roll.py model top --limit 10
.venv/bin/python roll.py model recommendations --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-recommendations --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model recommendation-report --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-recommendation-report --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model recommendation-html --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-recommendation-html --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model recommendation-spotlight --date 2026-03-20 --limit 3 --max-price 30
.venv/bin/python roll.py model save-recommendation-spotlight-html --date 2026-03-20 --limit 3 --max-price 30
.venv/bin/python roll.py model report
.venv/bin/python roll.py model review
.venv/bin/python roll.py model backtest
.venv/bin/python roll.py daily-run
.venv/bin/python roll.py clawteam-runner
```

For validation work, `model recommendations` is the best default view. It shows:

- the recommended stocks and names
- raw-price entry, breakout, stop, and take-profit levels
- the next trade day's OHLC prices
- whether the buy zone or breakout was actually touched
- a compact validation status for quick manual cross-checking

If you want a more readable daily brief than a wide console table, use `model recommendation-report` or `save-recommendation-report` to render the same recommendation sheet as Markdown.

If you want something visual you can open directly in a browser, use `model recommendation-html` or `save-recommendation-html` to render the daily brief as a standalone HTML page.

If you want a more analyst-style summary focused only on the most important candidates, use `model recommendation-spotlight` or `save-recommendation-spotlight-html` to export a top-3 interpretation page.

`daily-run` ties together:

- `data sync-market`
- `data sync-fundamentals`
- `data sync-events`
- `data verify-freshness`
- `train start`
- `model report`
- `model save-recommendations`
- `model save-recommendation-report`
- `model save-recommendation-html`

It writes both dated files and stable `latest_*` files under `~/.qlibAssistant/analysis`, so a local automation can keep replacing the latest daily brief without removing older dated archives. When the configured stock pool is `上证180`, it also refreshes the local `sse180.txt` universe file first.

If the configured freshness gate fails, `daily-run` now skips training and formal report generation. In that case it keeps the previous `latest_*` artifacts untouched and writes the reason into the manifest folder under `~/.qlibAssistant/daily_sync/manifests/YYYY-MM-DD/`.

If you want the same post-close flow with task-level tracking, run:

```bash
.venv/bin/python roll.py clawteam-runner
```

That creates a ClawTeam team for the run, updates task states step by step, writes per-task logs under `.clawteam-workbench/runs/<team>/logs/`, and saves a summary JSON for later inspection.

## Outputs You Can Open Directly

After a successful run, the most useful files are:

- `~/.qlibAssistant/analysis/latest_recommendations.csv`
- `~/.qlibAssistant/analysis/latest_recommendation_report.html`
- `~/.qlibAssistant/analysis/latest_recommendation_spotlight.html`

These are designed to answer three fast questions:

- what was recommended
- what price zone the system wanted
- whether the next trade day actually touched that plan

Backups:

```bash
.venv/bin/python roll.py model list-backups
.venv/bin/python roll.py model backup
.venv/bin/python roll.py model restore
```

Make shortcuts:

```bash
make test
make doctor
make probe
make refresh-sse180
make sync-akshare
make train-smoke
make model-recommendations
make model-recommendation-report
make model-recommendation-html
make model-report
make model-review
make model-backtest
make daily-run
make clawteam-runner
make clean-local
```

## Project Layout

- [`qlib_assistant_refactor/`](qlib_assistant_refactor): main application code
- [`tests/`](tests): unit tests
- [`docs/COMMANDS.md`](docs/COMMANDS.md): CLI reference
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): module-level architecture notes
- [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md): local development workflow
- [`docs/ROADMAP.md`](docs/ROADMAP.md): planned next steps
- [`CONTRIBUTING.md`](CONTRIBUTING.md): contribution guidance

## Runtime Paths

- Local Qlib data: `~/.qlib/qlib_data/cn_data`
- Raw structured sync cache: `~/.qlibAssistant/daily_sync/raw`
- Validated gold feeds: `~/.qlibAssistant/daily_sync/gold`
- Feed manifests: `~/.qlibAssistant/daily_sync/manifests`
- MLflow experiments: `~/.qlibAssistant/mlruns`
- Analysis outputs: `~/.qlibAssistant/analysis`
- Backup archives: `~/model_pkl`

## What Has Been Verified

In the current workspace, these flows have already been run successfully:

- Full Qlib CN data download and extraction
- Local `沪深300` feature access through Qlib
- Minimal `Linear + Alpha158` training run
- Prediction export to `top_predictions_*.csv`
- Selection report generation under `selection_*/`
- Chinese recommendation CSV / Markdown / HTML export
- Review and backtest summary generation
- `mlruns_YYYY-MM-DD.tar.gz` archive creation

## Current Scope

This repo is intentionally practical rather than exhaustive.

- It prioritizes reproducible workflows over framework abstraction
- It keeps the original project spirit but does not mirror every original feature one-to-one
- It is suitable as a personal research base or a starting point for a team-internal tool
- It is not yet positioned as a production execution engine

## Roadmap

Planned improvements are tracked in [`docs/ROADMAP.md`](docs/ROADMAP.md).

Short version:

- Better experiment selection and ranking logic
- Cleaner packaging and command ergonomics
- Optional richer reporting outputs
- More end-to-end smoke coverage

## Notes

- Network reachability can change over time; direct GitHub asset URLs and proxy mirrors are not equally stable.
- `pyqlib` is relatively heavy, so using a virtual environment is strongly recommended.
- Some environments show a `urllib3` and `LibreSSL` warning; it does not necessarily block the workflow.

## Docs

- Command reference: [`docs/COMMANDS.md`](docs/COMMANDS.md)
- Architecture notes: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- Development guide: [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md)
- Contribution guide: [`CONTRIBUTING.md`](CONTRIBUTING.md)
