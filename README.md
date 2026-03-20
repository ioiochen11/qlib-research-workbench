# qlib-research-workbench

[![CI](https://github.com/ioiochen11/qlib-research-workbench/actions/workflows/ci.yml/badge.svg)](https://github.com/ioiochen11/qlib-research-workbench/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Status](https://img.shields.io/badge/status-active-success)

A practical Qlib-based research workbench for validating Chinese market data, training lightweight rolling models, exporting daily selection reports, and running review and backtest workflows.

中文说明见 [docs/README_CN.md](docs/README_CN.md)。

The current default workflow is opinionated on purpose: it targets `沪深300` constituents, keeps only `30 元以下` names for both training and recommendation, and generates a fully Chinese daily recommendation brief after the close.

This project started as a focused refactor of [`touhoufan2024/qlibAssistant`](https://github.com/touhoufan2024/qlibAssistant), then grew into a cleaner public-facing repo with a testable CLI, documentation, and CI.

If this project is useful, a GitHub star helps a lot.

## Why This Repo

`qlibAssistant` has a strong idea: turn Qlib into a daily research pipeline instead of a one-off notebook. This repo keeps that idea, but reshapes it into a workbench that is easier to validate, extend, and publish.

What it gives you today:

- Remote data probing, download, extraction, and verification
- AkShare-based daily sync so you can refresh local CN daily bars without waiting for a remote package update
- Multi-feed post-close sync with raw / gold / manifest layers for market, fundamentals, and events
- Freshness gating so invalid or stale feeds skip the formal daily report instead of poisoning the latest output
- Optional SSE180 universe refresh from AkShare with a local cached fallback
- Local Qlib initialization and feature-read smoke checks
- Rolling-task training with sample-level `<= 30 元` price filtering
- Prediction aggregation and daily selection report export
- Rule-based entry-plan generation for selected candidates
- Validation-friendly recommendation tables with names, raw-price entry levels, and next-trade-day hit checks
- Fully Chinese CSV / Markdown / HTML daily recommendation reports
- A one-shot `daily-run` pipeline for post-close automation
- Review summaries and top-k backtest reports
- `mlruns` backup and restore utilities
- Unit tests, docs, Make targets, and GitHub Actions CI

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
