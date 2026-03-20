# qlib-research-workbench

[![CI](https://github.com/ioiochen11/qlib-research-workbench/actions/workflows/ci.yml/badge.svg)](https://github.com/ioiochen11/qlib-research-workbench/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Status](https://img.shields.io/badge/status-active-success)

A practical Qlib-based research workbench for validating Chinese market data, training lightweight rolling models, exporting daily selection reports, and running review and backtest workflows.

This project started as a focused refactor of [`touhoufan2024/qlibAssistant`](https://github.com/touhoufan2024/qlibAssistant), then grew into a cleaner public-facing repo with a testable CLI, documentation, and CI.

If this project is useful, a GitHub star helps a lot.

## Why This Repo

`qlibAssistant` has a strong idea: turn Qlib into a daily research pipeline instead of a one-off notebook. This repo keeps that idea, but reshapes it into a workbench that is easier to validate, extend, and publish.

What it gives you today:

- Remote data probing, download, extraction, and verification
- Local Qlib initialization and feature-read smoke checks
- Minimal rolling-task training with experiment persistence
- Prediction aggregation and daily selection report export
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

## Common Commands

Data:

```bash
python3 -m qlib_assistant_refactor probe
python3 -m qlib_assistant_refactor status
python3 -m qlib_assistant_refactor verify
python3 -m qlib_assistant_refactor qlib-check
python3 roll.py data update --proxy A
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
.venv/bin/python roll.py model report
.venv/bin/python roll.py model review
.venv/bin/python roll.py model backtest
```

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
make train-smoke
make model-report
make model-review
make model-backtest
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
- MLflow experiments: `~/.qlibAssistant/mlruns`
- Analysis outputs: `~/.qlibAssistant/analysis`
- Backup archives: `~/model_pkl`

## What Has Been Verified

In the current workspace, these flows have already been run successfully:

- Full Qlib CN data download and extraction
- Local `CSI300` feature access through Qlib
- Minimal `Linear + Alpha158` training run
- Prediction export to `top_predictions_*.csv`
- Selection report generation under `selection_*/`
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
