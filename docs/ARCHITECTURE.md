# Architecture

## Goal

This refactor keeps the original `qlibAssistant` intent, but splits the workflow into smaller, testable layers:

1. data reachability and local dataset validation
2. qlib initialization and task generation
3. training and experiment persistence
4. prediction aggregation, reporting, review, and backtest
5. backup and restore of `mlruns`

## Main Modules

### `qlib_assistant_refactor/config.py`

Holds the application config model and YAML loading.

### `qlib_assistant_refactor/qlib_env.py`

Centralizes:

- `provider_uri` path handling
- latest local trade date lookup
- MLflow experiment manager config
- `qlib.init(...)`

### `qlib_assistant_refactor/data_service.py`

Handles remote probing, archive download, extraction, and local dataset checks.

### `qlib_assistant_refactor/data_cli.py`

Implements the original-style data subcommands on top of `DataService`.

### `qlib_assistant_refactor/task_factory.py`

Builds minimal Qlib task templates and rolling segment windows.

### `qlib_assistant_refactor/train_cli.py`

Owns:

- rolling task generation
- train plan preview
- smoke training
- experiment listing

### `qlib_assistant_refactor/model_cli.py`

Owns:

- recorder discovery and filtering
- saved prediction aggregation
- per-day report generation
- review and backtest outputs
- `mlruns` backup and restore

### `qlib_assistant_refactor/roll_cli.py`

Provides the qlibAssistant-style top-level CLI:

- `data`
- `train`
- `model`

### `qlib_assistant_refactor/cli.py`

Provides the smaller data-focused package CLI for quick validation tasks.

## Entry Points

- `python3 -m qlib_assistant_refactor ...`
- `python3 roll.py ...`
- `qlib-roll ...` after editable install

## Outputs

- local qlib data: `~/.qlib/qlib_data/cn_data`
- mlflow experiments: `~/.qlibAssistant/mlruns`
- analysis outputs: `~/.qlibAssistant/analysis`
- archives: `~/model_pkl`
