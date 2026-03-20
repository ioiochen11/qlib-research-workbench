# Command Reference

## Data CLI

Use the lightweight package entry when you only need data validation and local dataset checks.

```bash
python3 -m qlib_assistant_refactor probe
python3 -m qlib_assistant_refactor status
python3 -m qlib_assistant_refactor verify
python3 -m qlib_assistant_refactor qlib-check
python3 -m qlib_assistant_refactor download --output ~/tmp/qlib_bin.tar.gz
python3 -m qlib_assistant_refactor extract --archive ~/tmp/qlib_bin.tar.gz --target-dir ~/.qlib/qlib_data/cn_data --strip-components 1
```

## Roll-Compatible CLI

Use the qlibAssistant-style entry when you want the full workflow.

```bash
python3 roll.py show-config
python3 roll.py data status
python3 roll.py data update --proxy A
python3 roll.py data qlib-check
```

## Training

```bash
.venv/bin/python roll.py train plan
.venv/bin/python roll.py train smoke
.venv/bin/python roll.py train start --limit 1
.venv/bin/python roll.py train list-experiments
```

## Model Analysis

```bash
.venv/bin/python roll.py model ls --all
.venv/bin/python roll.py model top --limit 10
.venv/bin/python roll.py model save-top --limit 20
.venv/bin/python roll.py model report
.venv/bin/python roll.py model review
.venv/bin/python roll.py model backtest
```

## Backup

```bash
.venv/bin/python roll.py model list-backups
.venv/bin/python roll.py model backup
.venv/bin/python roll.py model restore
.venv/bin/python roll.py model restore --archive-name mlruns_2026-03-19.tar.gz
```

## Makefile Shortcuts

```bash
make test
make doctor
make probe
make train-smoke
make model-report
make model-review
make model-backtest
make model-backup
make clean-local
```
