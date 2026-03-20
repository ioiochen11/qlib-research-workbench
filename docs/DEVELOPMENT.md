# Development

## Suggested Environment

Use a local virtual environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install -r requirements-qlib.txt
```

## Common Developer Commands

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

## Testing

The project currently uses `unittest`:

```bash
python3 -m unittest discover -s tests -v
```

## CLI Entry Points

Small data-focused CLI:

```bash
python3 -m qlib_assistant_refactor probe
```

Roll-compatible CLI:

```bash
python3 roll.py data status
python3 roll.py train smoke
python3 roll.py model report
```

Installed script entry points:

```bash
qlib-research-workbench probe
qlib-roll data status
```

Legacy compatibility wrappers remain available:

```bash
python3 scripts/smoke_test.py
.venv/bin/python scripts/qlib_smoke.py
```

## Notes

- `roll.py` is intentionally thin and delegates to `qlib_assistant_refactor.roll_cli`.
- Qlib init and MLflow runtime setup are centralized in `qlib_assistant_refactor.qlib_env`.
- `scripts/` now only contains thin wrappers around the package CLI to avoid duplicated logic.
- `make doctor` is the fastest way to re-check remote reachability, local data dates, and extracted dataset structure.
- Analysis outputs under `~/.qlibAssistant/analysis` can grow quickly; clean old runs when needed.
