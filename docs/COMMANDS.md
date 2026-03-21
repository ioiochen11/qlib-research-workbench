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
python3 roll.py data refresh-sse180
python3 roll.py data sync-akshare --start-date 2026-03-19 --end-date 2026-03-20
python3 roll.py data sync-market --start-date 2026-03-19 --end-date 2026-03-20
python3 roll.py data sync-fundamentals --date 2026-03-20
python3 roll.py data sync-events --date 2026-03-20
python3 roll.py data verify-freshness --date 2026-03-20
python3 roll.py data show-manifest --date 2026-03-20
python3 roll.py data qlib-check
python3 roll.py daily-run
python3 roll.py clawteam-runner
```

`sync-market` writes raw source snapshots under `raw/market/`, validates AkShare against Eastmoney, and only then updates the local Qlib provider and `gold/market/`.

`sync-fundamentals` and `sync-events` write structured feed snapshots under `gold/fundamentals/` and `gold/events/`.

`verify-freshness` checks the configured post-close gate. If a required feed is stale, invalid, or missing, `daily-run` will skip formal training and report generation.

`clawteam-runner` is the stable task-tracked variant of the post-close flow. It creates a ClawTeam board for the run, executes each business step locally, stores per-task logs, and writes a run summary JSON under `.clawteam-workbench/runs/<team>/`.

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
.venv/bin/python roll.py model entry-plan --limit 10 --max-price 30
.venv/bin/python roll.py model save-entry-plan --limit 10 --max-price 30
.venv/bin/python roll.py model recommendations --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-recommendations --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model recommendation-report --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-recommendation-report --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model recommendation-html --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-recommendation-html --date 2026-03-19 --limit 10 --max-price 30
.venv/bin/python roll.py model save-top --limit 20
.venv/bin/python roll.py model report
.venv/bin/python roll.py model review
.venv/bin/python roll.py model backtest
```

`model recommendations` is the most validation-friendly view. It combines:

- recommended instruments and names
- planned entry zone, breakout, stop, and take-profit levels
- next-trade-day raw OHLC prices
- whether the plan's buy zone or breakout was actually touched
- a simple validation status for quick manual checking

`model recommendation-report` renders the same data as a compact Markdown daily brief, which is easier to read or archive than the wide console table.

`model recommendation-html` renders the same recommendation brief as a self-contained HTML page with summary cards and a styled table, so you can open it directly in a browser.

`save-recommendations` now exports a fully Chinese CSV. `save-recommendation-report` and `save-recommendation-html` also use Chinese labels, validation states, and notes.

`daily-run` is the post-close one-shot pipeline for the default `沪深300 + 30 元以下` workflow. It syncs validated market / fundamentals / events feeds, runs the freshness gate, trains the model, generates the selection report, and writes both dated and `latest_*` recommendation artifacts. If you switch the configured stock pool to `上证180`, it will refresh the local `sse180.txt` universe file first.

When the freshness gate fails, `daily-run` exits without touching the previous `latest_*` files. The detailed reason is stored in the daily manifest directory so you can inspect why the formal report was skipped.

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
make refresh-sse180
make sync-akshare
make train-smoke
make model-recommendations
make model-recommendation-report
make model-recommendation-html
make model-report
make model-review
make model-backtest
make model-backup
make daily-run
make clawteam-runner
make clean-local
```
