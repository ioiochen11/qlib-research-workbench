PYTHON ?= python3
VENV_PYTHON ?= .venv/bin/python

.PHONY: test probe status verify qlib-check doctor refresh-sse180 sync-akshare train-plan train-smoke model-top model-recommendations model-recommendation-report model-recommendation-html model-report model-review model-backtest model-backup daily-run clawteam-runner clean-local

test:
	$(PYTHON) -m unittest discover -s tests -v

probe:
	$(PYTHON) -m qlib_assistant_refactor probe

status:
	$(PYTHON) roll.py data status

verify:
	$(PYTHON) -m qlib_assistant_refactor verify

qlib-check:
	$(VENV_PYTHON) roll.py data qlib-check

doctor:
	$(PYTHON) -m qlib_assistant_refactor probe
	$(PYTHON) roll.py data status
	$(PYTHON) -m qlib_assistant_refactor verify

refresh-sse180:
	$(VENV_PYTHON) roll.py data refresh-sse180

sync-akshare:
	$(VENV_PYTHON) roll.py data sync-akshare

train-plan:
	$(VENV_PYTHON) roll.py train plan

train-smoke:
	$(VENV_PYTHON) roll.py train smoke

model-top:
	$(VENV_PYTHON) roll.py model top --limit 10

model-recommendations:
	$(VENV_PYTHON) roll.py model recommendations --limit 10

model-recommendation-report:
	$(VENV_PYTHON) roll.py model save-recommendation-report --limit 10

model-recommendation-html:
	$(VENV_PYTHON) roll.py model save-recommendation-html --limit 10

model-report:
	$(VENV_PYTHON) roll.py model report

model-review:
	$(VENV_PYTHON) roll.py model review

model-backtest:
	$(VENV_PYTHON) roll.py model backtest

model-backup:
	$(VENV_PYTHON) roll.py model backup

daily-run:
	$(VENV_PYTHON) roll.py daily-run

clawteam-runner:
	$(VENV_PYTHON) roll.py clawteam-runner

clean-local:
	rm -f qlib_bin.tar.gz
	rm -f tmp_head.bin
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
