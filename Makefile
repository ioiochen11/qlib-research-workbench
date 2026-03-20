PYTHON ?= python3
VENV_PYTHON ?= .venv/bin/python

.PHONY: test probe status verify qlib-check doctor train-plan train-smoke model-top model-report model-review model-backtest model-backup clean-local

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

train-plan:
	$(VENV_PYTHON) roll.py train plan

train-smoke:
	$(VENV_PYTHON) roll.py train smoke

model-top:
	$(VENV_PYTHON) roll.py model top --limit 10

model-report:
	$(VENV_PYTHON) roll.py model report

model-review:
	$(VENV_PYTHON) roll.py model review

model-backtest:
	$(VENV_PYTHON) roll.py model backtest

model-backup:
	$(VENV_PYTHON) roll.py model backup

clean-local:
	rm -f qlib_bin.tar.gz
	rm -f tmp_head.bin
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
