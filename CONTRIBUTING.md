# Contributing

## Before You Start

- Use Python `3.9+`.
- Prefer working inside a local virtual environment.
- Keep changes focused and small when possible.

## Local Setup

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

If you need Qlib-related commands:

```bash
.venv/bin/python -m pip install -r requirements-qlib.txt
```

## Recommended Workflow

1. Create a branch for your change.
2. Make the smallest change that solves the problem.
3. Run the relevant checks locally.
4. Open a pull request with a short summary and test notes.

## Local Checks

Run the baseline test suite:

```bash
make test
```

Run a quick environment and data health check:

```bash
make doctor
```

If your change affects the training or reporting flow, these are useful spot checks:

```bash
make train-smoke
make model-report
```

## Pull Request Notes

Please include:

- What changed
- Why it changed
- How you verified it
- Any follow-up work or known limitations

## Scope Guidance

- Keep generated large files, downloaded archives, and local experiment outputs out of git.
- Prefer updating the package CLI instead of adding more standalone scripts.
- If a change introduces a new command or workflow, update the docs in `README.md` or `docs/`.
