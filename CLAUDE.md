# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Layout

The working project lives in `ml_pipeline_demo/` — a Python ML pipeline for prescriptive maintenance on industrial equipment (regression target: days-until-next-maintenance). The sibling `sabo/` directory contains archived course material (`.rar`) and is not code to be modified. `fluxos.drawio` is the canonical spec the pipeline mirrors.

## Environment & Commands

Two virtualenvs exist at the repo root (`.venv/`, `venv/`). Activate one before running anything:

```bash
source .venv/bin/activate
pip install -r ml_pipeline_demo/requirements.txt
```

Pipeline entry points — **always run from `ml_pipeline_demo/scripts/`**, because `run_pipeline.py` does `os.chdir(OUTPUTS_DIR)` and then imports sibling step modules by plain name via `sys.path` insertion:

```bash
cd ml_pipeline_demo/scripts

python run_pipeline.py                    # full pipeline (steps 0,1,2,3,3b,4,5,6)
python run_pipeline.py --step 4           # single step (0,1,2,3,3b,4,5,6)
python run_pipeline.py --list             # list steps
python run_pipeline.py --diagram          # show ASCII flow
python run_pipeline.py --history          # past runs
python run_pipeline.py --compare 5        # markdown table of last N runs
python run_pipeline.py --inicio 2024-01-01 --fim 2024-12-31 --version R12_v1

python auto_pipeline.py                   # one-shot change detection
python auto_pipeline.py --watch           # continuous watch (default 300s)
python auto_pipeline.py --force           # force rerun
python auto_pipeline.py --status          # show tracked file hashes

python generate_dummy_data.py             # regenerate dummy data in data/raw/
```

There is no test suite, linter config, or CI in this repo.

## Architecture

### Pipeline shape (the core mental model)
Six numbered stages plus an optional `0` (split unified workbook) and `3b` (advanced EDA). Each stage is a standalone module `sXX_<name>.py` exposing a `main(**pipeline_context)` function; `run_pipeline.py` dispatches via `__import__(script_name)`. The declarative `PIPELINE_STEPS` dict in `run_pipeline.py` is the source of truth for order, inputs/outputs, and which steps are `optional` (optional-step failure doesn't abort the pipeline).

```
raw CSV/XLSX (data/raw/EQ-*.csv + data/manutencao/*.csv)
  → s01 data_raw.csv
  → s02 data_preprocessed.csv         (target = days-until-next-maintenance)
  → s03/s03b data_eda.csv + eda_plots/
  → s04 models/*.joblib + train_test_split.npz
  → s05 best_model.joblib + evaluation_report.txt
  → s06 Report_DemoML_R<N>.pdf
```

### Data flow contract
Stages communicate through **files in `outputs/`**, not in-memory objects. `main()` return values are logged to history but are not how the next stage gets its input — each stage reads its inputs from disk. If you change an output filename/location in one stage, every downstream stage that consumes it must be updated.

### Path configuration
`config/paths.py` centralizes every directory/file path and auto-creates directories on import (`ensure_directories()` runs at import time). Stage scripts import from `paths` with a relative-path fallback — so they can run standalone from `scripts/` even if `config/` isn't on `sys.path`. Prefer adding new paths there rather than hardcoding.

### Auto-pipeline change detection
`auto_pipeline.py` hashes files under `data/raw/`, `data/manutencao/`, and `data/arquivo_unico/` into `outputs/.data_state.json`. Any hash/mtime delta triggers a rerun. Don't hand-edit `.data_state.json`; use `--force` instead.

### Run history
`HistoryManager` (`scripts/history_manager.py`) stamps each run with `YYYYMMDD_HHMMSS` and writes `outputs/history/runs/run_<id>.json` plus `outputs/history/reports/report_<id>.txt`. Step results flow in via `history.log_step(script_name, results)` inside `run_step`. `--compare N` uses this to build a metrics table; `--no-history` disables it.

### Report versioning
`s06_generate_report.py` auto-increments `Report_DemoML_R<N>.pdf` unless `--version` or `--suffix` overrides it. The `version`/`suffix`/`inicio`/`fim` flags flow through `pipeline_context` and are passed as kwargs to every stage's `main()`, so new stages should accept `**kwargs` even if they ignore them.

## Conventions in this codebase

- Code and user-facing strings are primarily **Portuguese (pt-BR)**. Keep new messages, docstrings, and CLI help in Portuguese to match.
- Stage modules follow a fixed header: a top docstring describing the fluxos.drawio stage, `sys.path` insertion for `config/`, then a `paths` import with a fallback block.
- `outputs/` is the working directory during a run — stages write relative paths expecting to be there.
