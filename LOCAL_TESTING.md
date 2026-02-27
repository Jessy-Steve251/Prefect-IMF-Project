# Local Testing & GitHub Block Guide

---

## How to Test Locally (CMD)

Your existing workflow is still correct. Nothing about the rewrite
changes how you test from the command line.

### The two commands you always need first

```cmd
cd C:\Users\Admin\Projects\Prefect_Project-main
venv\Scripts\activate
```

After activation your prompt will show `(venv)` — that confirms you are
running the project's Python, not the system one.

---

## Option A — Run a flow directly as a Python script (fastest, no worker needed)

This is the quickest way to test a single flow in isolation.
Prefect still sends logs and run state to Cloud — you just don't need
the worker running.

```cmd
REM Test Flow 1 only (fetch + validate, does NOT chain to Flow 2)
python -m flows.currency_acquisition_flow

REM Test Flow 2 only (requires a valid exchange_rates CSV in data\)
python -m flows.prepare_batch_flow

REM Test Flow 3 only (requires a MANIFEST.json in the hotfolder)
python -m flows.process_batch_flow

REM Test historical backfill (safe — skips months already fetched)
python -m flows.historical_backfill_flow

REM Test the validator standalone on any CSV
python utils\imf_data_validator.py --csv data\exchange_rates_2025_01.csv
```

When you run a flow this way it executes **in-process** — you see output
directly in the CMD window and Prefect Cloud records the run.

> **Note on chaining:** In this mode Flow 1 will call `prepare_batch_flow()`
> directly at the end (as coded). If you want to test Flow 1 alone without
> triggering the chain, temporarily comment out the last two lines of
> `currency_acquisition_flow.py`:
> ```python
> # logger.info("Handing off to prepare_batch_flow...")
> # from flows.prepare_batch_flow import prepare_batch_flow
> # prepare_batch_flow()
> ```

---

## Option B — Run via the worker (mirrors production exactly)

This is how the pipeline runs in production. Start the worker in one
CMD window, trigger the flow in another.

### Window 1 — Start the worker
```cmd
cd C:\Users\Admin\Projects\Prefect_Project-main
venv\Scripts\activate
prefect worker start --pool Yichen_Test
```
Leave this window open. You will see the worker polling for work.

### Window 2 — Trigger a flow run
```cmd
cd C:\Users\Admin\Projects\Prefect_Project-main
venv\Scripts\activate

REM Trigger the full pipeline (Flow 1 → 2 → 3)
prefect deployment run "currency_acquisition_flow/currency-acquisition"

REM Or trigger individual flows
prefect deployment run "prepare_batch_flow/prepare-batch"
prefect deployment run "process_batch_flow/process-batch"
prefect deployment run "historical_backfill_flow/historical-backfill"

REM With parameters (e.g. backfill from a specific year)
prefect deployment run "historical_backfill_flow/historical-backfill" ^
  --param start_year=2023 --param start_month=1

REM Watch the run until it finishes (blocks the terminal)
prefect deployment run "currency_acquisition_flow/currency-acquisition" --watch
```

The worker in Window 1 picks up the run and executes it locally.
You see live logs in the worker window AND in Prefect Cloud UI.

---

## Option C — Run the .bat scripts directly (mirrors Task Scheduler exactly)

```cmd
scripts\run_CurrencyAcquisition.bat
scripts\run_PrepareBatch.bat
scripts\run_ProcessBatch.bat
scripts\run_backfill.bat
```

These are the exact same commands Task Scheduler runs. Use these for
end-to-end testing before setting up the scheduled tasks.

---

## Useful Prefect CLI commands for local debugging

```cmd
REM Check you are logged in to the right workspace
prefect cloud workspace ls

REM List all deployments (confirm they are registered)
prefect deployment ls

REM List recent flow runs and their status
prefect flow-run ls

REM View logs for a specific run (get the ID from `flow-run ls`)
prefect flow-run logs <RUN_ID>

REM Check the worker is connected and polling
prefect worker ls

REM Check work pool exists
prefect work-pool ls
```

---

## The GitHub Block — What It Is and When It Matters

### What it does

The GitHub block (`imf-github-repo`) stores two things:
- Your repository URL
- A reference to your `github-token` secret block (PAT with `repo` scope)

It is referenced in every deployment in `prefect.yaml`:
```yaml
pull:
  - prefect.deployments.steps.git_clone:
      repository: "{{ prefect.blocks.github-repository.imf-github-repo.repository_url }}"
      branch: "{{ prefect.blocks.github-repository.imf-github-repo.reference }}"
      credentials:
        access_token: "{{ prefect.blocks.secret.github-token }}"
```

### When it is used (and when it is NOT)

| Scenario | GitHub block used? |
|---|---|
| `python -m flows.currency_acquisition_flow` (Option A) | ❌ No — runs code from disk |
| `prefect deployment run ...` with local worker (Option B) | ✅ Yes — worker clones repo fresh |
| `.bat` scripts via Task Scheduler (production) | ✅ Yes — worker clones repo fresh |
| `python utils\imf_data_validator.py` standalone | ❌ No |

When the worker picks up a deployment run it executes the `pull` steps
first — cloning a fresh copy of the repo from GitHub into a temp folder,
then running the flow from that clean copy. This ensures production always
runs the latest committed code, not whatever happens to be on disk.

This is why **you must `git push` any changes before triggering a
deployment run via the worker** — the worker ignores your local
uncommitted edits.

### Creating / verifying the blocks

`setup_new_machine.bat` creates both blocks automatically. If you need
to recreate them manually:

```cmd
cd C:\Users\Admin\Projects\Prefect_Project-main
venv\Scripts\activate

python -c "
from prefect.blocks.system import Secret
Secret(value='YOUR_GITHUB_PAT_HERE').save(name='github-token', overwrite=True)
print('Secret block saved: github-token')
"

python -c "
from prefect_github import GitHubRepository
GitHubRepository(
    repository_url='https://github.com/Jessy-Steve251/Prefect-IMF-Project.git',
    reference='main'
).save('imf-github-repo', overwrite=True)
print('GitHub block saved: imf-github-repo')
"
```

Verify they exist in Prefect Cloud UI under **Blocks**.

### Recommended local dev workflow

```
Edit code locally
      │
      ▼
Test with Option A (python -m flows.xxx)  ← fast, no worker needed
      │
      ▼
Commit and push to GitHub (git push)
      │
      ▼
Test with Option B (worker + deployment run)  ← mirrors production
      │
      ▼
Confirm in Prefect Cloud UI
      │
      ▼
Task Scheduler runs it automatically on the 17th
```

---

## Quick Reference Card

```
ACTIVATE ENVIRONMENT (always first):
  cd C:\Users\Admin\Projects\Prefect_Project-main
  venv\Scripts\activate

FAST LOCAL TEST (no worker):
  python -m flows.currency_acquisition_flow

PRODUCTION-EQUIVALENT TEST (worker needed in another window):
  prefect worker start --pool Yichen_Test     ← Window 1
  prefect deployment run "currency_acquisition_flow/currency-acquisition" --watch  ← Window 2

CHECK EVERYTHING IS WIRED UP:
  prefect deployment ls
  prefect work-pool ls
  prefect cloud workspace ls
```
