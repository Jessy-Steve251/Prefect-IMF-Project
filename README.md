# IMF Currency Exchange Rate Pipeline

Automated monthly pipeline for fetching, validating, and archiving IMF exchange
rate data. Uses a **hybrid execution model** — local execution via Windows Task
Scheduler with Prefect Cloud for monitoring.

---

## What's New (v2)

- **Chunked historical backfill**: Fetches data in yearly API calls instead of 300+ individual monthly calls (2000-present in ~25 API calls)
- **Cross-validation**: Compare stored CSVs against live IMF API values to detect rate mismatches, missing countries, and stale data
- **Rate accuracy checks**: Single-month validation now includes actual rate comparison against IMF
- **Force-refetch**: Overwrite existing data when needed
- **Date-range fetching**: Single API call returns multiple months of data

---

## Architecture

| Layer | Component | Role |
|---|---|---|
| Cloud | Prefect Cloud Schedules | Disabled — monitoring only |
| Cloud | Prefect Cloud UI | View logs, run history, artifacts |
| Local | Windows Task Scheduler | **Triggers flows at scheduled times** |
| Local | Prefect Worker | **Executes flow code locally** |
| Local | Python Interpreter | Runs flow logic |
| Local | Data Storage | Stores exchange rate CSVs and archives |

### Flow Chain

Task Scheduler fires **only Flow 1**. Each flow calls the next directly on
success — no time gaps, no race conditions, no cloud scheduling involved.

```
Windows Task Scheduler
  +-- 17th of month, 17:10
        +-- run_CurrencyAcquisition.bat
              +-- [Flow 1] currency_acquisition_flow
                    |  fetch IMF rates -> validate (incl. rate accuracy) -> artifacts
                    |  SUCCESS ---+
                    |  FAIL -> stops here
                    v             |
              [Flow 2] prepare_batch_flow  <---+
                    |  scan 1_input -> merge files -> write MANIFEST.json
                    |  SUCCESS ---+
                    |  FAIL -> stops here
                    v             |
              [Flow 3] process_batch_flow  <---+
                    |  load MANIFEST -> transform -> archive to 4_archive/
                    +-- done
```

### Historical Backfill (separate flow)

```
run_backfill.bat
  +-- historical_backfill_flow
        |  Chunked fetch: 2000-01 -> present (yearly API calls)
        |  Build combined exchange_rates_ALL.csv
        |  Optional: cross-validate against live IMF data
        +-- done
```

---

## Repository Structure

```
+-- flows/
|   +-- currency_acquisition_flow.py   Flow 1: fetch + validate + chain
|   +-- prepare_batch_flow.py          Flow 2: prep files + manifest + chain
|   +-- process_batch_flow.py          Flow 3: transform + archive
|   +-- historical_backfill_flow.py    Backfill (2000->present) + cross-validation
|
+-- utils/
|   +-- config.py                      All paths and constants
|   +-- exchange_rate_fetcher.py       IMF API (single, range, chunked) + REST Countries
|   +-- batch_prepare.py               Preprocessing + manifest creation
|   +-- core_processor.py              Core transformation + archiving
|   +-- imf_data_validator.py          Validation + cross-validation against IMF
|
+-- watcher/
|   +-- local_file_event_watcher.py    Optional hotfolder event emitter
|
+-- scripts/
|   +-- setup_new_machine.bat          First-time setup (run once)
|   +-- setup_task_scheduler.ps1       Register Windows Scheduled Tasks
|   +-- start_worker.bat               Start Prefect worker
|   +-- run_CurrencyAcquisition.bat    Pipeline entry point
|   +-- run_PrepareBatch.bat           Manual re-run: Flow 2 only
|   +-- run_ProcessBatch.bat           Manual re-run: Flow 3 only
|   +-- run_pipeline_manual.bat        Manual full pipeline run
|   +-- run_backfill.bat               Historical backfill (2000->present)
|   +-- run_cross_validate.bat         Cross-validate stored data vs IMF
|
+-- data/                              Exchange rate CSVs (git-ignored)
+-- data_pipeline/                     Pipeline stage folders (git-ignored)
```

---

## First-Time Setup (New Machine)

### Step 1 — Clone and set up environment
```
git clone https://github.com/Jessy-Steve251/Prefect-IMF-Project.git
cd Prefect-IMF-Project
scripts\setup_new_machine.bat
```

### Step 2 — Register Windows Scheduled Tasks
Right-click `scripts\setup_task_scheduler.ps1` -> **Run with PowerShell** (as Administrator).

### Step 3 — Start the worker
```
scripts\start_worker.bat
```

### Step 4 — Run historical backfill (first time only)
```
scripts\run_backfill.bat
```
This fetches 2000-present in ~25 yearly API calls (takes a few minutes).

### Step 5 — (Optional) Cross-validate the data
```
scripts\run_cross_validate.bat --sample 24
```
Validates 24 random months against live IMF values.

---

## Monthly Operation

The pipeline runs automatically on the 17th at 17:10.

Monitor at [app.prefect.cloud](https://app.prefect.cloud).

---

## Manual Operations

| Scenario | Command |
|---|---|
| Re-run full pipeline | `scripts\run_pipeline_manual.bat` |
| Flow 2+ only | `scripts\run_PrepareBatch.bat` |
| Flow 3 only | `scripts\run_ProcessBatch.bat` |
| Backfill 2000->present | `scripts\run_backfill.bat` |
| Force re-fetch all history | `scripts\run_backfill.bat --force` |
| Backfill + validate all | `scripts\run_backfill.bat --validate-all` |
| Backfill from 2020 only | `scripts\run_backfill.bat --start-year 2020` |
| Cross-validate all months | `scripts\run_cross_validate.bat` |
| Cross-validate 24 random months | `scripts\run_cross_validate.bat --sample 24` |
| Cross-validate specific range | `scripts\run_cross_validate.bat --start 2020-01 --end 2024-12` |
| Validate single CSV | `python utils\imf_data_validator.py --csv data\exchange_rates_2025_01.csv` |
| Validate CSV + rate check | Same as above (rate check is now on by default) |
| Validate CSV without rate check | `python utils\imf_data_validator.py --csv data\exchange_rates_2025_01.csv --no-rate-check` |

---

## How Fetching Works

### Monthly fetch (Flow 1)
Single API call for one month -> saves `exchange_rates_YYYY_MM.csv`

### Historical backfill (chunked)
Instead of 300+ individual API calls:
```
Chunk 1:  2000-01 -> 2000-12  (1 API call, 12 months of data)
Chunk 2:  2001-01 -> 2001-12  (1 API call)
...
Chunk 25: 2024-01 -> 2024-12  (1 API call)
Chunk 26: 2025-01 -> 2025-12  (1 API call, partial year)
```
Each chunk's response is split into individual monthly CSVs.

### Cross-validation
For each month being validated:
1. Load stored CSV from `data/`
2. Fetch fresh rates from IMF API
3. Compare every country's rate (tolerance: 0.1%)
4. Flag mismatches

---

## Validation Checks

### Single-month validation (runs automatically in Flow 1)
| Check | Action |
|---|---|
| Country coverage | Compares CSV countries vs IMF API |
| Null currency codes | Flags unresolved currencies |
| Duplicate records | Same Country+Date appearing twice |
| Rate anomalies | Zero, negative, implausibly large/small |
| Date correctness | Rows stamped with wrong month |
| Month-on-month | Rates that moved >30% (warning only) |
| **Rate accuracy** | **Compares values against live IMF data** |

### Cross-validation (run on demand)
Compares stored historical CSVs against fresh IMF API pulls to detect:
- Rate drift (values changed since original fetch)
- Missing countries (IMF added data retroactively)
- Stale data (your CSV has old values)

---

## Configuration

All paths and constants are in `utils/config.py`.

| Setting | Default | Description |
|---|---|---|
| `PIPELINE_ROOT` | `<repo>\data_pipeline` | Override via env var |
| `IMF_START_YEAR` | 2000 | First year for backfill |
| `MAX_MONTH_ON_MONTH_CHANGE` | 30% | MoM flag threshold |
| `RATE_MISMATCH_TOLERANCE` | 0.1% | Cross-validation tolerance |
| `BACKFILL_CHUNK_SIZE` | 12 | Months per API call |

---

## Troubleshooting

**Worker not running?** `scripts\start_worker.bat`

**Re-fetch a specific month?** Delete `data\exchange_rates_YYYY_MM.csv` and run the pipeline.

**Force re-fetch everything?** `scripts\run_backfill.bat --force`

**Validate manually?** `python utils\imf_data_validator.py --csv data\exchange_rates_2025_01.csv`

**Cross-validate a range?** `scripts\run_cross_validate.bat --start 2020-01 --end 2024-12`
