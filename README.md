# IMF Currency Exchange Rate Pipeline

Automated monthly pipeline for fetching, validating, and archiving IMF exchange
rate data. Uses a **hybrid execution model** — local execution via Windows Task
Scheduler with Prefect Cloud for monitoring.

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
  └── 17th of month, 17:10
        └── run_CurrencyAcquisition.bat
              └── [Flow 1] currency_acquisition_flow
                    │  fetch IMF rates → validate → create Cloud artifacts
                    │  SUCCESS ──────────────────────────────────────────┐
                    │  FAIL → stops here, logs to Cloud + local log      │
                    ▼                                                     │
              [Flow 2] prepare_batch_flow  ◄────────────────────────────┘
                    │  scan 1_input → merge files → write MANIFEST.json
                    │  SUCCESS ──────────────────────────────────────────┐
                    │  FAIL → stops here                                 │
                    ▼                                                     │
              [Flow 3] process_batch_flow  ◄────────────────────────────┘
                    │  load MANIFEST → transform → archive to 4_archive/
                    └── done

All 3 flow runs visible in Prefect Cloud UI with full logs and status.
```

---

## Repository Structure

```
├── flows/
│   ├── currency_acquisition_flow.py   Flow 1: fetch + validate + chain
│   ├── prepare_batch_flow.py          Flow 2: prep files + manifest + chain
│   ├── process_batch_flow.py          Flow 3: transform + archive
│   └── historical_backfill_flow.py    One-shot backfill (2000 → present)
│
├── utils/
│   ├── config.py                      All paths and constants
│   ├── exchange_rate_fetcher.py       IMF API + REST Countries (parallel, cached)
│   ├── batch_prepare.py               Preprocessing + manifest creation
│   ├── core_processor.py              Core transformation + archiving
│   └── imf_data_validator.py          Data completeness validator
│
├── watcher/
│   └── local_file_event_watcher.py    Optional hotfolder event emitter
│
├── scripts/
│   ├── setup_new_machine.bat          First-time setup (run once)
│   ├── setup_task_scheduler.ps1       Register Windows Scheduled Tasks (run once, as Admin)
│   ├── start_worker.bat               Start Prefect worker (kept open / auto-started)
│   ├── run_CurrencyAcquisition.bat    Pipeline entry point (Task Scheduler target)
│   ├── run_PrepareBatch.bat           Manual re-run: Flow 2 only
│   ├── run_ProcessBatch.bat           Manual re-run: Flow 3 only
│   ├── run_pipeline_manual.bat        Manual full pipeline run (same as Task Scheduler)
│   └── run_backfill.bat               Trigger historical backfill
│
├── data/                              Exchange rate CSVs (git-ignored)
├── data_pipeline/                     Pipeline stage folders (git-ignored)
│   ├── 1_input/                       Drop partner/unit CSVs here
│   ├── 2_preprocessing/
│   ├── 3_processing_hotfolder/
│   ├── 4_archive/
│   ├── 5_error/
│   └── 6_logs/
├── logs/                              Task Scheduler local log (git-ignored)
├── prefect.yaml
└── requirements.txt
```

---

## First-Time Setup (New Machine)

### Step 1 — Clone and set up environment
```
git clone https://github.com/Jessy-Steve251/Prefect-IMF-Project.git
cd Prefect-IMF-Project
scripts\setup_new_machine.bat
```

This will:
- Create the Python virtual environment
- Install all dependencies
- Log you in to Prefect Cloud
- Save your GitHub token as a Prefect secret block
- Deploy all flows to Prefect Cloud

### Step 2 — Register Windows Scheduled Tasks
Right-click `scripts\setup_task_scheduler.ps1` → **Run with PowerShell** (as Administrator).

This registers two tasks:
- `Prefect-IMF-Worker` — starts the Prefect worker on boot + daily restart at 08:00
- `Prefect-IMF-Pipeline` — triggers Flow 1 on the 17th at 17:10

### Step 3 — Start the worker now (without rebooting)
```
scripts\start_worker.bat
```
Keep this window open. The Task Scheduler will manage it automatically after a reboot.

### Step 4 — Run historical backfill (first time only)
```
scripts\run_backfill.bat
```

---

## Monthly Operation

The pipeline runs automatically. Nothing needs to be done manually each month.

On the 17th at 17:10:
1. Task Scheduler fires `run_CurrencyAcquisition.bat`
2. Flow 1 fetches IMF data and validates it
3. Flow 2 prepares the batch artefacts
4. Flow 3 processes and archives the results

Monitor results at [app.prefect.cloud](https://app.prefect.cloud).

---

## Manual Re-runs

| Scenario | Script to run |
|---|---|
| Re-run the full pipeline | `scripts\run_pipeline_manual.bat` |
| Flow 1 succeeded, re-run from Flow 2 | `scripts\run_PrepareBatch.bat` |
| Flow 2 succeeded, re-run from Flow 3 | `scripts\run_ProcessBatch.bat` |
| Backfill missing historical data | `scripts\run_backfill.bat` |

---

## Configuration

All paths and constants are in `utils/config.py`.

| Setting | Default | How to override |
|---|---|---|
| `PIPELINE_ROOT` | `<repo>\data_pipeline` | Set `PIPELINE_ROOT` env var |
| `IMF_START_YEAR` | 2000 | Edit `config.py` |
| `MAX_MONTH_ON_MONTH_CHANGE` | 30% | Edit `config.py` |

Pipeline schedule day and time are set in `scripts\setup_task_scheduler.ps1`
(look for `-DaysOfMonth 17 -At "17:10"`).

---

## Troubleshooting

**Worker not running?**
```
scripts\start_worker.bat
```

**Task Scheduler not firing?**
```
schtasks /query /tn Prefect-IMF* /fo LIST
schtasks /run /tn Prefect-IMF-Pipeline
```

**Flow failed — where are the logs?**
- Prefect Cloud UI: full logs per flow run
- Local: `logs\task_scheduler.log` — exit codes and timestamps
- Error details: `data_pipeline\6_logs\<batch_id>_error.log`

**Want to re-fetch a month that already has a CSV?**
Delete `data\exchange_rates_YYYY_MM.csv` and run `scripts\run_CurrencyAcquisition.bat`.

**Validate any CSV manually:**
```
python utils\imf_data_validator.py --csv data\exchange_rates_2025_01.csv
```
