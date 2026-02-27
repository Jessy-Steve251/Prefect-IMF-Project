@echo off
chcp 65001 >nul

REM ============================================================
REM  run_CurrencyAcquisition.bat
REM  PRIMARY PIPELINE ENTRY POINT — triggered by Windows Task Scheduler
REM
REM  Runs Flow 1 (currency_acquisition_flow) locally via the Prefect worker.
REM  Flow 1 chains to Flow 2 (prepare_batch) on success,
REM  which chains to Flow 3 (process_batch) on success.
REM
REM  All runs are reported to Prefect Cloud for monitoring.
REM  Local log: logs\task_scheduler.log
REM ============================================================

cd /d "%~dp0.."
set LOG_FILE=%CD%\logs\task_scheduler.log

REM Ensure logs directory exists
if not exist logs mkdir logs

echo %date% %time% - [CurrencyAcquisition] Starting >> "%LOG_FILE%"

call venv\Scripts\activate
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - [CurrencyAcquisition] FAILED - Could not activate venv >> "%LOG_FILE%"
    exit /b 1
)

REM Run flow directly through the local worker
prefect deployment run "currency_acquisition_flow/currency-acquisition" --watch

if %ERRORLEVEL% EQU 0 (
    echo %date% %time% - [CurrencyAcquisition] SUCCESS >> "%LOG_FILE%"
) else (
    echo %date% %time% - [CurrencyAcquisition] FAILED (Error %ERRORLEVEL%) >> "%LOG_FILE%"
    echo %date% %time% - Check Prefect Cloud for details: https://app.prefect.cloud >> "%LOG_FILE%"
    exit /b %ERRORLEVEL%
)
