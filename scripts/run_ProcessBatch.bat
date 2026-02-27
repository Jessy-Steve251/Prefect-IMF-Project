@echo off
chcp 65001 >nul

REM ============================================================
REM  run_ProcessBatch.bat
REM  MANUAL RE-RUN ONLY — Flow 3 (process_batch_flow)
REM
REM  Use this when Flow 3 failed and you want to re-run it
REM  without re-running Flows 1 or 2. The latest MANIFEST.json
REM  in the hotfolder will be used automatically.
REM
REM  NOT registered in Task Scheduler — triggered manually.
REM  All runs reported to Prefect Cloud for monitoring.
REM  Local log: logs\task_scheduler.log
REM ============================================================

cd /d "%~dp0.."
set LOG_FILE=%CD%\logs\task_scheduler.log

if not exist logs mkdir logs

echo.
echo  Manual re-run: process_batch_flow
echo  The latest manifest in the hotfolder will be used.
echo.

echo %date% %time% - [ProcessBatch] Manual re-run starting >> "%LOG_FILE%"

call venv\Scripts\activate
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - [ProcessBatch] FAILED - Could not activate venv >> "%LOG_FILE%"
    pause
    exit /b 1
)

prefect deployment run "process_batch_flow/process-batch" --watch

if %ERRORLEVEL% EQU 0 (
    echo %date% %time% - [ProcessBatch] SUCCESS >> "%LOG_FILE%"
    echo.
    echo Flow completed successfully. Check Prefect Cloud for details.
) else (
    echo %date% %time% - [ProcessBatch] FAILED (Error %ERRORLEVEL%) >> "%LOG_FILE%"
    echo.
    echo Flow failed. Check Prefect Cloud for error details:
    echo https://app.prefect.cloud
)

echo.
pause
