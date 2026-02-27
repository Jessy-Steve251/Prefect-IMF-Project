@echo off
chcp 65001 >nul

REM ============================================================
REM  run_PrepareBatch.bat
REM  MANUAL RE-RUN ONLY — Flow 2 (prepare_batch_flow)
REM
REM  Use this when Flow 2 failed and you want to re-run it
REM  without re-fetching data (Flow 1 already succeeded).
REM  Flow 2 will chain to Flow 3 on success.
REM
REM  NOT registered in Task Scheduler — triggered manually.
REM  All runs reported to Prefect Cloud for monitoring.
REM  Local log: logs\task_scheduler.log
REM ============================================================

cd /d "%~dp0.."
set LOG_FILE=%CD%\logs\task_scheduler.log

if not exist logs mkdir logs

echo.
echo  Manual re-run: prepare_batch_flow
echo  This will also trigger process_batch_flow on success.
echo.

echo %date% %time% - [PrepareBatch] Manual re-run starting >> "%LOG_FILE%"

call venv\Scripts\activate
if %ERRORLEVEL% NEQ 0 (
    echo %date% %time% - [PrepareBatch] FAILED - Could not activate venv >> "%LOG_FILE%"
    pause
    exit /b 1
)

prefect deployment run "prepare_batch_flow/prepare-batch" --watch

if %ERRORLEVEL% EQU 0 (
    echo %date% %time% - [PrepareBatch] SUCCESS >> "%LOG_FILE%"
    echo.
    echo Flow completed successfully. Check Prefect Cloud for details.
) else (
    echo %date% %time% - [PrepareBatch] FAILED (Error %ERRORLEVEL%) >> "%LOG_FILE%"
    echo.
    echo Flow failed. Check Prefect Cloud for error details:
    echo https://app.prefect.cloud
)

echo.
pause
