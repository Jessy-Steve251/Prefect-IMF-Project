@echo off
chcp 65001 >nul
title IMF Pipeline - Historical Backfill

echo ============================================================
echo   IMF Pipeline - Historical Backfill
echo ============================================================
echo.
echo This triggers a full backfill of IMF exchange rates.
echo Default range: 2000-01 to last month.
echo Already-complete months are skipped automatically.
echo.
echo Make sure the worker is running (scripts\start_worker.bat).
echo.

cd /d "%~dp0.."
call venv\Scripts\activate

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Could not activate virtual environment.
    echo Run setup_new_machine.bat first.
    pause
    exit /b 1
)

REM Ask if user wants a custom start date
set /p CUSTOM="Start from a specific year? (y/n, default=n): "
if /i "%CUSTOM%"=="y" (
    set /p START_YEAR="Enter start year (e.g. 2020): "
    set /p START_MONTH="Enter start month (e.g. 6): "
    echo.
    echo Triggering backfill from %START_YEAR%-%START_MONTH%...
    prefect deployment run "historical_backfill_flow/historical-backfill" ^
        --param start_year=%START_YEAR% ^
        --param start_month=%START_MONTH%
) else (
    echo.
    echo Triggering full backfill from 2000-01...
    prefect deployment run "historical_backfill_flow/historical-backfill"
)

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Backfill triggered successfully.
    echo This will take a while - monitor progress at: https://app.prefect.cloud
    echo It is safe to close this window. The backfill runs on the worker.
) else (
    echo.
    echo ERROR: Failed to trigger backfill.
    echo Check that the worker is running and you are logged in to Prefect Cloud.
)

echo.
pause
