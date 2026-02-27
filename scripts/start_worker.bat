@echo off
chcp 65001 >nul
title Prefect Worker - IMF Pipeline

echo ============================================================
echo   PREFECT WORKER - IMF Currency Pipeline
echo ============================================================
echo.
echo Role:   Executes flows locally when triggered by Task Scheduler
echo         Reports all run results to Prefect Cloud for monitoring
echo Pool:   Yichen_Test
echo.
echo This window must stay open for the pipeline to run.
echo It is registered as a startup task — do not close it manually.
echo.

REM Navigate to repo root (this script lives in scripts\)
cd /d "%~dp0.."

call venv\Scripts\activate
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Could not activate virtual environment.
    echo Run scripts\setup_new_machine.bat first.
    pause
    exit /b 1
)

echo Worker starting...
echo.
prefect worker start --pool Yichen_Test

echo.
echo Worker stopped unexpectedly. Press any key to close.
pause
