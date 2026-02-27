@echo off
chcp 65001 >nul
title IMF Pipeline - Manual Full Run

echo ============================================================
echo   IMF Pipeline - Full Manual Run
echo ============================================================
echo.
echo Triggers the complete pipeline manually:
echo   Flow 1: currency_acquisition_flow  (fetches + validates)
echo     chains to Flow 2 on success
echo   Flow 2: prepare_batch_flow         (prepares + manifest)
echo     chains to Flow 3 on success
echo   Flow 3: process_batch_flow         (processes + archives)
echo.
echo All runs are reported to Prefect Cloud for monitoring.
echo Make sure the worker is running (scripts\start_worker.bat).
echo.

cd /d "%~dp0.."

call venv\Scripts\activate
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Could not activate virtual environment.
    echo Run scripts\setup_new_machine.bat first.
    pause
    exit /b 1
)

call "%~dp0run_CurrencyAcquisition.bat"

echo.
echo Monitor at: https://app.prefect.cloud
echo.
pause
