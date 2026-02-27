@echo off
REM ============================================================
REM  Cross-Validate stored data against live IMF API
REM ============================================================
REM
REM  Usage:
REM    run_cross_validate.bat                          Validate ALL months
REM    run_cross_validate.bat --sample 24              Validate 24 random months
REM    run_cross_validate.bat --start 2020-01 --end 2024-12   Specific range
REM
REM  This compares your stored CSVs against fresh IMF API data
REM  to detect any rate mismatches, missing countries, or stale data.
REM ============================================================

cd /d "%~dp0.."
call .venv\Scripts\activate

echo Starting cross-validation against IMF...
python utils/imf_data_validator.py --cross-validate %*

echo.
echo Exit code: %ERRORLEVEL%
pause
