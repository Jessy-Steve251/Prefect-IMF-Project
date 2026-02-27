@echo off
REM ============================================================
REM  Historical Backfill - Fetch 2000 to present
REM ============================================================
REM
REM  Usage:
REM    run_backfill.bat                     Standard (skip existing)
REM    run_backfill.bat --force             Re-fetch everything
REM    run_backfill.bat --validate-all      Fetch + cross-validate
REM    run_backfill.bat --sample 24         Cross-validate 24 random months
REM
REM  This uses chunked yearly API calls (much faster than month-by-month).
REM ============================================================

cd /d "%~dp0.."
call .venv\Scripts\activate

echo Starting historical backfill (2000 -> present)...
python flows/historical_backfill_flow.py %*

echo.
echo Exit code: %ERRORLEVEL%
pause
