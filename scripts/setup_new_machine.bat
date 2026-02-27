@echo off
chcp 65001 >nul
title Prefect IMF Project - First-Time Setup
color 0A

echo ============================================================
echo   PREFECT IMF PROJECT - FIRST-TIME SETUP
echo ============================================================
echo.
echo Run this once from the root of the cloned repository.
echo It will set up everything needed for the hybrid pipeline.
echo.

cd /d "%~dp0.."
echo Working directory: %CD%
echo.

REM ---------------------------------------------------------------
REM STEP 1: Check Python
REM ---------------------------------------------------------------
echo [1/7] Checking Python...
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ERROR: Python not found. Install Python 3.11+ and add it to PATH.
    echo Download: https://www.python.org/downloads/
    pause
    exit /b 1
)
python --version
echo.

REM ---------------------------------------------------------------
REM STEP 2: Create virtual environment
REM ---------------------------------------------------------------
echo [2/7] Creating virtual environment...
if not exist venv (
    python -m venv venv
    echo Virtual environment created.
) else (
    echo Virtual environment already exists - skipping.
)
call venv\Scripts\activate
echo.

REM ---------------------------------------------------------------
REM STEP 3: Install dependencies
REM ---------------------------------------------------------------
echo [3/7] Installing dependencies...
python -m pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
pip install prefect-github --quiet
echo Done.
echo.

REM ---------------------------------------------------------------
REM STEP 4: Prefect Cloud login
REM ---------------------------------------------------------------
echo [4/7] Prefect Cloud login...
echo.
prefect cloud login
echo.

REM ---------------------------------------------------------------
REM STEP 5: Create GitHub PAT secret block
REM   This stores your GitHub Personal Access Token in Prefect Cloud.
REM   The Prefect worker uses it to clone the repo when running flows.
REM   Required scope: repo (read access to your repository)
REM ---------------------------------------------------------------
echo [5/7] Creating GitHub token secret block...
echo.
echo The Prefect worker clones your GitHub repo each time it runs a flow.
echo It needs a Personal Access Token (PAT) with 'repo' scope to do this.
echo.
echo Create a PAT at: https://github.com/settings/tokens
echo   - Click 'Generate new token (classic)'
echo   - Select scope: repo
echo   - Copy the token (you only see it once)
echo.
set /p GITHUB_TOKEN="Paste your GitHub PAT here: "

if "%GITHUB_TOKEN%"=="" (
    echo No token entered - skipping block creation.
    echo You will need to create the blocks manually before running flows.
    goto STEP6
)

python -c "
from prefect.blocks.system import Secret
Secret(value='%GITHUB_TOKEN%').save(name='github-token', overwrite=True)
print('  Secret block saved: github-token')
"
echo.

REM ---------------------------------------------------------------
REM STEP 6: Create GitHub repository block
REM   This tells Prefect which repo and branch to clone.
REM   References the github-token secret created above.
REM ---------------------------------------------------------------
:STEP6
echo [6/7] Creating GitHub repository block...
python -c "
from prefect_github import GitHubRepository
GitHubRepository(
    repository_url='https://github.com/Jessy-Steve251/Prefect-IMF-Project.git',
    reference='main'
).save('imf-github-repo', overwrite=True)
print('  GitHub repository block saved: imf-github-repo')
print('  Block name: imf-github-repo')
print('  Repo URL:   https://github.com/Jessy-Steve251/Prefect-IMF-Project.git')
print('  Branch:     main')
"
echo.
echo Both blocks are now visible in Prefect Cloud under: Blocks
echo.

REM ---------------------------------------------------------------
REM STEP 7: Deploy all flows to Prefect Cloud
REM   This registers the 4 deployments for monitoring.
REM   No cloud schedules are created - execution is via Task Scheduler.
REM ---------------------------------------------------------------
echo [7/7] Deploying flows to Prefect Cloud...
echo.
echo Note: This registers deployments for monitoring only.
echo       Schedules are managed by Windows Task Scheduler, not Prefect Cloud.
echo.
prefect deploy --all
echo.

REM ---------------------------------------------------------------
REM Done
REM ---------------------------------------------------------------
echo ============================================================
echo   SETUP COMPLETE
echo ============================================================
echo.
echo NEXT STEPS:
echo.
echo 1. Verify blocks in Prefect Cloud UI:
echo    https://app.prefect.cloud ^> Blocks
echo    You should see: github-token, imf-github-repo
echo.
echo 2. Register Windows Scheduled Tasks (run as Administrator):
echo    Right-click scripts\setup_task_scheduler.ps1
echo    ^> Run with PowerShell
echo.
echo 3. Start the worker now (test before reboot):
echo    scripts\start_worker.bat
echo.
echo 4. Test the pipeline locally:
echo    Option A (fast, no worker needed):
echo      python -m flows.currency_acquisition_flow
echo.
echo    Option B (mirrors production):
echo      Window 1: scripts\start_worker.bat
echo      Window 2: scripts\run_CurrencyAcquisition.bat
echo.
echo 5. Run historical backfill (first time only):
echo    scripts\run_backfill.bat
echo.
echo 6. Full testing guide: LOCAL_TESTING.md
echo.
pause
