@echo off
chcp 65001 >nul
title Prefect IMF Project - Auto Setup
color 0A

echo ============================================================
echo    PREFECT IMF PROJECT - AUTOMATED SETUP
echo ============================================================
echo.
echo This script will prepare the project for a new computer
echo.

REM Get GitHub token
echo.
echo ============================================================
echo GITHUB TOKEN SETUP
echo ============================================================
echo.
echo Create a token at: https://github.com/settings/tokens
echo (Select 'repo' scope)
echo.
set /p GITHUB_TOKEN="Enter your GitHub token: "

if "%GITHUB_TOKEN%"=="" (
    echo ❌ No token entered. Exiting.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo STEP 1: Creating virtual environment
echo ============================================================
if not exist venv (
    python -m venv venv
    echo ✅ Virtual environment created
) else (
    echo ⏭️  Virtual environment already exists
)

echo.

echo ============================================================
echo STEP 2: Installing dependencies
echo ============================================================
call venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install prefect-github
echo ✅ Dependencies installed
echo.

echo ============================================================
echo STEP 3: Creating GitHub token secret
echo ============================================================
python -c "
from prefect.blocks.system import Secret
Secret(value='%GITHUB_TOKEN%').save(name='github-token', overwrite=True)
print('✅ GitHub token saved as secret block')
"
echo.

echo ============================================================
echo STEP 4: Creating GitHub block
echo ============================================================
python -c "
from prefect_github import GitHubRepository
github_block = GitHubRepository(
    repository_url='https://github.com/Jessy-Steve251/Prefect-IMF-Project.git',
    reference='main'
)
github_block.save('imf-github-repo', overwrite=True)
print('✅ GitHub block created')
"
echo.

echo ============================================================
echo STEP 5: Updating paths for this computer
echo ============================================================
set "CURRENT_PATH=%CD%"
echo Current directory: %CURRENT_PATH%

REM Update batch files with current path
powershell -Command "(gc run_Prefect-CurrencyAcquisition.bat) -replace 'C:\\\\Users\\\\Admin\\\\Projects\\\\Prefect_Project-main', '%CURRENT_PATH:\=\\%' | Out-File -encoding ASCII run_Prefect-CurrencyAcquisition.bat"
powershell -Command "(gc run_Prefect-PrepareBatch.bat) -replace 'C:\\\\Users\\\\Admin\\\\Projects\\\\Prefect_Project-main', '%CURRENT_PATH:\=\\%' | Out-File -encoding ASCII run_Prefect-PrepareBatch.bat"
powershell -Command "(gc run_Prefect-ProcessBatch.bat) -replace 'C:\\\\Users\\\\Admin\\\\Projects\\\\Prefect_Project-main', '%CURRENT_PATH:\=\\%' | Out-File -encoding ASCII run_Prefect-ProcessBatch.bat"
powershell -Command "(gc run_all_pipeline.bat) -replace 'C:\\\\Users\\\\Admin\\\\Projects\\\\Prefect_Project-main', '%CURRENT_PATH:\=\\%' | Out-File -encoding ASCII run_all_pipeline.bat"
powershell -Command "(gc setup_task_scheduler_schtasks.ps1) -replace 'C:\\\\Users\\\\Admin\\\\Projects\\\\Prefect_Project-main', '%CURRENT_PATH:\=\\%' | Out-File -encoding ASCII setup_task_scheduler_schtasks.ps1"

echo ✅ Paths updated
echo.

echo ============================================================
echo 🎉 SETUP COMPLETE!
echo ============================================================
echo.
echo NEXT STEPS:
echo.
echo 1️⃣  Login to Prefect Cloud:
echo    prefect cloud login
echo.
echo 2️⃣  Deploy flows:
echo    prefect deploy --all
echo.
echo 3️⃣  Setup Task Scheduler (Run as Administrator):
echo    powershell -ExecutionPolicy Bypass -File setup_task_scheduler_schtasks.ps1
echo.
echo 4️⃣  Test the pipeline:
echo    run_all_pipeline.bat
echo.
echo 5️⃣  View in Prefect Cloud: https://app.prefect.cloud
echo.
pause