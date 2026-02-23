@echo off
chcp 65001 >nul
cd /d "C:\Users\Admin\Projects\Prefect_Project-main"

echo ================================
echo UPLOADING TO YOUR GITHUB
echo ================================
echo.

REM Check if there are changes to commit
git status --porcelain | findstr . >nul
if %ERRORLEVEL% NEQ 0 (
    echo 📝 No changes to commit. Repository is up to date.
    pause
    exit /b 0
)

echo Current changes:
git status -s
echo.

echo ================================
echo Adding all files...
git add .

echo.
echo ================================
set /p commit_msg="Enter commit message (or press Enter for default): "
if "%commit_msg%"=="" set commit_msg="Update Prefect IMF project"
git commit -m "%commit_msg%"

echo.
echo ================================
echo Pushing to GitHub...
git push origin main

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ================================
    echo ✅ UPLOAD SUCCESSFUL!
    echo ================================
    echo https://github.com/Jessy-Steve251/Prefect-IMF-Project
) else (
    echo.
    echo ================================
    echo ❌ UPLOAD FAILED
    echo ================================
)

pause