# setup_task_scheduler.ps1
# =============================================================================
# Registers Windows Scheduled Tasks for the IMF hybrid pipeline.
#
# HYBRID MODEL:
#   - Task Scheduler EXECUTES  (triggers flows locally)
#   - Prefect Cloud  MONITORS  (logs, UI, run history only)
#
# Tasks registered:
#   1. Prefect-IMF-Worker         — starts Prefect worker on boot + daily 08:00
#   2. Prefect-IMF-Pipeline       — triggers Flow 1 on the 17th at 17:10
#                                   (Flow 1 chains to Flow 2 → Flow 3 locally)
#
# Run as Administrator:
#   Right-click this file > Run with PowerShell
#   or: powershell -ExecutionPolicy Bypass -File scripts\setup_task_scheduler.ps1
# =============================================================================

#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Paths — derived from this script's location (repo root\scripts\)
# ---------------------------------------------------------------------------
$ScriptsDir      = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot        = Split-Path -Parent $ScriptsDir
$VenvPython      = Join-Path $RepoRoot "venv\Scripts\python.exe"
$StartWorkerBat  = Join-Path $ScriptsDir "start_worker.bat"
$AcquisitionBat  = Join-Path $ScriptsDir "run_CurrencyAcquisition.bat"

Write-Host ""
Write-Host "============================================================"
Write-Host "  IMF Pipeline - Windows Task Scheduler Setup"
Write-Host "  Hybrid Model: Local execution + Prefect Cloud monitoring"
Write-Host "============================================================"
Write-Host ""
Write-Host "  Repo root      : $RepoRoot"
Write-Host "  Python         : $VenvPython"
Write-Host "  Worker script  : $StartWorkerBat"
Write-Host "  Pipeline entry : $AcquisitionBat"
Write-Host ""

# Validate
foreach ($path in @($VenvPython, $StartWorkerBat, $AcquisitionBat)) {
    if (-not (Test-Path $path)) {
        Write-Host "ERROR: Not found: $path"
        Write-Host "Run scripts\setup_new_machine.bat first."
        Read-Host "Press Enter to exit"
        exit 1
    }
}

# Shared task settings
$Settings = New-ScheduledTaskSettingsSet `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew

$Principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType S4U `
    -RunLevel Highest

# ---------------------------------------------------------------------------
# Task 1: Start Prefect worker on boot + daily restart at 08:00
# The worker must be running for ANY flow to execute locally.
# ---------------------------------------------------------------------------
$WorkerTaskName = "Prefect-IMF-Worker"
Write-Host "[1/2] Registering worker task: $WorkerTaskName"

if (Get-ScheduledTask -TaskName $WorkerTaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $WorkerTaskName -Confirm:$false
    Write-Host "      Removed existing task."
}

$WorkerAction = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$StartWorkerBat`""

# Two triggers: at startup AND daily at 08:00 (safety net if worker crashes)
$WorkerTriggers = @(
    $(New-ScheduledTaskTrigger -AtStartup),
    $(New-ScheduledTaskTrigger -Daily -At "08:00")
)

Register-ScheduledTask `
    -TaskName    $WorkerTaskName `
    -Action      $WorkerAction `
    -Trigger     $WorkerTriggers `
    -Settings    $Settings `
    -Principal   $Principal `
    -Description "Starts and maintains the Prefect local worker for the IMF pipeline. Restarts daily at 08:00 as a safety net." `
    | Out-Null

Write-Host "      Registered: starts on boot + restarts daily at 08:00"
Write-Host ""

# ---------------------------------------------------------------------------
# Task 2: Trigger Flow 1 on the 17th of each month at 17:10 (Europe/Zurich)
#
# Only Flow 1 is scheduled here. Flow 1 programmatically chains to
# Flow 2 on success, which chains to Flow 3 — all running locally.
# ---------------------------------------------------------------------------
$PipelineTaskName = "Prefect-IMF-Pipeline"
Write-Host "[2/2] Registering pipeline task: $PipelineTaskName"

if (Get-ScheduledTask -TaskName $PipelineTaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $PipelineTaskName -Confirm:$false
    Write-Host "      Removed existing task."
}

$PipelineAction = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$AcquisitionBat`""

# Monthly on the 17th at 17:10
# Note: Windows Task Scheduler uses the machine's local time.
# Ensure the machine clock matches Europe/Zurich or adjust the time below.
$PipelineTrigger = New-ScheduledTaskTrigger -Monthly -DaysOfMonth 17 -At "17:10"

Register-ScheduledTask `
    -TaskName    $PipelineTaskName `
    -Action      $PipelineAction `
    -Trigger     $PipelineTrigger `
    -Settings    $Settings `
    -Principal   $Principal `
    -Description "Monthly IMF pipeline entry point. Runs on the 17th at 17:10. Flow 1 chains to Flow 2 and Flow 3 on success. All runs reported to Prefect Cloud." `
    | Out-Null

Write-Host "      Registered: monthly on the 17th at 17:10"
Write-Host ""

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
Write-Host "============================================================"
Write-Host "  Setup Complete — 2 tasks registered"
Write-Host "============================================================"
Write-Host ""
Write-Host "  Prefect-IMF-Worker"
Write-Host "    Starts on boot + daily restart at 08:00"
Write-Host "    Must be running for any flow to execute"
Write-Host ""
Write-Host "  Prefect-IMF-Pipeline"
Write-Host "    Fires on the 17th at 17:10 (local machine time)"
Write-Host "    Triggers Flow 1 → Flow 2 → Flow 3 (local chain)"
Write-Host "    All results visible at: https://app.prefect.cloud"
Write-Host ""
Write-Host "  Verify tasks:"
Write-Host "    schtasks /query /tn Prefect-IMF* /fo LIST"
Write-Host ""
Write-Host "  Start worker now (without rebooting):"
Write-Host "    scripts\start_worker.bat"
Write-Host ""
Write-Host "  Test pipeline manually:"
Write-Host "    scripts\run_CurrencyAcquisition.bat"
Write-Host ""
Read-Host "Press Enter to close"
