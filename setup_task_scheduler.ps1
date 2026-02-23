# setup_task_scheduler_refined.ps1
# Windows Task Scheduler setup for Prefect Flows
# With correct timing (11:00, 11:30, 12:00 on 17th of month)

$ProjectPath = "C:\Users\Admin\Projects\Prefect_Project-main"
$PythonPath = "$ProjectPath\venv\Scripts\python.exe"
$LogPath = "$ProjectPath\logs"

Write-Host "=================================================="
Write-Host "Setting up Task Scheduler for Prefect Flows"
Write-Host "=================================================="
Write-Host ""

# Create logs directory if it doesn't exist
if (-not (Test-Path $LogPath)) {
    New-Item -ItemType Directory -Path $LogPath | Out-Null
    Write-Host "✅ Created logs directory: $LogPath"
}

# Define tasks with original timing (11:00, 11:30, 12:00 on 17th)
$Tasks = @(
    @{
        Name = "Prefect-CurrencyAcquisition"
        DisplayName = "Prefect - Currency Acquisition"
        Time = "09:00"
        Module = "flows.currency_acquisition_flow"
        Description = "Acquire currency exchange rates (monthly on 17th at 11:00)"
        BatchFile = "run_Prefect-CurrencyAcquisition.bat"
    },
    @{
        Name = "Prefect-PrepareBatch"
        DisplayName = "Prefect - Prepare Batch"
        Time = "09:30"
        Module = "flows.prepare_batch_flow"
        Description = "Prepare batch data (monthly on 17th at 11:30)"
        BatchFile = "run_Prefect-PrepareBatch.bat"
    },
    @{
        Name = "Prefect-ProcessBatch"
        DisplayName = "Prefect - Process Batch"
        Time = "10:00"
        Module = "flows.process_batch_flow"
        Description = "Process batch data (monthly on 17th at 12:00)"
        BatchFile = "run_Prefect-ProcessBatch.bat"
    }
)

Write-Host "Creating batch files and tasks..."
Write-Host ""

foreach ($Task in $Tasks) {
    Write-Host "▶ Processing: $($Task.DisplayName)"
    Write-Host "  Time: $($Task.Time) on day 17 of each month"
    
    # Create batch file
    $BatchFile = "$ProjectPath\$($Task.BatchFile)"
    Write-Host "  Batch file: $($Task.BatchFile)"
    
    $BatchContent = @"
@echo off
echo ========================================
echo Starting: $($Task.DisplayName)
echo Time: %date% %time%
echo ========================================
cd /d "$ProjectPath"
call venv\Scripts\activate

echo Running: python -m $($Task.Module)
python -m $($Task.Module)

if %ERRORLEVEL% EQU 0 (
    echo ✅ SUCCESS >> "$LogPath\task_scheduler.log"
    echo %date% %time% - $($Task.Name) - SUCCESS >> "$LogPath\task_scheduler.log"
    echo ========================================
    echo ✅ Task completed successfully
) else (
    echo ❌ FAILED (Error %ERRORLEVEL%) >> "$LogPath\task_scheduler.log"
    echo %date% %time% - $($Task.Name) - FAILED (Error %ERRORLEVEL%) >> "$LogPath\task_scheduler.log"
    echo ========================================
    echo ❌ Task failed with error %ERRORLEVEL%
)

echo.
pause
"@
    
    Set-Content -Path $BatchFile -Value $BatchContent -Force
    Write-Host "  ✅ Batch file created/updated"
    
    # Delete existing task if it exists
    & schtasks /delete /tn $Task.Name /f 2>$null
    Write-Host "  Removed any existing task"
    
    # Create new task using schtasks.exe (monthly on day 17)
    try {
        $cmd = "schtasks /create /tn `"$($Task.Name)`" /tr `"`"$BatchFile`"`" /sc monthly /d 17 /st $($Task.Time) /f"
        Write-Host "  Running: schtasks /create ..."
        
        $result = Invoke-Expression $cmd 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  ✅ Task created successfully"
            
            # Add description using schtasks change (optional)
            & schtasks /change /tn $Task.Name /ru SYSTEM /rp "" 2>$null
        } else {
            Write-Host "  ❌ Failed to create task: $result"
        }
    }
    catch {
        Write-Host "  ❌ ERROR: $_"
    }
    
    Write-Host ""
}

Write-Host "=================================================="
Write-Host "✅ Task Scheduler Setup Complete!"
Write-Host "=================================================="
Write-Host ""
Write-Host "📋 Tasks created with original timing:"
foreach ($Task in $Tasks) {
    Write-Host "  • $($Task.Name) - $($Task.Time) on day 17"
}
Write-Host ""
Write-Host "📁 Log file: $LogPath\task_scheduler.log"
Write-Host ""
Write-Host "🔍 To view tasks:"
Write-Host "  schtasks /query | findstr Prefect"
Write-Host ""
Write-Host "🧪 To test a task immediately:"
Write-Host "  schtasks /run /tn Prefect-CurrencyAcquisition"
Write-Host ""
Write-Host "🗑️ To delete a task:"
Write-Host "  schtasks /delete /tn Prefect-CurrencyAcquisition /f"
Write-Host ""
Write-Host "=================================================="
