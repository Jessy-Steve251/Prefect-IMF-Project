# Using schtasks.exe (built-in Windows tool)
# Updated for Admin's system

$ProjectPath = "C:\Users\Admin\Projects\Prefect_Project-main"
$PythonPath = "C:\Users\Admin\Projects\Prefect_Project-main\venv\Scripts\python.exe"

Write-Host "=================================================="
Write-Host "Setting up Task Scheduler using schtasks.exe"
Write-Host "=================================================="
Write-Host ""

# Define tasks
$Tasks = @(
    @{
        Name = "Prefect-CurrencyAcquisition"
        Time = "09:00"
        Module = "currency_acquisition_flow"
        Description = "Acquire currency exchange rates"
    },
    @{
        Name = "Prefect-PrepareBatch"
        Time = "09:30"
        Module = "prepare_batch_flow"
        Description = "Prepare batch data"
    },
    @{
        Name = "Prefect-ProcessBatch"
        Time = "10:00"
        Module = "process_batch_flow"
        Description = "Process batch data"
    }
)

foreach ($Task in $Tasks) {
    Write-Host "Creating task: $($Task.Name)"
    Write-Host "  Time: $($Task.Time) on day 17"
    Write-Host "  Description: $($Task.Description)"
    Write-Host ""

    # Create batch file
    $BatchFile = "$ProjectPath\run_$($Task.Name).bat"
    
    # Create batch content with venv activation
    $BatchContent = @"
@echo off
cd /d "$ProjectPath"
call venv\Scripts\activate
python -m flows.$($Task.Module)
if %ERRORLEVEL% EQU 0 (
    echo %date% %time% - $($Task.Name) SUCCESS >> "$ProjectPath\logs\task_scheduler.log"
) else (
    echo %date% %time% - $($Task.Name) FAILED >> "$ProjectPath\logs\task_scheduler.log"
)
"@

    Set-Content -Path $BatchFile -Value $BatchContent
    Write-Host "  Created batch file: $BatchFile"

    # Delete existing task if it exists
    & schtasks /delete /tn $Task.Name /f 2>$null

    # Create new task using schtasks.exe (monthly on day 17)
    try {
        & schtasks /create /tn $Task.Name /tr "`"$BatchFile`"" /sc monthly /d 17 /st $Task.Time /f
        Write-Host "  ✅ Task created successfully"
        Write-Host ""
    }
    catch {
        Write-Host "  ❌ ERROR: $_"
        Write-Host ""
    }
}

Write-Host "=================================================="
Write-Host "Task Scheduler Setup Complete!"
Write-Host "=================================================="
Write-Host ""
Write-Host "To view created tasks:"
Write-Host "  schtasks /query | findstr Prefect"
Write-Host ""
Write-Host "To delete a task:"
Write-Host "  schtasks /delete /tn Prefect-CurrencyAcquisition /f"
Write-Host ""