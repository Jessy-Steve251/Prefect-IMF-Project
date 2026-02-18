# 使用 schtasks.exe (Windows 内置工具) 创建任务
# 这不需要管理员权限的 PowerShell 对象

$ProjectPath = "C:\Users\yli\Desktop\Prefect_Project"
$ScriptPath = "$ProjectPath\run_flows_locally.py"

Write-Host "=================================================="
Write-Host "Setting up Task Scheduler using schtasks.exe"
Write-Host "=================================================="
Write-Host ""

# 定义任务
$Tasks = @(
    @{
        Name = "Prefect-CurrencyAcquisition"
        Time = "09:00"
        Description = "Acquire currency exchange rates"
    },
    @{
        Name = "Prefect-PrepareBatch"
        Time = "09:30"
        Description = "Prepare batch data"
    },
    @{
        Name = "Prefect-ProcessBatch"
        Time = "10:00"
        Description = "Process batch data"
    }
)

foreach ($Task in $Tasks) {
    Write-Host "Creating task: $($Task.Name)"
    Write-Host "  Time: $($Task.Time)"
    Write-Host "  Description: $($Task.Description)"
    Write-Host ""
    
    # 使用 schtasks.exe 创建任务
    # 格式: schtasks /create /tn "任务名" /tr "命令" /sc daily /st HH:MM
    
    $Command = "python `"$ScriptPath`""
    $BatchFile = "$ProjectPath\run_$($Task.Name).bat"
    
    # 创建 .bat 文件作为中间层
    $BatchContent = @"
@echo off
cd /d "$ProjectPath"
python "$ScriptPath"
"@
    
    Set-Content -Path $BatchFile -Value $BatchContent
    Write-Host "  Created batch file: $BatchFile"
    
    # 使用 schtasks.exe 创建任务
    try {
        & schtasks /create /tn $Task.Name /tr "`"$BatchFile`"" /sc daily /st $Task.Time /f
        Write-Host "  OK Task created successfully"
        Write-Host ""
    }
    catch {
        Write-Host "  ERROR: $_"
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
