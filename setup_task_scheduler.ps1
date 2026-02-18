# 注册 Windows 任务计划程序，用于定时执行 Flow
# 这样 Flow 可以在本地运行，不需要 Cloud 托管执行

$ProjectPath = "C:\Users\yli\Desktop\Prefect_Project"
$ScriptPath = "$ProjectPath\run_flows_locally.py"
$LogPath = "$ProjectPath\logs"

# 创建日志目录
if (-not (Test-Path $LogPath)) {
    New-Item -ItemType Directory -Path $LogPath | Out-Null
}

Write-Host "=" * 70
Write-Host "Setting up Windows Task Scheduler for Prefect Flows"
Write-Host "=" * 70
Write-Host ""

# 定义三个任务的时间和名称
$Tasks = @(
    @{
        Name = "Prefect-CurrencyAcquisition"
        Time = "09:00"  # 改回原始时间（如需要）
        Script = "$ScriptPath"
        Description = "Acquire currency exchange rates"
    },
    @{
        Name = "Prefect-PrepareBatch"
        Time = "09:30"
        Script = "$ScriptPath"
        Description = "Prepare batch data"
    },
    @{
        Name = "Prefect-ProcessBatch"
        Time = "10:00"
        Script = "$ScriptPath"
        Description = "Process batch data"
    }
)

# 创建任务
foreach ($Task in $Tasks) {
    Write-Host "Creating task: $($Task.Name)"
    Write-Host "  Time: $($Task.Time)"
    Write-Host "  Script: $($Task.Script)"
    Write-Host ""
    
    # 检查任务是否已存在
    $ExistingTask = Get-ScheduledTask -TaskName $Task.Name -ErrorAction SilentlyContinue
    
    if ($ExistingTask) {
        Write-Host "  Task already exists. Skipping..."
        Write-Host ""
        continue
    }
    
    # 创建任务触发器（每天指定时间）
    $Trigger = New-ScheduledTaskTrigger `
        -Daily `
        -At $Task.Time
    
    # 创建任务动作
    $Action = New-ScheduledTaskAction `
        -Execute "python" `
        -Argument $Task.Script `
        -WorkingDirectory $ProjectPath
    
    # 创建任务设置
    $Settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -RunOnlyIfNetworkAvailable `
        -MultipleInstances IgnoreNew
    
    # 注册任务
    Register-ScheduledTask `
        -TaskName $Task.Name `
        -Description $Task.Description `
        -Trigger $Trigger `
        -Action $Action `
        -Settings $Settings `
        -RunLevel Highest `
        -Force
    
    Write-Host "  ✅ Task created successfully"
    Write-Host ""
}

Write-Host "=" * 70
Write-Host "Task Scheduler Setup Complete!"
Write-Host "=" * 70
Write-Host ""
Write-Host "Tasks created:"
foreach ($Task in $Tasks) {
    Write-Host "  - $($Task.Name) at $($Task.Time)"
}
Write-Host ""
Write-Host "Logs will be saved to: $LogPath"
Write-Host ""
Write-Host "To view tasks:"
Write-Host "  Get-ScheduledTask -TaskName 'Prefect-*' | Format-Table TaskName, State"
Write-Host ""
