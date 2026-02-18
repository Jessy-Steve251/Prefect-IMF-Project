# 创建带时区的 Task Scheduler 任务

Write-Host "Creating Prefect tasks with Europe/Zurich timezone..."
Write-Host ""

# 任务 1
Write-Host "1. Creating Prefect-CurrencyAcquisition..."
$Trigger1 = New-ScheduledTaskTrigger -Monthly -DaysOfMonth 17 -At "11:00:00"
$Trigger1.TimeZone = 'Europe/Zurich'
$Action1 = New-ScheduledTaskAction -Execute "C:\Users\yli\Desktop\Prefect_Project\run_Prefect-CurrencyAcquisition.bat"
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable
Register-ScheduledTask -TaskName 'Prefect-CurrencyAcquisition' -Trigger $Trigger1 -Action $Action1 -Settings $Settings -Force | Out-Null
Write-Host "   OK - Created at 11:00"

# 任务 2
Write-Host "2. Creating Prefect-PrepareBatch..."
$Trigger2 = New-ScheduledTaskTrigger -Monthly -DaysOfMonth 17 -At "11:30:00"
$Trigger2.TimeZone = 'Europe/Zurich'
$Action2 = New-ScheduledTaskAction -Execute "C:\Users\yli\Desktop\Prefect_Project\run_Prefect-PrepareBatch.bat"
Register-ScheduledTask -TaskName 'Prefect-PrepareBatch' -Trigger $Trigger2 -Action $Action2 -Settings $Settings -Force | Out-Null
Write-Host "   OK - Created at 11:30"

# 任务 3
Write-Host "3. Creating Prefect-ProcessBatch..."
$Trigger3 = New-ScheduledTaskTrigger -Monthly -DaysOfMonth 17 -At "12:00:00"
$Trigger3.TimeZone = 'Europe/Zurich'
$Action3 = New-ScheduledTaskAction -Execute "C:\Users\yli\Desktop\Prefect_Project\run_Prefect-ProcessBatch.bat"
Register-ScheduledTask -TaskName 'Prefect-ProcessBatch' -Trigger $Trigger3 -Action $Action3 -Settings $Settings -Force | Out-Null
Write-Host "   OK - Created at 12:00"

Write-Host ""
Write-Host "Verifying tasks..."
Write-Host ""

Get-ScheduledTask -TaskName 'Prefect-*' | ForEach-Object {
    $task = $_
    Write-Host "$($task.TaskName) - $($task.State)"
}

Write-Host ""
Write-Host "All tasks created successfully!"
