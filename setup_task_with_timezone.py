"""
使用 PowerShell 创建带时区的 Task Scheduler 任务
"""
import subprocess

tasks = [
    {
        "name": "Prefect-CurrencyAcquisition",
        "path": r"C:\Users\yli\Desktop\Prefect_Project\run_Prefect-CurrencyAcquisition.bat",
        "time": "11:00"
    },
    {
        "name": "Prefect-PrepareBatch",
        "path": r"C:\Users\yli\Desktop\Prefect_Project\run_Prefect-PrepareBatch.bat",
        "time": "11:30"
    },
    {
        "name": "Prefect-ProcessBatch",
        "path": r"C:\Users\yli\Desktop\Prefect_Project\run_Prefect-ProcessBatch.bat",
        "time": "12:00"
    }
]

for task in tasks:
    ps_script = f"""
$Trigger = New-ScheduledTaskTrigger -Monthly -DaysOfMonth 17 -At {task['time']}
$Trigger.TimeZone = 'Europe/Zurich'
$Action = New-ScheduledTaskAction -Execute '{task['path']}'
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable
Register-ScheduledTask -TaskName '{task['name']}' -Trigger $Trigger -Action $Action -Settings $Settings -Force
Write-Host "Created: {task['name']} at {task['time']} (Europe/Zurich)"
"""
    
    try:
        result = subprocess.run(
            ["powershell", "-Command", ps_script],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print(result.stdout.strip())
        else:
            print(f"Error creating {task['name']}: {result.stderr}")
    except Exception as e:
        print(f"Exception for {task['name']}: {str(e)}")

print("\nVerifying tasks...")
result = subprocess.run(
    ["schtasks", "/query"],
    capture_output=True,
    text=True
)

for line in result.stdout.split('\n'):
    if 'Prefect-' in line:
        print(line)
