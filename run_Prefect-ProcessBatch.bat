@echo off
cd /d "C:\Users\Admin\Projects\Prefect_Project-main"
call venv\Scripts\activate
python -m flows.process_batch_flow
if %ERRORLEVEL% EQU 0 (
    echo %date% %time% - Process Batch SUCCESS >> logs\task_scheduler.log
) else (
    echo %date% %time% - Process Batch FAILED (Error %ERRORLEVEL%) >> logs\task_scheduler.log
)
pause