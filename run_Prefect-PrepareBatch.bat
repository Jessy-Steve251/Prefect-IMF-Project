@echo off
cd /d "C:\Users\Admin\Projects\Prefect_Project-main"
call venv\Scripts\activate
python -m flows.prepare_batch_flow
if %ERRORLEVEL% EQU 0 (
    echo %date% %time% - Prepare Batch SUCCESS >> logs\task_scheduler.log
) else (
    echo %date% %time% - Prepare Batch FAILED (Error %ERRORLEVEL%) >> logs\task_scheduler.log
)
pause