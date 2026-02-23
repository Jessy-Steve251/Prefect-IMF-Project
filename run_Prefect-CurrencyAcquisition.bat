@echo off
cd /d "C:\Users\Admin\Projects\Prefect_Project-main"
call venv\Scripts\activate
python -m flows.currency_acquisition_flow
if %ERRORLEVEL% EQU 0 (
    echo %date% %time% - Currency Acquisition SUCCESS >> logs\task_scheduler.log
) else (
    echo %date% %time% - Currency Acquisition FAILED (Error %ERRORLEVEL%) >> logs\task_scheduler.log
)
pause