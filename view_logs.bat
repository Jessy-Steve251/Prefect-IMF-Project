@echo off
echo ================================
echo TASK SCHEDULER LOGS
echo ================================
type logs\task_scheduler.log
echo.
echo ================================
echo RECENT FLOW RUNS
echo ================================
prefect flow-run ls --limit 5
echo.
pause