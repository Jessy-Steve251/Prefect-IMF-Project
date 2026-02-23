@echo off
echo ================================
echo RUNNING COMPLETE BATCH PIPELINE
echo ================================
cd /d "C:\Users\Admin\Projects\Prefect_Project-main"
call venv\Scripts\activate

echo 🚀 Starting complete batch pipeline...
prefect deployment run "complete_batch_pipeline/complete-batch-pipeline"

echo.
echo ================================
echo ✅ Pipeline triggered! Check Prefect Cloud UI for status
echo ================================
pause