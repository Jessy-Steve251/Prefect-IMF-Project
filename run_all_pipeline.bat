@echo off
echo ================================
echo STARTING COMPLETE PIPELINE
echo ================================
cd /d "C:\Users\Admin\Projects\Prefect_Project-main"
call venv\Scripts\activate

echo Step 1: Currency Acquisition
python -m flows.currency_acquisition_flow
echo.

echo Step 2: Complete Batch Pipeline (Prepare + Process)
prefect deployment run "complete_batch_pipeline/complete-batch-pipeline"
echo.

echo ================================
echo ✅ PIPELINE COMPLETE
echo ================================
pause