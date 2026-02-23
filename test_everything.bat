@echo off
echo ========================================
echo    COMPLETE SYSTEM TEST (FIXED)
echo ========================================
echo Start time: %time%
echo.
cd /d "C:\Users\Admin\Projects\Prefect_Project-main"
call venv\Scripts\activate

echo Step 1: Testing Currency Acquisition
echo ----------------------------------------
prefect deployment run "currency_acquisition_flow/currency-acquisition"
echo.
timeout /t 10

echo Step 2: Testing Complete Batch Pipeline (Prepare + Process)
echo ----------------------------------------
prefect deployment run "complete_batch_pipeline/complete-batch-pipeline"
echo.
timeout /t 10

echo Step 3: Testing Historical Data Display
echo ----------------------------------------
python -m flows.show_historical_data_fixed
echo.

echo ========================================
echo ✅ ALL TESTS TRIGGERED!
echo ========================================
echo End time: %time%
echo.
echo Check results in Prefect Cloud:
echo https://app.prefect.cloud
echo.
pause