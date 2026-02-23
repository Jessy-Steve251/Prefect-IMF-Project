@echo off
echo ========================================
echo EXPORT COMPLETE DATASET FOR ANALYSIS
echo ========================================
cd /d "C:\Users\Admin\Projects\Prefect_Project-main"
call venv\Scripts\activate

echo.
echo Step 1: Loading and combining all data...
python quick_export.py

echo.
echo ========================================
echo ✅ EXPORT COMPLETE!
echo ========================================
echo File: data\complete_dataset_for_analysis.csv
pause