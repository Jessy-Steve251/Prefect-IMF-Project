@echo off
cd /d "C:\Users\Admin\Projects\Prefect_Project-main"
call venv\Scripts\activate
python -m flows.show_historical_data_fixed
echo ✅ Historical data displayed in Prefect!
pause