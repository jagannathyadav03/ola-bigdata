@echo off
setlocal enabledelayedexpansion

cd C:\Users\yjaga\OneDrive\Desktop\uber_trip_analysis

REM Create data folder if it doesn't exist
mkdir streamlit_app\data 2>nul

REM Copy and rename each dataset
echo Copying and renaming CSV files...

copy "output_local\query_results\business_summary\part-*.csv" "streamlit_app\data\business_summary.csv" /Y >nul
copy "output_local\query_results\city_revenue_analysis\part-*.csv" "streamlit_app\data\city_revenue_analysis.csv" /Y >nul
copy "output_local\query_results\distance_category_analysis\part-*.csv" "streamlit_app\data\distance_category_analysis.csv" /Y >nul
copy "output_local\query_results\fare_per_km_analysis\part-*.csv" "streamlit_app\data\fare_per_km_analysis.csv" /Y >nul
copy "output_local\query_results\monthly_trends\part-*.csv" "streamlit_app\data\monthly_trends.csv" /Y >nul
copy "output_local\query_results\payment_type_analysis\part-*.csv" "streamlit_app\data\payment_type_analysis.csv" /Y >nul
copy "output_local\query_results\peak_hours_analysis\part-*.csv" "streamlit_app\data\peak_hours_analysis.csv" /Y >nul
copy "output_local\query_results\rate_code_analysis\part-*.csv" "streamlit_app\data\rate_code_analysis.csv" /Y >nul
copy "output_local\query_results\time_of_day_analysis\part-*.csv" "streamlit_app\data\time_of_day_analysis.csv" /Y >nul
copy "output_local\query_results\top_drivers\part-*.csv" "streamlit_app\data\top_drivers.csv" /Y >nul
copy "output_local\query_results\vendor_comparison\part-*.csv" "streamlit_app\data\vendor_comparison.csv" /Y >nul

echo.
echo ✅ All files copied and renamed!
echo.
echo Verifying...
dir streamlit_app\data

pause
