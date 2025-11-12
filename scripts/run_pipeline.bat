@echo off
REM =============================================================================
REM run_pipeline.bat
REM Windows Batch Script for Chh-OLA Big Data Analysis Pipeline
REM =============================================================================

echo ==========================================================================
echo 🚀 CHH-OLA BIG DATA ANALYSIS PIPELINE (Windows)
echo ==========================================================================
echo.

REM Step 1: Check if Docker is running
echo [1/7] Checking Docker...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not running. Please start Docker Desktop and try again.
    pause
    exit /b 1
)
echo ✅ Docker is running
echo.

REM Step 2: Navigate to docker directory
cd docker

REM Step 3: Build Docker image
echo [2/7] Building Docker image...
docker-compose build
if %errorlevel% neq 0 (
    echo ❌ Failed to build Docker image
    pause
    exit /b 1
)
echo ✅ Docker image built successfully
echo.

REM Step 4: Start containers
echo [3/7] Starting containers...
docker-compose up -d
if %errorlevel% neq 0 (
    echo ❌ Failed to start containers
    pause
    exit /b 1
)
echo ✅ Containers started
echo.

REM Wait for Spark to initialize
echo Waiting 15 seconds for Spark to initialize...
timeout /t 15 /nobreak >nul
echo.

REM Step 5: Generate sample data
echo [4/7] Generating sample Chh-OLA trip data...
docker exec chh-ola-spark python /app/scripts/generate_data.py
if %errorlevel% neq 0 (
    echo ❌ Failed to generate data
    pause
    exit /b 1
)
echo ✅ Sample data generated (10,000 records)
echo.

REM Step 6: Run ETL pipeline
echo [5/7] Running ETL pipeline (CSV to Parquet)...
echo This will demonstrate:
echo   • Distributed data processing
echo   • Data cleaning and transformation
echo   • Partitioning and caching
echo   • Parquet columnar storage
echo.
docker exec chh-ola-spark python /app/scripts/etl_process.py
if %errorlevel% neq 0 (
    echo ❌ ETL pipeline failed
    pause
    exit /b 1
)
echo ✅ ETL pipeline completed
echo.

REM Step 7: Execute analytical queries
echo [6/7] Executing analytical queries...
echo This will demonstrate:
echo   • Spark SQL analytics
echo   • Complex aggregations
echo   • Query optimization
echo.
docker exec chh-ola-spark python /app/scripts/spark_queries.py
if %errorlevel% neq 0 (
    echo ❌ Query execution failed
    pause
    exit /b 1
)
echo ✅ Analytical queries completed
echo.

REM Step 8: Copy results to local machine
echo [7/7] Copying results to local machine...
cd ..
if not exist "output_local" mkdir output_local
docker cp chh-ola-spark:/app/output/query_results output_local/
docker cp chh-ola-spark:/app/output/parquet output_local/

if %errorlevel% neq 0 (
    echo ❌ Failed to copy results
    pause
    exit /b 1
)
echo ✅ Results copied to .\output_local\
echo.

REM Summary
echo ==========================================================================
echo 🎉 PIPELINE EXECUTION COMPLETE!
echo ==========================================================================
echo.
echo 📊 Generated Outputs:
echo    1. Parquet files: .\output_local\parquet\
echo    2. Query results: .\output_local\query_results\
echo.
echo 📈 Query Result Files:
echo    • city_revenue_analysis.csv
echo    • peak_hours_analysis.csv
echo    • fare_per_km_analysis.csv
echo    • payment_type_analysis.csv
echo    • distance_category_analysis.csv
echo    • top_drivers.csv
echo    • time_of_day_analysis.csv
echo    • rate_code_analysis.csv
echo    • monthly_trends.csv
echo    • vendor_comparison.csv
echo    • business_summary.csv
echo.
echo 🌐 Access Points:
echo    • Jupyter Notebook: http://localhost:8888
echo    • Spark UI: http://localhost:4040
echo.
echo 📝 Next Steps:
echo    1. Import CSV files from .\output_local\query_results\ into Power BI
echo    2. Create dashboards using the provided template
echo    3. Review the project report in docs\final-report.md
echo    4. Prepare for viva using docs\bigdata-concepts.md
echo.
echo 🛑 To stop containers:
echo    cd docker
echo    docker-compose down
echo.
echo 🔄 To restart pipeline:
echo    run_pipeline.bat
echo.
echo ==========================================================================
echo ✅ All Big Data concepts successfully demonstrated!
echo ==========================================================================
echo.

pause
