@echo off
REM Test workflow for historical data import tools using relative paths

echo === Testing Historical Data Import Tools ===
echo.

REM Set working directory to script location
cd /d "%~dp0"

REM Step 1: Generate helper scripts
echo Step 1: Generating helper scripts...
python create_helper_scripts.py
echo.

REM Step 2: Prepare a small list of commits for testing
echo Step 2: Preparing commit list with a small date range...
python prepare_commits.py --start-date 2024-06-08 --end-date 2024-06-10
echo.

REM Step 3: Process a limited number of commits
echo Step 3: Processing a limited number of commits...
python process_commits.py --max-commits 3
echo.

REM Step 4: Import the processed data
echo Step 4: Importing processed data...
python import_data.py
echo.

REM Step 5: Verify results
echo Step 5: Verifying results...
echo Checking for output files:

if exist "domains_rankings.parquet" (
    echo - domains_rankings.parquet: FOUND
) else (
    echo - domains_rankings.parquet: NOT FOUND
)

if exist "domains_first_seen.parquet" (
    echo - domains_first_seen.parquet: FOUND
) else (
    echo - domains_first_seen.parquet: NOT FOUND
)

if exist "last_processed_date.txt" (
    echo - last_processed_date.txt: FOUND
    echo   Last processed date: 
    type "last_processed_date.txt"
) else (
    echo - last_processed_date.txt: NOT FOUND
)

echo.
echo === Test Complete ===
echo.
echo Note: If any files are missing, check the console output above for errors.
echo To test resume capability, run this script again.

pause