@echo off
REM Test script for historical data import tools

echo === Testing Historical Data Import Tools ===
echo.

REM Step 1: Generate helper scripts
echo Step 1: Generating helper scripts...
python d:\Download\audio-visual\heytcm\new-domain-in-top-1m\create_helper_scripts.py
echo.

REM Step 2: Prepare a small list of commits for testing
echo Step 2: Preparing commit list with a small date range...
python d:\Download\audio-visual\heytcm\new-domain-in-top-1m\prepare_commits.py --start-date 2024-06-08 --end-date 2024-06-10
echo.

REM Step 3: Process a limited number of commits
echo Step 3: Processing a limited number of commits...
python d:\Download\audio-visual\heytcm\new-domain-in-top-1m\process_commits.py --max-commits 3
echo.

REM Step 4: Import the processed data
echo Step 4: Importing processed data...
python d:\Download\audio-visual\heytcm\new-domain-in-top-1m\import_data.py
echo.

REM Step 5: Verify results
echo Step 5: Verifying results...
echo Checking for output files:

if exist "d:\Download\audio-visual\heytcm\new-domain-in-top-1m\domains_rankings.parquet" (
    echo - domains_rankings.parquet: FOUND
) else (
    echo - domains_rankings.parquet: NOT FOUND
)

if exist "d:\Download\audio-visual\heytcm\new-domain-in-top-1m\domains_first_seen.parquet" (
    echo - domains_first_seen.parquet: FOUND
) else (
    echo - domains_first_seen.parquet: NOT FOUND
)

if exist "d:\Download\audio-visual\heytcm\new-domain-in-top-1m\last_processed_date.txt" (
    echo - last_processed_date.txt: FOUND
    echo   Last processed date: 
    type "d:\Download\audio-visual\heytcm\new-domain-in-top-1m\last_processed_date.txt"
) else (
    echo - last_processed_date.txt: NOT FOUND
)

echo.
echo === Test Complete ===
echo.
echo Note: If any files are missing, check the console output above for errors.
echo To test resume capability, run this script again.

pause