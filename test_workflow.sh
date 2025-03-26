#!/bin/bash
# Test workflow for historical data import tools using relative paths

echo "=== Testing Historical Data Import Tools ==="
echo ""

# Set working directory to script location
cd "$(dirname "$0")"

# Step 1: Generate helper scripts
echo "Step 1: Generating helper scripts..."
python create_helper_scripts.py
echo ""

# Check if the helper scripts were created
if [ ! -f "prepare_commits.py" ] || [ ! -f "process_commits.py" ] || [ ! -f "import_data.py" ]; then
    echo "ERROR: Helper scripts were not created properly by create_helper_scripts.py"
    echo "Please check the create_helper_scripts.py file for errors"
    echo ""
    echo "=== Test Failed ==="
    exit 1
fi

# Step 2: Prepare a small list of commits for testing
echo "Step 2: Preparing commit list with a small date range..."
python prepare_commits.py --start-date 2024-06-08 --end-date 2024-06-10
if [ $? -ne 0 ]; then
    echo "ERROR: prepare_commits.py failed"
    echo "=== Test Failed ==="
    exit 1
fi
echo ""

# Step 3: Process a limited number of commits
echo "Step 3: Processing a limited number of commits..."
python process_commits.py --max-commits 3
if [ $? -ne 0 ]; then
    echo "ERROR: process_commits.py failed"
    echo "=== Test Failed ==="
    exit 1
fi
echo ""

# Step 4: Import the processed data
echo "Step 4: Importing processed data..."
python import_data.py
if [ $? -ne 0 ]; then
    echo "ERROR: import_data.py failed"
    echo "=== Test Failed ==="
    exit 1
fi
echo ""

# Step 5: Verify results
echo "Step 5: Verifying results..."
echo "Checking for output files:"

if [ -f "domains_rankings.parquet" ]; then
    echo "- domains_rankings.parquet: FOUND"
else
    echo "- domains_rankings.parquet: NOT FOUND"
    MISSING_FILES=1
fi

if [ -f "domains_first_seen.parquet" ]; then
    echo "- domains_first_seen.parquet: FOUND"
else
    echo "- domains_first_seen.parquet: NOT FOUND"
    MISSING_FILES=1
fi

if [ -f "last_processed_date.txt" ]; then
    echo "- last_processed_date.txt: FOUND"
    echo "  Last processed date:"
    cat "last_processed_date.txt"
else
    echo "- last_processed_date.txt: NOT FOUND"
    MISSING_FILES=1
fi

echo ""
if [ "$MISSING_FILES" == "1" ]; then
    echo "WARNING: Some expected output files are missing"
    echo "=== Test Completed with Warnings ==="
else
    echo "=== Test Completed Successfully ==="
fi
echo ""
echo "Note: If any files are missing, check the console output above for errors."
echo "To test resume capability, run this script again."

# In CI environment, don't wait for input
if [ -z "$CI" ]; then
    read -p "Press Enter to continue..."
fi