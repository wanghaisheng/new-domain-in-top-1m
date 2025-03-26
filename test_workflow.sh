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

# Step 2: Prepare a small list of commits for testing
echo "Step 2: Preparing commit list with a small date range..."
python prepare_commits.py --start-date 2024-06-08 --end-date 2024-06-10
echo ""

# Step 3: Process a limited number of commits
echo "Step 3: Processing a limited number of commits..."
python process_commits.py --max-commits 3
echo ""

# Step 4: Import the processed data
echo "Step 4: Importing processed data..."
python import_data.py
echo ""

# Step 5: Verify results
echo "Step 5: Verifying results..."
echo "Checking for output files:"

if [ -f "domains_rankings.parquet" ]; then
    echo "- domains_rankings.parquet: FOUND"
else
    echo "- domains_rankings.parquet: NOT FOUND"
fi

if [ -f "domains_first_seen.parquet" ]; then
    echo "- domains_first_seen.parquet: FOUND"
else
    echo "- domains_first_seen.parquet: NOT FOUND"
fi

if [ -f "last_processed_date.txt" ]; then
    echo "- last_processed_date.txt: FOUND"
    echo "  Last processed date:"
    cat "last_processed_date.txt"
else
    echo "- last_processed_date.txt: NOT FOUND"
fi

echo ""
echo "=== Test Complete ==="
echo ""
echo "Note: If any files are missing, check the console output above for errors."
echo "To test resume capability, run this script again."

# Wait for user input before closing (similar to pause in batch)
read -p "Press Enter to continue..."