#!/bin/bash

# fetch_all_prodcom.sh
# Script to fetch all PRODCOM data for both datasets

# Set up logging
LOG_FILE="prodcom_fetch_all.log"
echo "Starting PRODCOM data fetch at $(date)" | tee -a "$LOG_FILE"

# Default year range - adjust as needed
YEARS=${1:-"1995-2023"}

echo "Fetching PRODCOM data for years: $YEARS" | tee -a "$LOG_FILE"
echo "This will fetch both ds-056120 and ds-056121 datasets" | tee -a "$LOG_FILE"

# Check if database exists
DB_PATH="CirQuant-database/raw/CirQuant_1995-2023.duckdb"
if [ -f "$DB_PATH" ]; then
    echo "Database exists at: $DB_PATH" | tee -a "$LOG_FILE"
    echo "Tables will be added to the existing database" | tee -a "$LOG_FILE"
else
    echo "Database will be created at: $DB_PATH" | tee -a "$LOG_FILE"
    mkdir -p "$(dirname "$DB_PATH")"
fi

# Run the Julia script with error handling
echo "Starting data fetch process..." | tee -a "$LOG_FILE"
if julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_prodcom_data(\"$YEARS\")" 2>&1 | tee -a "$LOG_FILE"; then
    echo "Data fetch completed successfully" | tee -a "$LOG_FILE"
else
    echo "Error occurred during data fetch" | tee -a "$LOG_FILE"
    exit 1
fi

# Verify tables in database
echo "Verifying database tables..." | tee -a "$LOG_FILE"
julia --project=. -e 'using DuckDB; db = DuckDB.DB("CirQuant-database/raw/CirQuant_1995-2023.duckdb"); tables = DuckDB.query(db, "SHOW TABLES"); println("Tables in database:"); for row in tables; println(" - $(row.name)"); end; DuckDB.close!(db)' | tee -a "$LOG_FILE"

echo "Process completed at $(date)" | tee -a "$LOG_FILE"
echo "Log saved to $LOG_FILE"