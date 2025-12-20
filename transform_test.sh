#!/bin/bash

# transform_test.sh
# Transform the raw test DuckDB into processed test outputs.
# Uses the test raw DB (CirQuant-database/raw/test.duckdb) and writes to test_processed.duckdb.
#
# Usage:
#   ./transform_test.sh [YEARS]
#
# Example:
#   ./transform_test.sh "2022-2023"

YEARS=${1:-"2022-2023"}
RAW_DB="CirQuant-database/raw/test.duckdb"
PROCESSED_DB="CirQuant-database/processed/test.duckdb"

echo "=== CirQuant Test Data Processor ==="
echo "Processing years: $YEARS"
echo "Raw DB: $RAW_DB"
echo "Processed DB: $PROCESSED_DB"
echo

# Ensure output directory exists
mkdir -p "$(dirname "$PROCESSED_DB")"

# Run processing using test DB paths
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.process_data(\"$YEARS\"; source_db=\"$RAW_DB\", target_db=\"$PROCESSED_DB\")" 2>&1 | tee process_test.log

echo
echo "Test processing completed. Log saved to process_test.log"
