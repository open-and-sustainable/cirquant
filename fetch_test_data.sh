#!/bin/bash

# fetch_test_data.sh
# Quick combined fetch for a small product/year slice into the test raw database.
# Useful for validating the combined pipeline before running the full download.
#
# Usage:
#   ./fetch_test_data.sh [YEARS] [PRODUCT_KEYS]
#     ./fetch_test_data.sh "2022-2023" "heat_pumps,pv_panels"
#
# Defaults:
#   YEARS="2022-2023" (recent data)
#   PRODUCT_KEYS="heat_pumps,pv_panels" (small representative subset)
#   DB_PATH="CirQuant-database/raw/test.duckdb"

YEARS=${1:-"2022-2023"}
PRODUCT_KEYS=${2:-"heat_pumps,pv_panels"}
DB_PATH=${DB_PATH:-"CirQuant-database/raw/test.duckdb"}

echo "=== CirQuant Test Data Fetcher ==="
echo "Fetching data for years: $YEARS"
echo "Writing to: $DB_PATH"
echo "Combined fetch (test scope)"
echo "Products: $PRODUCT_KEYS"
echo "UMP filters: years=$YEARS products=$PRODUCT_KEYS"
echo

julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; product_keys = filter(!isempty, split(\"$PRODUCT_KEYS\", \",\")); CirQuant.fetch_combined_data(\"$YEARS\"; db_path=\"$DB_PATH\", product_keys_filter=product_keys)" 2>&1 | tee raw_test.log

echo
echo "Test fetch completed. Log saved to raw_test.log"
