#!/bin/bash

# compute_test_product_weights.sh
# Derive data-driven average product weights from the test raw DuckDB
# and write results into the test processed DuckDB.
#
# Usage:
#   ./compute_test_product_weights.sh [YEARS]
#     YEARS="2024" ./compute_test_product_weights.sh
#
# Env overrides:
#   RAW_DB="CirQuant-database/raw/test.duckdb"
#   PROCESSED_DB="CirQuant-database/processed/test_processed.duckdb"

set -euo pipefail

YEARS=${1:-"2024"}
RAW_DB=${RAW_DB:-"CirQuant-database/raw/test.duckdb"}
PROCESSED_DB=${PROCESSED_DB:-"CirQuant-database/processed/test_processed.duckdb"}

echo "=== CirQuant Test Average Weight Calculator ==="
echo "Years: $YEARS"
echo "Raw DB: $RAW_DB"
echo "Processed DB: $PROCESSED_DB"
echo

mkdir -p "$(dirname "$PROCESSED_DB")"

julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.ProductWeightsFetch.fetch_product_weights_data(\"$YEARS\"; db_path=\"$RAW_DB\", processed_db_path=\"$PROCESSED_DB\")" 2>&1 | tee product_weights_test.log

echo
echo "Average weight derivation completed. Log saved to product_weights_test.log"
