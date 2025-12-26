#!/bin/bash

# fetch_test_data.sh
# Quick fetch for a small PRODCOM slice into the test raw database.
# Useful for development while the full raw download runs separately.
#
# Usage:
#   ./fetch_test_data.sh [YEARS] [PRODUCT_KEYS]
#     PARALLEL_YEARS=true MAX_PARALLEL_YEARS=2 ./fetch_test_data.sh "2022-2023" "heat_pumps,pv_panels"
#     RATE_LIMIT_SECONDS=0.6 RATE_LIMIT_JITTER=0.2 ./fetch_test_data.sh
#
# Defaults:
#   YEARS="2022-2023" (recent data)
#   PRODUCT_KEYS="heat_pumps,pv_panels" (small representative subset)
#   DB_PATH="CirQuant-database/raw/test.duckdb"

YEARS=${1:-"2022-2023"}
PRODUCT_KEYS=${2:-"heat_pumps,pv_panels"}
PARALLEL_YEARS=${PARALLEL_YEARS:-true}
MAX_PARALLEL_YEARS=${MAX_PARALLEL_YEARS:-2}
RATE_LIMIT_SECONDS=${RATE_LIMIT_SECONDS:-0.2}
RATE_LIMIT_JITTER=${RATE_LIMIT_JITTER:-0.1}
DB_PATH=${DB_PATH:-"CirQuant-database/raw/test.duckdb"}

echo "=== CirQuant Test Data Fetcher ==="
echo "Fetching data for years: $YEARS"
echo "Writing to: $DB_PATH"
echo "Parallel years: $PARALLEL_YEARS (max=$MAX_PARALLEL_YEARS, rate_limit=${RATE_LIMIT_SECONDS}s±${RATE_LIMIT_JITTER})"
echo "Products: $PRODUCT_KEYS"
echo "UMP filters: years=$YEARS products=$PRODUCT_KEYS"
echo

julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; product_keys = filter(!isempty, split(\"$PRODUCT_KEYS\", \",\")); CirQuant.fetch_prodcom_data(\"$YEARS\"; db_path=\"$DB_PATH\", product_keys_filter=product_keys, parallel_years=$PARALLEL_YEARS, max_parallel_years=$MAX_PARALLEL_YEARS, rate_limit_seconds=$RATE_LIMIT_SECONDS, rate_limit_jitter=$RATE_LIMIT_JITTER)" 2>&1 | tee raw_test.log
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; product_keys = filter(!isempty, split(\"$PRODUCT_KEYS\", \",\")); CirQuant.fetch_comext_data(\"$YEARS\"; db_path=\"$DB_PATH\", product_keys_filter=product_keys)" 2>&1 | tee -a raw_test.log
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; product_keys = filter(!isempty, split(\"$PRODUCT_KEYS\", \",\")); CirQuant.fetch_material_recycling_rates_data(\"$YEARS\"; db_path=\"$DB_PATH\")" 2>&1 | tee -a raw_test.log
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; product_keys = filter(!isempty, split(\"$PRODUCT_KEYS\", \",\")); CirQuant.fetch_product_collection_rates_data(\"$YEARS\"; db_path=\"$DB_PATH\", product_keys_filter=product_keys)" 2>&1 | tee -a raw_test.log
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; product_keys = filter(!isempty, split(\"$PRODUCT_KEYS\", \",\")); CirQuant.fetch_ump_weee_data(db_path=\"$DB_PATH\", years_range=\"$YEARS\", product_keys_filter=product_keys)" 2>&1 | tee -a raw_test.log

echo
echo "Test fetch completed. Log saved to raw_test.log"
