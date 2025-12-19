#!/bin/bash

# fetch_test_data.sh
# Quick fetch for a small PRODCOM slice into the test raw database.
# Useful for development while the full raw download runs separately.
#
# Usage:
#   ./fetch_test_data.sh [YEARS]
#     PARALLEL_YEARS=true MAX_PARALLEL_YEARS=2 ./fetch_test_data.sh "2002-2003"
#     RATE_LIMIT_SECONDS=0.6 RATE_LIMIT_JITTER=0.2 ./fetch_test_data.sh
#
# Defaults:
#   YEARS="2002" (smallest snapshot)
#   DB_PATH="CirQuant-database/raw/test.duckdb"

YEARS=${1:-"2002"}
PARALLEL_YEARS=${PARALLEL_YEARS:-true}
MAX_PARALLEL_YEARS=${MAX_PARALLEL_YEARS:-2}
RATE_LIMIT_SECONDS=${RATE_LIMIT_SECONDS:-0.6}
RATE_LIMIT_JITTER=${RATE_LIMIT_JITTER:-0.2}
DB_PATH=${DB_PATH:-"CirQuant-database/raw/test.duckdb"}

echo "=== CirQuant Test Data Fetcher ==="
echo "Fetching data for years: $YEARS"
echo "Writing to: $DB_PATH"
echo "Parallel years: $PARALLEL_YEARS (max=$MAX_PARALLEL_YEARS, rate_limit=${RATE_LIMIT_SECONDS}s±${RATE_LIMIT_JITTER})"
echo

julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_prodcom_data(\"$YEARS\"; db_path=\"$DB_PATH\", parallel_years=$PARALLEL_YEARS, max_parallel_years=$MAX_PARALLEL_YEARS, rate_limit_seconds=$RATE_LIMIT_SECONDS, rate_limit_jitter=$RATE_LIMIT_JITTER)" 2>&1 | tee prodcom_test.log

echo
echo "Test fetch completed. Log saved to prodcom_test.log"
