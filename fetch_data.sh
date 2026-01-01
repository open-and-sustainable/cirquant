#!/bin/bash

# fetch_data.sh
# Script to fetch PRODCOM and COMEXT raw data
# and store it in the raw DuckDB database
#
# Requirements:
# - ProdcomAPI v0.0.3 or higher (for filtered queries and large dataset handling)
# - ComextAPI package

# Usage:
#   ./fetch_data.sh [YEARS]
#     PARALLEL_YEARS=true ./fetch_data.sh "2020-2023"       # enable parallel per-year fetch
#     PARALLEL_YEARS=true MAX_PARALLEL_YEARS=3 ./fetch_data.sh "2020-2023"
#     RATE_LIMIT_SECONDS=0.6 RATE_LIMIT_JITTER=0.2 ./fetch_data.sh
#
# Examples:
#   ./fetch_data.sh                  # Fetches data for 2010-2024 (default)
#   ./fetch_data.sh "2020-2023"      # Fetches data for 2020-2023
#   ./fetch_data.sh "2022-2022"      # Fetches data for 2022 only

# Get years parameter with default value
YEARS=${1:-"2010-2024"}
PARALLEL_YEARS=${PARALLEL_YEARS:-false}
MAX_PARALLEL_YEARS=${MAX_PARALLEL_YEARS:-2}
RATE_LIMIT_SECONDS=${RATE_LIMIT_SECONDS:-0.6}
RATE_LIMIT_JITTER=${RATE_LIMIT_JITTER:-0.2}

echo "=== CirQuant Raw Data Fetcher ==="
echo "Fetching data for years: $YEARS"
echo "Output will be saved to CirQuant-database/raw/CirQuant_2010-2024.duckdb"
echo "Parallel years: $PARALLEL_YEARS (max=$MAX_PARALLEL_YEARS, rate_limit=${RATE_LIMIT_SECONDS}s±${RATE_LIMIT_JITTER})"
echo

# Run the Julia script
#julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_prodcom_data(\"$YEARS\"; parallel_years=$PARALLEL_YEARS, max_parallel_years=$MAX_PARALLEL_YEARS, rate_limit_seconds=$RATE_LIMIT_SECONDS, rate_limit_jitter=$RATE_LIMIT_JITTER)" 2>&1 | tee prodcom.log
#julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_comext_data(\"$YEARS\")" 2>&1 | tee comext.log
#julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_material_composition_data(\"$YEARS\")" 2>&1 | tee material_composition.log
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_combined_data(\"$YEARS\")" 2>&1 | tee combined_fetch.log

echo
echo "Process completed. Log saved to combined_fetch.log"
