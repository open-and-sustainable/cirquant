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
#
# Examples:
#   ./fetch_data.sh                  # Fetches data for 2002-2023 (default)
#   ./fetch_data.sh "2020-2023"      # Fetches data for 2020-2023
#   ./fetch_data.sh "2022-2022"      # Fetches data for 2022 only

# Get years parameter with default value
YEARS=${1:-"2002-2024"}

echo "=== CirQuant Raw Data Fetcher ==="
echo "Fetching data for years: $YEARS"
echo "Output will be saved to CirQuant-database/raw/CirQuant_2002-2024.duckdb"
echo

# Run the Julia script
#julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_prodcom_data(\"$YEARS\")" 2>&1 | tee prodcom.log
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_comext_data(\"$YEARS\")" 2>&1 | tee comext.log
#julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_combined_data(\"$YEARS\")" 2>&1 | tee combined_fetch.log

echo
echo "Process completed. Log saved to combined_fetch.log"
