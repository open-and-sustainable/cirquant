#!/bin/bash

# fetch_data.sh
# Script to fetch PRODCOM data using the external ProdcomAPI package
# and store it in the raw DuckDB database

# Usage:
#   ./fetch_data.sh [YEARS]
#
# Examples:
#   ./fetch_data.sh                  # Fetches data for 1995-2023 (default)
#   ./fetch_data.sh "2020-2023"      # Fetches data for 2020-2023
#   ./fetch_data.sh "2022-2022"      # Fetches data for 2022 only

# Get years parameter with default value
YEARS=${1:-"1995-2023"}

echo "=== CirQuant PRODCOM Data Fetcher ==="
echo "Fetching PRODCOM data for years: $YEARS"
echo "Using external ProdcomAPI package"
echo "Output will be saved to CirQuant-database/raw/CirQuant_1995-2023.duckdb"
echo

# Run the Julia script and call the fetch_prodcom_data function with years parameter
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_prodcom_data(\"$YEARS\")" 2>&1 | tee prodcom.log

echo
echo "Process completed. Log saved to prodcom.log"
