#!/bin/bash

# transform_data.sh
# Script to process PRODCOM and COMEXT raw data
# and store it in the processed DuckDB database

# Usage:
#   ./transform_data.sh [YEARS]
#
# Examples:
#   ./transform_data.sh                  # Transforms data for 1995-2023 (default)
#   ./transform_data.sh "2020-2023"      # Transforms data for 2020-2023
#   ./transform_data.sh "2022-2022"      # Transforms data for 2022 only

# Get years parameter with default value
YEARS=${1:-"1995-2023"}

echo "=== CirQuant Data Processing ==="
echo "Processing data for years: $YEARS"
echo "Output will be saved to CirQuant-database/processed/CirQuant_1995-2023.duckdb"
echo

# Store transformation table for industries/sectors/products
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.write_product_conversion_table()" 2>&1 | tee prod_converison_table.log

# Run the Julia script and call the fetch_prodcom_data function with years parameter
#julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_prodcom_data(\"$YEARS\")" 2>&1 | tee prodcom.log
#julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_comext_data(\"$YEARS\")" 2>&1 | tee comext.log
#julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_combined_data(\"$YEARS\")" 2>&1 | tee combined_fetch.log

echo
echo "Process completed. Log saved to combined_processing.log"
