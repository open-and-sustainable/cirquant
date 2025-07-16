#!/bin/bash

# transform_data.sh
# Script to transform PRODCOM and COMEXT raw data into circularity indicators
# Creates year-specific tables with production, trade, and circularity metrics

# Usage:
#   ./transform_data.sh [YEARS]
#
# Examples:
#   ./transform_data.sh                  # Fetches data for 2002-2023 (default)
#   ./transform_data.sh "2020-2023"      # Fetches data for 2020-2023
#   ./transform_data.sh "2022-2022"      # Fetches data for 2022 only

# Get years parameter with default value
YEARS=${1:-"2002-2024"}

echo "=== CirQuant Raw Data Processor ==="
echo "Processing data for years: $YEARS"
echo "Output will be saved to CirQuant-database/processed/CirQuant_2002-2024.duckdb"
echo

# Run the Julia script
#julia -e 'using Pkg; Pkg.activate("."); include("src/CirQuant.jl"); using .CirQuant; process_data("2002", use_test_mode=true)'
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.process_data(\"$YEARS\")" 2>&1 | tee process.log

echo
echo "Process completed. Log saved to process.log"
