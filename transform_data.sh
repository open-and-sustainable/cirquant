#!/bin/bash

# transform_data.sh
# Script to transform PRODCOM and COMEXT raw data into circularity indicators
# Creates year-specific tables with production, trade, and circularity metrics

# Usage:
#   ./transform_data.sh [YEARS] [REPLACE]
#
# Parameters:
#   YEARS   - Year range (e.g., "2020-2023") or single year (e.g., "2022")
#   REPLACE - Optional: "replace" to overwrite existing tables
#
# Examples:
#   ./transform_data.sh                  # Transforms data for 1995-2023 (default)
#   ./transform_data.sh "2020-2023"      # Transforms data for 2020-2023
#   ./transform_data.sh "2022"           # Transforms data for 2022 only
#   ./transform_data.sh "2009" replace   # Transforms 2009, replacing existing table

# Get parameters
YEARS=${1:-"1995-2023"}
REPLACE_FLAG=${2:-""}

# Check if replace flag is set
if [ "$REPLACE_FLAG" = "replace" ]; then
    REPLACE_OPTION="true"
else
    REPLACE_OPTION="false"
fi

echo "=== CirQuant Data Transformation ==="
echo "Transforming data for years: $YEARS"
echo "Replace existing tables: $REPLACE_OPTION"
echo "Output: CirQuant-database/processed/CirQuant_1995-2023.duckdb"
echo

# Step 1: Install PRQL extension if needed
echo "Step 1: Installing PRQL extension..."
julia --project=. -e "
push!(LOAD_PATH, \"src\")
using CirQuant
success = CirQuant.CircularityProcessor.ensure_prql_installed()
println(success ? \"✓ PRQL extension ready\" : \"✗ Failed to install PRQL extension\")
" 2>&1 | tee transformation_$(date +%Y%m%d_%H%M%S).log

# Step 2: Ensure product conversion table exists
echo -e "\nStep 2: Creating/updating product conversion table..."
julia --project=. -e "
push!(LOAD_PATH, \"src\")
using CirQuant
success = CirQuant.write_product_conversion_table()
println(success ? \"✓ Product conversion table ready\" : \"✗ Failed to create product conversion table\")
" 2>&1 | tee -a transformation_$(date +%Y%m%d_%H%M%S).log

# Step 3: Parse year range and process each year
echo -e "\nStep 3: Processing circularity data..."

# Extract start and end years from the range
if [[ $YEARS =~ ^([0-9]{4})-([0-9]{4})$ ]]; then
    START_YEAR=${BASH_REMATCH[1]}
    END_YEAR=${BASH_REMATCH[2]}
elif [[ $YEARS =~ ^[0-9]{4}$ ]]; then
    START_YEAR=$YEARS
    END_YEAR=$YEARS
else
    echo "Error: Invalid year format. Use YYYY or YYYY-YYYY"
    exit 1
fi

# Process each year
julia --project=. -e "
push!(LOAD_PATH, \"src\")
using CirQuant
using CirQuant.CircularityProcessor
using DataFrames

start_year = $START_YEAR
end_year = $END_YEAR
replace_flag = $REPLACE_OPTION

println(\"\\nProcessing years \$start_year to \$end_year...\")

# Define PRQL files for data extraction
prql_files = Dict(
    \"production\" => joinpath(\"src\", \"DataTransform\", \"production_data.prql\"),
    \"trade\" => joinpath(\"src\", \"DataTransform\", \"trade_data.prql\")
)

# Process each year
global successful = 0
global failed = 0

for year in start_year:end_year
    println(\"\\n\" * \"=\"^60)
    println(\"Processing year \$year\")
    println(\"=\"^60)

    try
        # Create circularity table
        table_created = CirQuant.create_circularity_table(year;
            db_path=CirQuant.DB_PATH_PROCESSED,
            replace=replace_flag)

        if table_created
            println(\"✓ Created circularity table for year \$year\")

            # Process the data using PRQL queries
            results = CircularityProcessor.process_year_data(
                year,
                raw_db_path=CirQuant.DB_PATH_RAW,
                processed_db_path=CirQuant.DB_PATH_PROCESSED,
                prql_files=prql_files,
                replace=replace_flag
            )

            if results[:success]
                println(\"✓ Successfully processed data for year \$year\")
                println(\"  - Rows processed: \$(results[:rows_processed])\")
                global successful += 1
            else
                println(\"✗ Failed to process data for year \$year\")
                if !isempty(results[:errors])
                    println(\"  Errors: \$(join(results[:errors], \"; \"))\")
                end
                global failed += 1
            end
        else
            println(\"✗ Failed to create table for year \$year\")
            global failed += 1
        end
    catch e
        println(\"✗ Error processing year \$year: \$e\")
        global failed += 1
    end
end

println(\"\\n\" * \"=\"^60)
println(\"Transformation Summary\")
println(\"=\"^60)
println(\"Total years processed: \$(successful + failed)\")
println(\"Successful: \$successful\")
println(\"Failed: \$failed\")
" 2>&1 | tee -a transformation_$(date +%Y%m%d_%H%M%S).log

echo
echo "Transformation completed. Check the log file for details."
