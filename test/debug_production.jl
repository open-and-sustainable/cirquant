# Test script to debug production data pipeline
# Run this script to trace through the production data processing

using Pkg
cd(@__DIR__)
cd("..")  # Go to project root
Pkg.activate(".")

using DataFrames
using DuckDB, DBInterface
using Dates

# Include the necessary modules
include("../src/DataTransform/CircularityProcessor.jl")
using .CircularityProcessor

# Configuration
const YEAR = 2009
const RAW_DB_PATH = "data/raw_data.duckdb"
const PROCESSED_DB_PATH = "data/processed_data.duckdb"

println("="^80)
println("DEBUGGING PRODUCTION DATA PIPELINE FOR YEAR $YEAR")
println("="^80)

# Step 1: Inspect raw table structure
println("\n1. INSPECTING RAW TABLES")
println("-"^40)
raw_info = CircularityProcessor.inspect_raw_tables(RAW_DB_PATH, YEAR, show_sample=true)

# Step 2: Run the debug function to trace production pipeline
println("\n2. DEBUGGING PRODUCTION PIPELINE")
println("-"^40)
debug_info = CircularityProcessor.debug_production_pipeline(RAW_DB_PATH, YEAR)

# Step 3: Check specific product codes (e.g., batteries)
println("\n3. CHECKING SPECIFIC PRODUCT: Batteries (code 07)")
println("-"^40)
battery_debug = CircularityProcessor.debug_production_pipeline(RAW_DB_PATH, YEAR, product_code="07")

# Step 4: Manually check the raw data for production indicators
println("\n4. MANUAL CHECK OF RAW DATA")
println("-"^40)

db_conn = DuckDB.DB(RAW_DB_PATH)
con = DBInterface.connect(db_conn)

# Check what's in the indicators column
indicators_query = """
SELECT DISTINCT indicators, COUNT(*) as count
FROM prodcom_ds_056120_$YEAR
GROUP BY indicators
ORDER BY count DESC
"""
println("Distinct indicators:")
indicators_df = DBInterface.execute(con, indicators_query) |> DataFrame
println(indicators_df)

# Check the structure of PRODQNT and PRODVAL records
structure_query = """
SELECT
    indicators,
    prccode,
    decl as geo,
    value,
    COUNT(*) as count
FROM prodcom_ds_056120_$YEAR
WHERE indicators IN ('PRODQNT', 'PRODVAL')
AND prccode = '07'
GROUP BY indicators, prccode, decl, value
ORDER BY indicators, count DESC
LIMIT 20
"""
println("\nStructure of production data for batteries:")
structure_df = DBInterface.execute(con, structure_query) |> DataFrame
println(structure_df)

# Check if value column contains actual numeric data or unit codes
value_analysis_query = """
SELECT
    indicators,
    value,
    COUNT(*) as count,
    MIN(LENGTH(value)) as min_len,
    MAX(LENGTH(value)) as max_len
FROM prodcom_ds_056120_$YEAR
WHERE indicators IN ('PRODQNT', 'PRODVAL')
GROUP BY indicators, value
ORDER BY indicators, count DESC
LIMIT 30
"""
println("\nValue column analysis:")
value_df = DBInterface.execute(con, value_analysis_query) |> DataFrame
println(value_df)

# Check if there might be another column with actual values
columns_query = """
DESCRIBE prodcom_ds_056120_$YEAR
"""
println("\nTable structure:")
columns_df = DBInterface.execute(con, columns_query) |> DataFrame
println(columns_df)

# Sample some complete records
sample_query = """
SELECT *
FROM prodcom_ds_056120_$YEAR
WHERE indicators IN ('PRODQNT', 'PRODVAL')
AND prccode = '07'
LIMIT 10
"""
println("\nSample complete records for batteries:")
sample_df = DBInterface.execute(con, sample_query) |> DataFrame
println(sample_df)

DBInterface.close!(con)
DBInterface.close!(db_conn)

# Step 5: Test the full processing pipeline with debug output
println("\n5. RUNNING FULL PROCESSING PIPELINE WITH DEBUG OUTPUT")
println("-"^40)

# First ensure the circularity table exists
CircularityProcessor.create_circularity_table(YEAR, db_path=PROCESSED_DB_PATH, replace=true)

# Run the processing with our debug additions
results = CircularityProcessor.process_year_data(
    YEAR,
    raw_db_path=RAW_DB_PATH,
    processed_db_path=PROCESSED_DB_PATH
)

println("\nProcessing results:")
println(results)

# Step 6: Check the final processed table
println("\n6. CHECKING FINAL PROCESSED TABLE")
println("-"^40)

db_conn = DuckDB.DB(PROCESSED_DB_PATH)
con = DBInterface.connect(db_conn)

final_query = """
SELECT
    product_code,
    product_name,
    geo,
    production_volume_tonnes,
    production_value_eur,
    COUNT(*) as count
FROM circularity_$YEAR
WHERE product_code = '07'
GROUP BY product_code, product_name, geo, production_volume_tonnes, production_value_eur
ORDER BY production_volume_tonnes DESC NULLS LAST
LIMIT 20
"""
println("Final circularity table for batteries:")
final_df = DBInterface.execute(con, final_query) |> DataFrame
println(final_df)

# Check if ANY non-zero production values exist
nonzero_query = """
SELECT
    COUNT(*) as total_rows,
    COUNT(CASE WHEN production_volume_tonnes > 0 THEN 1 END) as nonzero_volume,
    COUNT(CASE WHEN production_value_eur > 0 THEN 1 END) as nonzero_value
FROM circularity_$YEAR
"""
println("\nNon-zero production counts in final table:")
nonzero_df = DBInterface.execute(con, nonzero_query) |> DataFrame
println(nonzero_df)

DBInterface.close!(con)
DBInterface.close!(db_conn)

println("\n" * "="^80)
println("DEBUG COMPLETE")
println("="^80)
