# Simple script to check raw production data directly
# This avoids module dependencies and directly queries the database

using DataFrames
using DuckDB, DBInterface

# Configuration
const YEAR = 2009
const RAW_DB_PATH = "../CirQuant-database/raw/CirQuant_1995-2023.duckdb"

println("="^80)
println("CHECKING RAW PRODUCTION DATA FOR YEAR $YEAR")
println("="^80)

# Connect to database
db_conn = DuckDB.DB(RAW_DB_PATH)
con = DBInterface.connect(db_conn)

# 1. Check if the table exists
println("\n1. CHECKING TABLE EXISTENCE")
println("-"^40)
table_name = "prodcom_ds_056120_$YEAR"
exists_query = """
SELECT COUNT(*) as exists
FROM information_schema.tables
WHERE table_name = '$table_name'
"""
exists_df = DBInterface.execute(con, exists_query) |> DataFrame
println("Table $table_name exists: ", exists_df.exists[1] > 0)

if exists_df.exists[1] == 0
    println("ERROR: Table does not exist!")
    DBInterface.close!(con)
    DBInterface.close!(db_conn)
    exit(1)
end

# 2. Get table structure
println("\n2. TABLE STRUCTURE")
println("-"^40)
structure_query = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = '$table_name'
ORDER BY ordinal_position
"""
structure_df = DBInterface.execute(con, structure_query) |> DataFrame
println(structure_df)

# 3. Check distinct indicators
println("\n3. DISTINCT INDICATORS")
println("-"^40)
indicators_query = """
SELECT indicators, COUNT(*) as count
FROM $table_name
GROUP BY indicators
ORDER BY count DESC
"""
indicators_df = DBInterface.execute(con, indicators_query) |> DataFrame
println(indicators_df)

# 4. Sample records for PRODQNT and PRODVAL
println("\n4. SAMPLE PRODQNT RECORDS")
println("-"^40)
prodqnt_query = """
SELECT *
FROM $table_name
WHERE indicators = 'PRODQNT'
LIMIT 10
"""
prodqnt_df = DBInterface.execute(con, prodqnt_query) |> DataFrame
println("Columns: ", names(prodqnt_df))
println(prodqnt_df)

println("\n5. SAMPLE PRODVAL RECORDS")
println("-"^40)
prodval_query = """
SELECT *
FROM $table_name
WHERE indicators = 'PRODVAL'
LIMIT 10
"""
prodval_df = DBInterface.execute(con, prodval_query) |> DataFrame
println(prodval_df)

# 6. Check what's in the 'value' column for production data
println("\n6. VALUE COLUMN ANALYSIS FOR PRODUCTION INDICATORS")
println("-"^40)
value_analysis_query = """
SELECT
    indicators,
    value,
    COUNT(*) as count
FROM $table_name
WHERE indicators IN ('PRODQNT', 'PRODVAL')
GROUP BY indicators, value
ORDER BY indicators, count DESC
LIMIT 50
"""
value_analysis_df = DBInterface.execute(con, value_analysis_query) |> DataFrame
println(value_analysis_df)

# 7. Check if numeric values might be in a different column
println("\n7. CHECKING FOR NUMERIC COLUMNS")
println("-"^40)
# Get all column names
all_columns = names(prodqnt_df)
println("All columns: ", all_columns)

# Look for columns that might contain numeric values
for col in all_columns
    if col ∉ ["indicators", "prccode", "decl", "value"]  # Skip known non-numeric columns
        sample_query = """
        SELECT
            indicators,
            $col as column_value,
            COUNT(*) as count
        FROM $table_name
        WHERE indicators IN ('PRODQNT', 'PRODVAL')
        AND $col IS NOT NULL
        GROUP BY indicators, $col
        ORDER BY count DESC
        LIMIT 10
        """
        try
            sample_df = DBInterface.execute(con, sample_query) |> DataFrame
            if nrow(sample_df) > 0
                println("\nColumn '$col' sample values:")
                println(sample_df)
            end
        catch e
            # Skip if column doesn't exist or query fails
        end
    end
end

# 8. Check specific product code (batteries)
println("\n8. CHECKING BATTERIES (PRODUCT CODE STARTING WITH '07')")
println("-"^40)
battery_query = """
SELECT *
FROM $table_name
WHERE indicators IN ('PRODQNT', 'PRODVAL')
AND prccode LIKE '07%'
LIMIT 20
"""
battery_df = DBInterface.execute(con, battery_query) |> DataFrame
println("Battery records found: ", nrow(battery_df))
if nrow(battery_df) > 0
    println(battery_df)
end

# 9. Try to find any numeric production values
println("\n9. SEARCHING FOR NUMERIC VALUES")
println("-"^40)
# Check if 'value' column contains numbers
numeric_check_query = """
SELECT
    indicators,
    value,
    TRY_CAST(value AS DOUBLE) as numeric_value,
    COUNT(*) as count
FROM $table_name
WHERE indicators IN ('PRODQNT', 'PRODVAL')
AND TRY_CAST(value AS DOUBLE) IS NOT NULL
GROUP BY indicators, value
ORDER BY numeric_value DESC
LIMIT 20
"""
numeric_df = DBInterface.execute(con, numeric_check_query) |> DataFrame
println("Records with numeric values in 'value' column:")
println(numeric_df)

# 10. Final check - see if there are any other tables we should look at
println("\n10. OTHER PRODCOM TABLES")
println("-"^40)
other_tables_query = """
SELECT table_name
FROM information_schema.tables
WHERE table_name LIKE 'prodcom%$YEAR'
ORDER BY table_name
"""
other_tables_df = DBInterface.execute(con, other_tables_query) |> DataFrame
println("All PRODCOM tables for year $YEAR:")
println(other_tables_df)

DBInterface.close!(con)
DBInterface.close!(db_conn)

println("\n" * "="^80)
println("CHECK COMPLETE")
println("="^80)
println("\nKey findings will help debug why production values are 0.0")
