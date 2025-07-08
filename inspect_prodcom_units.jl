#!/usr/bin/env julia

# Script to inspect PRODCOM data structure and understand unit handling
# This helps understand how to properly convert units to tonnes

using DataFrames
using DuckDB, DBInterface

# Configuration
const RAW_DB_PATH = "../CirQuant-database/raw/CirQuant_1995-2023.duckdb"
const TEST_YEAR = 2020  # Year to inspect

println("="^80)
println("PRODCOM DATA STRUCTURE INSPECTION")
println("Year: $TEST_YEAR")
println("Database: $RAW_DB_PATH")
println("="^80)

# Connect to database
db_conn = DuckDB.DB(RAW_DB_PATH)
con = DBInterface.connect(db_conn)

try
    table_name = "prodcom_ds_056120_$TEST_YEAR"

    # 1. Check if table exists
    println("\n1. TABLE EXISTENCE CHECK")
    println("-"^40)
    exists_query = """
    SELECT COUNT(*) as exists
    FROM information_schema.tables
    WHERE table_name = '$table_name'
    """
    exists_df = DBInterface.execute(con, exists_query) |> DataFrame

    if exists_df.exists[1] == 0
        println("ERROR: Table $table_name does not exist!")
        exit(1)
    end

    println("✓ Table $table_name exists")

    # 2. Get table structure
    println("\n2. TABLE STRUCTURE")
    println("-"^40)
    structure_query = """
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns
    WHERE table_name = '$table_name'
    ORDER BY ordinal_position
    """
    structure_df = DBInterface.execute(con, structure_query) |> DataFrame
    println(structure_df)

    # 3. Sample raw data
    println("\n3. SAMPLE RAW DATA (first 10 rows)")
    println("-"^40)
    sample_query = """
    SELECT *
    FROM $table_name
    LIMIT 10
    """
    sample_df = DBInterface.execute(con, sample_query) |> DataFrame
    println("Columns: ", names(sample_df))
    println(sample_df)

    # 4. Check distinct indicators
    println("\n4. DISTINCT INDICATORS AND THEIR COUNTS")
    println("-"^40)
    indicators_query = """
    SELECT
        indicators,
        COUNT(*) as record_count
    FROM $table_name
    GROUP BY indicators
    ORDER BY record_count DESC
    """
    indicators_df = DBInterface.execute(con, indicators_query) |> DataFrame
    println(indicators_df)

    # 5. Analyze PRODQNT records specifically
    println("\n5. PRODUCTION QUANTITY (PRODQNT) ANALYSIS")
    println("-"^40)

    # Sample PRODQNT records
    prodqnt_sample_query = """
    SELECT *
    FROM $table_name
    WHERE indicators = 'PRODQNT'
    LIMIT 20
    """
    prodqnt_sample_df = DBInterface.execute(con, prodqnt_sample_query) |> DataFrame
    println("\nSample PRODQNT records:")
    println(prodqnt_sample_df)

    # 6. Analyze qntunit field
    println("\n6. QUANTITY UNITS (qntunit) ANALYSIS")
    println("-"^40)

    # Get all unique units with counts
    units_query = """
    SELECT
        qntunit,
        COUNT(*) as count,
        COUNT(DISTINCT prccode) as product_count
    FROM $table_name
    WHERE indicators = 'PRODQNT'
        AND qntunit IS NOT NULL
    GROUP BY qntunit
    ORDER BY count DESC
    """
    units_df = DBInterface.execute(con, units_query) |> DataFrame
    println("\nAll unique units found (total: $(nrow(units_df))):")
    println(units_df)

    # 7. Check if there's a unit label field
    println("\n7. CHECKING FOR UNIT LABELS")
    println("-"^40)

    # Look for columns that might contain unit labels
    label_columns = filter(col -> contains(lowercase(string(col)), "label") ||
                                 contains(lowercase(string(col)), "unit"),
                          names(sample_df))
    println("Columns that might contain unit labels: ", label_columns)

    if length(label_columns) > 0
        for col in label_columns
            label_query = """
            SELECT DISTINCT
                qntunit,
                $col as label
            FROM $table_name
            WHERE indicators = 'PRODQNT'
                AND qntunit IS NOT NULL
                AND $col IS NOT NULL
            LIMIT 20
            """
            try
                label_df = DBInterface.execute(con, label_query) |> DataFrame
                println("\nUnit labels from column '$col':")
                println(label_df)
            catch e
                println("Could not query column '$col': $e")
            end
        end
    end

    # 8. Analyze value field for PRODQNT
    println("\n8. VALUE FIELD ANALYSIS FOR PRODQNT")
    println("-"^40)

    value_types_query = """
    SELECT
        CASE
            WHEN TRY_CAST(value AS DOUBLE) IS NOT NULL THEN 'Numeric'
            WHEN value IS NULL THEN 'NULL'
            ELSE 'Text'
        END as value_type,
        COUNT(*) as count,
        MIN(value) as min_value,
        MAX(value) as max_value
    FROM $table_name
    WHERE indicators = 'PRODQNT'
    GROUP BY value_type
    """
    value_types_df = DBInterface.execute(con, value_types_query) |> DataFrame
    println("\nValue field types for PRODQNT:")
    println(value_types_df)

    # Show some non-numeric values if they exist
    non_numeric_query = """
    SELECT DISTINCT
        value,
        COUNT(*) as count
    FROM $table_name
    WHERE indicators = 'PRODQNT'
        AND TRY_CAST(value AS DOUBLE) IS NULL
        AND value IS NOT NULL
    GROUP BY value
    ORDER BY count DESC
    LIMIT 20
    """
    non_numeric_df = DBInterface.execute(con, non_numeric_query) |> DataFrame
    if nrow(non_numeric_df) > 0
        println("\nNon-numeric values found in PRODQNT:")
        println(non_numeric_df)
    end

    # 9. Check specific examples by unit
    println("\n9. EXAMPLES BY UNIT TYPE")
    println("-"^40)

    # Show examples for kg (1500), tonnes (1000), and pieces (2600)
    example_units = [
        ("1500", "kg - Kilogram"),
        ("1000", "GT - Gross tonnage"),
        ("2600", "p/st - Number of items"),
        ("2000", "l - Litre")
    ]

    for (unit_code, unit_desc) in example_units
        example_query = """
        SELECT
            prccode as product_code,
            decl as country,
            value,
            qntunit
        FROM $table_name
        WHERE indicators = 'PRODQNT'
            AND qntunit = '$unit_code'
            AND value IS NOT NULL
        LIMIT 5
        """
        try
            example_df = DBInterface.execute(con, example_query) |> DataFrame
            if nrow(example_df) > 0
                println("\nExamples for $unit_desc:")
                println(example_df)
            end
        catch e
            println("\nNo examples found for $unit_desc")
        end
    end

    # 10. Summary statistics
    println("\n10. SUMMARY STATISTICS")
    println("-"^40)

    summary_query = """
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT prccode) as unique_products,
        COUNT(DISTINCT decl) as unique_countries,
        COUNT(DISTINCT qntunit) as unique_units,
        SUM(CASE WHEN TRY_CAST(value AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) as numeric_values,
        SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_values
    FROM $table_name
    WHERE indicators = 'PRODQNT'
    """
    summary_df = DBInterface.execute(con, summary_query) |> DataFrame
    println("\nPRODQNT Summary:")
    for col in names(summary_df)
        println("- $col: $(summary_df[1, col])")
    end

    # Calculate percentage of numeric values
    if summary_df.total_records[1] > 0
        numeric_pct = round(100 * summary_df.numeric_values[1] / summary_df.total_records[1], digits=1)
        println("\nNumeric values: $numeric_pct% of PRODQNT records")
    end

finally
    DBInterface.close!(con)
    DBInterface.close!(db_conn)
end

println("\n" * "="^80)
println("INSPECTION COMPLETE")
println("="^80)

println("\nKey findings:")
println("1. PRODQNT indicator contains production quantities")
println("2. Units are stored in the 'qntunit' field")
println("3. Values are stored in the 'value' field")
println("4. Some values may be non-numeric (need to check and filter)")
println("5. Unit codes need to be mapped to conversion factors")
