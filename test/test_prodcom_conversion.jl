# Test script for PRODCOM unit conversion to tonnes
# This demonstrates the first step of the circularity processor

using DataFrames
using DuckDB, DBInterface

# Add the src directory to the load path
push!(LOAD_PATH, joinpath(@__DIR__, "../src"))

# Include the converter modules
include("../src/DataTransform/UnitConversion/UnitConverter.jl")
include("../src/DataTransform/ProdcomUnitConverter.jl")

using .UnitConverter
using .ProdcomUnitConverter

# Configuration
const RAW_DB_PATH = "../CirQuant-database/raw/CirQuant_1995-2023.duckdb"
const TEST_YEAR = 2020  # Year to test

println("="^80)
println("PRODCOM UNIT CONVERSION TEST")
println("="^80)

# 1. Test unit converter functionality
println("\n1. TESTING UNIT CONVERTER")
println("-"^40)

# Test some conversions
test_values = [
    (1000.0, "1500", "kg"),      # 1000 kg = 1 tonne
    (5000.0, "1400", "g"),       # 5000 g = 0.005 tonnes
    (100.0, "2000", "l"),        # 100 l ≈ 0.1 tonnes (water)
    (50.0, "2600", "p/st"),      # 50 pieces - cannot convert
]

for (value, code, unit) in test_values
    result = convert_to_tonnes(value, code)
    convertible = is_convertible_to_tonnes(code)
    unit_info = get_unit_name(code)

    println("$value $unit (code: $code) -> $(isnan(result) ? "Not convertible" : "$result tonnes")")
    println("  Unit: $(unit_info[1]) - $(unit_info[2])")
    println("  Convertible: $convertible")
    println()
end

# 2. Show all convertible units
println("\n2. ALL CONVERTIBLE UNITS")
println("-"^40)
convertible_units = UnitConverter.get_all_convertible_units()
println("Total convertible units: $(nrow(convertible_units))")
println("\nFirst 10 convertible units:")
println(first(convertible_units, 10))

# 3. Analyze units in the database for the test year
println("\n3. ANALYZING UNITS IN DATABASE FOR YEAR $TEST_YEAR")
println("-"^40)

try
    units_analysis = analyze_units_in_database(RAW_DB_PATH, TEST_YEAR)

    println("Total unique units found: $(nrow(units_analysis))")
    println("\nTop 20 units by frequency:")
    println(first(units_analysis, 20))

    # Summary of convertible vs non-convertible
    convertible_records = sum(units_analysis[units_analysis.convertible, :record_count])
    total_records = sum(units_analysis.record_count)

    println("\nConversion coverage:")
    println("- Convertible records: $convertible_records ($(round(100*convertible_records/total_records, digits=1))%)")
    println("- Non-convertible records: $(total_records - convertible_records)")

catch e
    println("Error analyzing units: $e")
end

# 4. Process PRODCOM data for the test year
println("\n4. PROCESSING PRODCOM DATA FOR YEAR $TEST_YEAR")
println("-"^40)

try
    # Convert the data
    result_df = process_prodcom_to_tonnes(RAW_DB_PATH, TEST_YEAR)

    if nrow(result_df) > 0
        println("\nConversion successful!")
        println("Total product-geo combinations: $(nrow(result_df))")

        # Show some statistics
        println("\nProduction statistics (tonnes):")
        println("- Min: $(minimum(result_df.production_tonnes))")
        println("- Max: $(maximum(result_df.production_tonnes))")
        println("- Mean: $(round(mean(result_df.production_tonnes), digits=2))")
        println("- Median: $(round(median(result_df.production_tonnes), digits=2))")

        # Show top 10 products by production volume
        top_products = sort(result_df, :production_tonnes, rev=true)
        println("\nTop 10 product-geo combinations by production volume:")
        println(first(select(top_products, :product_code, :geo, :production_tonnes), 10))

        # Check specific product categories (e.g., batteries starting with '27')
        battery_products = filter(row -> startswith(row.product_code, "27"), result_df)
        if nrow(battery_products) > 0
            println("\nBattery products (code starting with '27'):")
            println("Found $(nrow(battery_products)) battery product-geo combinations")
            println(first(sort(battery_products, :production_tonnes, rev=true), 5))
        end
    else
        println("No data was converted successfully")
    end

catch e
    println("Error processing PRODCOM data: $e")
    println("\nStacktrace:")
    for (exc, bt) in Base.catch_stack()
        showerror(stdout, exc, bt)
        println()
    end
end

# 5. Verify the created table in the database
println("\n5. VERIFYING OUTPUT TABLE IN DATABASE")
println("-"^40)

try
    db_conn = DuckDB.DB(RAW_DB_PATH)
    con = DBInterface.connect(db_conn)

    output_table = "prodcom_tonnes_$TEST_YEAR"

    # Check if table exists
    exists_query = """
    SELECT COUNT(*) as exists
    FROM information_schema.tables
    WHERE table_name = '$output_table'
    """
    exists_df = DBInterface.execute(con, exists_query) |> DataFrame

    if exists_df.exists[1] > 0
        println("Table '$output_table' exists in database")

        # Get table info
        info_query = """
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT product_code) as unique_products,
            COUNT(DISTINCT geo) as unique_countries,
            MIN(production_tonnes) as min_tonnes,
            MAX(production_tonnes) as max_tonnes,
            AVG(production_tonnes) as avg_tonnes,
            SUM(production_tonnes) as total_tonnes
        FROM $output_table
        """
        info_df = DBInterface.execute(con, info_query) |> DataFrame

        println("\nTable statistics:")
        println("- Total records: $(info_df.total_records[1])")
        println("- Unique products: $(info_df.unique_products[1])")
        println("- Unique countries: $(info_df.unique_countries[1])")
        println("- Total production: $(round(info_df.total_tonnes[1]/1e6, digits=2)) million tonnes")
        println("- Average per record: $(round(info_df.avg_tonnes[1], digits=2)) tonnes")

        # Sample records
        sample_query = """
        SELECT *
        FROM $output_table
        ORDER BY production_tonnes DESC
        LIMIT 10
        """
        sample_df = DBInterface.execute(con, sample_query) |> DataFrame

        println("\nTop 10 records by production:")
        println(sample_df)
    else
        println("Table '$output_table' does not exist in database")
    end

    DBInterface.close!(con)
    DBInterface.close!(db_conn)

catch e
    println("Error verifying table: $e")
end

println("\n" * "="^80)
println("TEST COMPLETE")
println("="^80)

# 6. Example of how to process multiple years
println("\n6. EXAMPLE: Processing multiple years")
println("-"^40)
println("""
To process multiple years, you can use:

```julia
# Process years 2018-2022
results = ProdcomUnitConverter.convert_prodcom_range(RAW_DB_PATH, 2018, 2022)

# Access results for each year
for (year, df) in results
    println("Year \$year: \$(nrow(df)) records")
end
```
""")

println("\nNext steps:")
println("1. Add import/export data conversion")
println("2. Merge with product mapping tables")
println("3. Calculate apparent consumption (production + imports - exports)")
println("4. Add circularity indicators from literature/secondary sources")
