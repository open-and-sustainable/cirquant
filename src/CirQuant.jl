module CirQuant

using DataFrames, ProdcomAPI, ComextAPI

# Define the database paths as constants
const DB_PATH_RAW = "CirQuant-database/raw/CirQuant_1995-2023.duckdb"
const DB_PATH_PROCESSED = "CirQuant-database/processed/CirQuant_1995-2023.duckdb"

# Include and use the modules
include("utils/DatabaseAccess.jl")
include("utils/ProductConversionTables.jl")
include("DataFetch/ProdcomDataFetch.jl")
include("DataFetch/ComextDataFetch.jl")
include("DataTransform/CircularityProcessor.jl")

using .DatabaseAccess
using .ProductConversionTables
using .ProdcomDataFetch
using .ComextDataFetch
using .CircularityProcessor


"""
    fetch_prodcom_data(years_str::String="1995-2023", custom_datasets=nothing)

Fetches PRODCOM data using the external ProdcomAPI package and saves it to the raw DuckDB database.
This function delegates all Eurostat API interactions to ProdcomAPI.jl, which handles the
complex data fetching and transformation logic. The function focuses on orchestrating the
data fetch process and persisting results to the database.

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "1995-2023".
- `custom_datasets`: Optional. An array of dataset IDs to fetch.
                   If not provided, uses ProdcomAPI.get_available_datasets() to get default datasets.

Returns:
- Statistics about the fetching process including success/failure counts
"""
function fetch_prodcom_data(years_str::String="1995-2023", custom_datasets=nothing)
    @info "Fetching PRODCOM data for years $years_str and saving to database"
    ProdcomDataFetch.fetch_prodcom_data(years_str, custom_datasets)
end

"""
    fetch_prodcom_dataset(dataset::String, year::Int)

Fetches PRODCOM data from Eurostat API for a specific dataset and year.
Returns a DataFrame with the processed data, without saving to a database.

This function uses the standalone ProdcomAPI module that can be used
independently of the database functionality.

Parameters:
- `dataset`: The Eurostat dataset ID (e.g., "ds-056120" or "ds-056121")
- `year`: The year to fetch data for (e.g., 2023)

Returns:
- A DataFrame containing the processed PRODCOM data
"""
function fetch_prodcom_dataset(dataset::String, year::Int)
    @info "Fetching PRODCOM dataset $dataset for year $year"
    return ProdcomAPI.fetch_prodcom_dataset(dataset, year)
end

"""
    fetch_comext_data(years_str::String="1995-2023", custom_datasets=nothing)

Fetches COMEXT data using the external ComextAPI package and saves it to the raw DuckDB database.
This function focuses on the data fetch and storage part of the workflow.
The years_str parameter should be in the format "START_YEAR-END_YEAR".

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "1995-2023".
- `custom_datasets`: Optional. An array of dataset IDs to fetch.
                   If not provided, uses available datasets from ComextAPI.

Returns:
- Statistics about the fetching process including success/failure counts
"""
function fetch_comext_data(years_str::String="1995-2023", custom_datasets=nothing)
    @info "Fetching COMEXT data for years $years_str and saving to database"
    return ComextDataFetch.fetch_comext_data(years_str, custom_datasets)
end

"""
    fetch_comext_dataset(dataset::String, year::Int)

Fetches COMEXT data from Eurostat API for a specific dataset and year.
Returns a DataFrame with the processed data, without saving to a database.

This function uses the standalone ComextAPI module that can be used
independently of the database functionality.

Parameters:
- `dataset`: The Eurostat dataset ID (e.g., "DS-045409")
- `year`: The year to fetch data for (e.g., 2023)

Returns:
- A DataFrame containing the processed COMEXT data
"""
function fetch_comext_dataset(dataset::String, year::Int)
    @info "Fetching COMEXT dataset $dataset for year $year"
    return ComextAPI.fetch_comext_dataset(dataset, year)
end

"""
    get_available_prodcom_datasets()

Returns information about available PRODCOM datasets.

Returns:
- A DataFrame with dataset IDs and descriptions
"""
function get_available_prodcom_datasets()
    return ProdcomAPI.get_available_datasets()
end

"""
    get_available_comext_datasets()

Returns information about available COMEXT datasets.

Returns:
- A DataFrame with dataset IDs and descriptions
"""
function get_available_comext_datasets()
    return ComextAPI.get_available_datasets()
end

"""
    fetch_combined_data(years_str::String="1995-2023", prodcom_datasets=nothing, comext_datasets=nothing)

Fetches both PRODCOM and COMEXT data for the same year range and saves to the same DuckDB database.
This is the main function to use for comprehensive data collection as requested.

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "1995-2023".
- `prodcom_datasets`: Optional. Array of Prodcom dataset IDs to fetch.
                     If not provided, defaults to ["ds-056120", "ds-056121"].
- `comext_datasets`: Optional. Array of Comext dataset IDs to fetch.
                    If not provided, uses available datasets from ComextAPI.

Returns:
- A dictionary with statistics for both data sources
"""
function fetch_combined_data(years_str::String="1995-2023", prodcom_datasets=nothing, comext_datasets=nothing)
    @info "Starting combined data fetch for PRODCOM and COMEXT data for years $years_str"

    # Initialize results
    results = Dict(
        :prodcom_stats => nothing,
        :comext_stats => nothing,
        :overall_success => false
    )

    try
        # Fetch Prodcom data first
        @info "Step 1/2: Fetching PRODCOM data..."
        prodcom_stats = fetch_prodcom_data(years_str, prodcom_datasets)
        results[:prodcom_stats] = prodcom_stats
        @info "PRODCOM fetch completed: $(prodcom_stats[:successful]) successful, $(prodcom_stats[:failed]) failed"

        # Fetch Comext data second
        @info "Step 2/2: Fetching COMEXT data..."
        comext_stats = fetch_comext_data(years_str, comext_datasets)
        results[:comext_stats] = comext_stats
        @info "COMEXT fetch completed: $(comext_stats[:successful]) successful, $(comext_stats[:failed]) failed"

        # Calculate overall statistics
        total_successful = prodcom_stats[:successful] + comext_stats[:successful]
        total_failed = prodcom_stats[:failed] + comext_stats[:failed]
        total_rows = prodcom_stats[:rows_processed] + comext_stats[:rows_processed]

        results[:overall_success] = total_successful > 0

        @info "COMBINED DATA FETCH COMPLETED:"
        @info "  PRODCOM: $(prodcom_stats[:successful])/$(prodcom_stats[:total_datasets]) successful ($(prodcom_stats[:rows_processed]) rows)"
        @info "  COMEXT: $(comext_stats[:successful])/$(comext_stats[:total_datasets]) successful ($(comext_stats[:rows_processed]) rows)"
        @info "  TOTAL: $total_successful successful, $total_failed failed, $total_rows total rows"

        if results[:overall_success]
            @info "✓ Combined data fetch successful - data saved to same DuckDB database"
        else
            @warn "Combined data fetch completed with issues - check individual statistics"
        end

    catch e
        @error "Error in combined data fetch" exception = e
        results[:overall_success] = false
    end

    return results
end

"""
    write_product_conversion_table(db_path::String = DB_PATH_PROCESSED; kwargs...)

Writes the product conversion mapping table to a DuckDB database.

# Arguments
- `db_path::String`: Path to the DuckDB database file (default: DB_PATH_PROCESSED)
- `table_name::String`: Name of the table to create (default: "product_mapping_codes")
- `replace::Bool`: Whether to replace existing table if it exists (default: true)

# Returns
- `Bool`: true if successful, false otherwise

# Example
```julia
# Write to the default processed database
success = write_product_conversion_table()

# Write to a custom database
success = write_product_conversion_table("path/to/custom.duckdb")
```
"""
function write_product_conversion_table(db_path::String=DB_PATH_PROCESSED; kwargs...)
    return ProductConversionTables.write_product_conversion_table(db_path; kwargs...)
end

"""
    read_product_conversion_table(db_path::String = DB_PATH_PROCESSED; kwargs...)

Reads the product conversion table from a DuckDB database.

# Arguments
- `db_path::String`: Path to the DuckDB database file (default: DB_PATH_PROCESSED)
- `table_name::String`: Name of the table to read (default: "product_mapping_codes")

# Returns
- `DataFrame`: The product mapping data from the database
- `nothing`: If the table doesn't exist or an error occurs
"""
function read_product_conversion_table(db_path::String=DB_PATH_PROCESSED; kwargs...)
    return ProductConversionTables.read_product_conversion_table(db_path; kwargs...)
end

"""
    get_product_mapping_data()

Returns a DataFrame containing the product mapping between different classification systems.

The DataFrame includes:
- `product_id`: Unique identifier for each product category
- `product`: Human-readable product name
- `prodcom_code`: PRODCOM classification code
- `hs_codes`: Harmonized System codes (comma-separated if multiple)

# Returns
- `DataFrame`: Product mapping data
"""
function get_product_mapping_data()
    return ProductConversionTables.get_product_mapping_data()
end

"""
    get_product_by_code(code::String, code_type::Symbol = :prodcom_code; db_path::String = DB_PATH_PROCESSED)

Look up product information by a specific code.

# Arguments
- `code::String`: The code to search for
- `code_type::Symbol`: Type of code (:prodcom_code or :hs_codes)
- `db_path::String`: Path to the DuckDB database file (default: DB_PATH_PROCESSED)

# Returns
- `DataFrame`: Matching products
- `nothing`: If no matches found or error occurs

# Example
```julia
# Look up by PRODCOM code
product = get_product_by_code("26.20.12.30", :prodcom_code)

# Look up by HS code
product = get_product_by_code("8507.60", :hs_codes)
```
"""
function get_product_by_code(code::String, code_type::Symbol=:prodcom_code; db_path::String=DB_PATH_PROCESSED)
    return ProductConversionTables.get_product_by_code(db_path, code, code_type)
end

"""
    create_circularity_table(year::Int; db_path::String = DB_PATH_PROCESSED, replace::Bool = false)

Creates the main circularity indicators table structure in the processed database for a specific year.
The table contains dimensions, key indicators, and circularity metrics.

# Arguments
- `year::Int`: The year for which to create the table
- `db_path::String`: Path to the DuckDB database (default: DB_PATH_PROCESSED)
- `replace::Bool`: Whether to replace existing table if it exists (default: false)

# Returns
- `Bool`: true if successful, false otherwise

# Example
```julia
# Create table for year 2023
success = create_circularity_table(2023)

# Create table with replace option
success = create_circularity_table(2023, replace=true)
```
"""
function create_circularity_table(year::Int; db_path::String=DB_PATH_PROCESSED, replace::Bool=false)
    return CircularityProcessor.create_circularity_table(year; db_path=db_path, replace=replace)
end

"""
    validate_circularity_table(year::Int; db_path::String = DB_PATH_PROCESSED)

Validates that the circularity table for a given year exists and has the correct structure.

# Arguments
- `year::Int`: The year to validate
- `db_path::String`: Path to the DuckDB database (default: DB_PATH_PROCESSED)

# Returns
- `Dict`: Dictionary with validation results

# Example
```julia
# Validate table for year 2023
validation_result = validate_circularity_table(2023)
if validation_result[:exists] && validation_result[:has_correct_columns]
    println("Table is valid with \$(validation_result[:row_count]) rows")
end
```
"""
function validate_circularity_table(year::Int; db_path::String=DB_PATH_PROCESSED)
    return CircularityProcessor.validate_circularity_table(year; db_path=db_path)
end

"""
    create_circularity_tables_range(start_year::Int, end_year::Int;
                                  db_path::String = DB_PATH_PROCESSED,
                                  replace::Bool = false)

Creates circularity indicator tables for a range of years.

# Arguments
- `start_year::Int`: Starting year
- `end_year::Int`: Ending year (inclusive)
- `db_path::String`: Path to the DuckDB database (default: DB_PATH_PROCESSED)
- `replace::Bool`: Whether to replace existing tables (default: false)

# Returns
- `Dict`: Summary of results with counts of successful and failed table creations

# Example
```julia
# Create tables for years 1995 to 2023
results = create_circularity_tables_range(1995, 2023)
println("Created \$(results[:successful]) tables successfully")
```
"""
function create_circularity_tables_range(start_year::Int, end_year::Int;
    db_path::String=DB_PATH_PROCESSED,
    replace::Bool=false)
    return CircularityProcessor.create_circularity_tables_range(start_year, end_year;
        db_path=db_path, replace=replace)
end

"""
    inspect_raw_tables(db_path::String, year::Int; show_sample::Bool = false)

Inspects the structure of raw database tables for a given year.
Shows column names and types for PRODCOM and COMEXT tables.

# Arguments
- `db_path::String`: Path to the raw DuckDB database (default: DB_PATH_RAW)
- `year::Int`: Year to inspect
- `show_sample::Bool`: Whether to show sample data (default: false)

# Returns
- `Dict`: Dictionary containing table information for each dataset

# Example
```julia
# Inspect tables for year 2009
table_info = inspect_raw_tables(DB_PATH_RAW, 2009)

# Inspect with sample data
table_info = inspect_raw_tables(DB_PATH_RAW, 2009, show_sample=true)
```
"""
function inspect_raw_tables(db_path::String=DB_PATH_RAW, year::Int=2009; show_sample::Bool=false)
    return CircularityProcessor.inspect_raw_tables(db_path, year; show_sample=show_sample)
end

"""
    ensure_prql_installed()

Ensures the PRQL extension is installed for DuckDB.
This should be called once before using PRQL queries.

# Returns
- `Bool`: true if PRQL is available, false otherwise

# Example
```julia
# Install PRQL extension
success = ensure_prql_installed()
if success
    println("PRQL extension is ready to use")
end
```
"""
function ensure_prql_installed()
    return CircularityProcessor.ensure_prql_installed()
end

end # module CirQuant
