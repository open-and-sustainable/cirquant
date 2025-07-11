module CirQuant

using DataFrames, ProdcomAPI, ComextAPI

# Define the database paths as constants
const DB_PATH_RAW = "CirQuant-database/raw/CirQuant_2002-2023.duckdb"
const DB_PATH_PROCESSED = "CirQuant-database/processed/CirQuant_2002-2023.duckdb"

# Test database for development (contains only 2002 data)
const DB_PATH_TEST = "CirQuant-database/raw/test.duckdb"

# External parameters for circularity calculations - PLACEHOLDERS
# TODO: These values need to be populated from literature/policy sources
const CIRCULARITY_PARAMETERS = Dict{String, Any}(
    # Current circularity rates by product (%)
    "current_circularity_rates" => Dict{String, Float64}(
        "default" => 0.0,
        # TODO: Add product-specific rates from literature
        # "28.21.13.30" => ?,  # Heat pumps
        # "27.11.40.00" => ?,  # PV panels
        # "26.20.12.30" => ?,  # Printers
        # "27.20.23.00" => ?,  # Li-ion batteries
        # etc.
    ),

    # Potential circularity rates with best practices (%)
    "potential_circularity_rates" => Dict{String, Float64}(
        "default" => 30.0,
        # TODO: Add product-specific potential rates from literature
        # "28.21.13.30" => ?,  # Heat pumps potential
        # "27.11.40.00" => ?,  # PV panels potential
        # "26.20.12.30" => ?,  # Printers potential
        # "27.20.23.00" => ?,  # Li-ion batteries potential
        # etc.
    ),

    # Product weight assumptions for unit conversions (tonnes per piece)
    "product_weights_tonnes" => Dict{String, Float64}(
        "default_piece" => 0.010,      # Default 10kg per piece
        "28211330" => 0.100,           # Heat pumps ~100kg
        "27114000" => 0.020,           # PV panels ~20kg
        "2720" => 0.025,               # Batteries (prefix) ~25kg
        "2620" => 0.005,               # ICT equipment (prefix) ~5kg
        "battery_cell" => 0.0003,      # Battery cells ~300g
        # TODO: Add more product-specific weights from technical specs
    ),

    # Other parameters as needed
    "placeholder_for_future_params" => Dict{String, Any}()
)

# Include and use the modules
include("utils/DatabaseAccess.jl")
include("utils/ProductConversionTables.jl")
include("DataFetch/ProdcomDataFetch.jl")
include("DataFetch/ComextDataFetch.jl")
include("DataTransform/CircularityProcessor.jl")
include("DataTransform/UnitConversion/UnitConverter.jl")
include("DataTransform/ProdcomUnitConverter.jl")
include("DataTransform/DataProcessor.jl")

using .DatabaseAccess
using .ProductConversionTables
using .ProdcomDataFetch
using .ComextDataFetch
using .CircularityProcessor
using .ProdcomUnitConverter
using .DataProcessor


"""
    fetch_prodcom_data(years_str::String="2002-2023", custom_datasets=nothing)

Fetches PRODCOM data using the external ProdcomAPI package and saves it to the raw DuckDB database.
This function delegates all Eurostat API interactions to ProdcomAPI.jl, which handles the
complex data fetching and transformation logic. The function focuses on orchestrating the
data fetch process and persisting results to the database.

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "2002-2023".
- `custom_datasets`: Optional. An array of dataset IDs to fetch.
                   If not provided, uses ProdcomAPI.get_available_datasets() to get default datasets.

Returns:
- Statistics about the fetching process including success/failure counts
"""
function fetch_prodcom_data(years_str::String="2002-2023", custom_datasets=nothing)
    @info "Fetching PRODCOM data for years $years_str and saving to database"
    return ProdcomDataFetch.fetch_prodcom_data(years_str, custom_datasets; db_path=DB_PATH_RAW)
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
function fetch_comext_data(years_str::String="2002-2023", custom_datasets=nothing)
    @info "Fetching COMEXT data for years $years_str and saving to database"
    return ComextDataFetch.fetch_comext_data(years_str, custom_datasets; db_path=DB_PATH_RAW)
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
                     If not provided, defaults to ["ds-056120"].
- `comext_datasets`: Optional. Array of Comext dataset IDs to fetch.
                    If not provided,  defaults to ["DS-059341"].

Returns:
- A dictionary with statistics for both data sources
"""
function fetch_combined_data(years_str::String="1995-2023", prodcom_datasets=nothing, comext_datasets=nothing)
    @info "Starting combined data fetch for PRODCOM and COMEXT data for years $years_str"

    try
        # Fetch Prodcom data first
        @info "Step 1/2: Fetching PRODCOM data..."
        prodcom_stats = fetch_prodcom_data(years_str, prodcom_datasets)
        @info "PRODCOM fetch completed."

        # Fetch Comext data second
        @info "Step 2/2: Fetching COMEXT data..."
        comext_stats = fetch_comext_data(years_str, comext_datasets)
        @info "COMEXT fetch completed."
    catch e
        @error "Error in combined data fetch" exception = e
    end

    return true
end

"""
    convert_prodcom_to_tonnes(year::Int; db_path::String = DB_PATH_RAW)

Converts PRODCOM production data for a specific year from various units to tonnes.
This function processes raw PRODCOM data and converts all production quantities
to a unified measurement unit (tonnes).

# Arguments
- `year::Int`: The year to process
- `db_path::String`: Path to the raw DuckDB database (default: DB_PATH_RAW)

# Returns
- `DataFrame`: Converted production data with columns:
  - product_code: PRODCOM product code
  - geo: Geographic location (country code or EU27)
  - year: Year of data
  - production_tonnes: Production quantity in tonnes

# Example
```julia
# Convert year 2020 data to tonnes
df = convert_prodcom_to_tonnes(2020)

# Convert with custom database path
df = convert_prodcom_to_tonnes(2020, db_path="path/to/database.duckdb")
```
"""
function convert_prodcom_to_tonnes(year::Int; db_path::String=DB_PATH_RAW)
    return ProdcomUnitConverter.process_prodcom_to_tonnes(db_path, year)
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

"""
    process_raw_to_processed(; kwargs...)

Main function to process raw data into the processed database format.
This orchestrates the complete transformation pipeline.

# Keywords
- `use_test_mode`: Use test database with only 2002 data (default: false)
- `start_year`: Starting year for processing (default: 2002)
- `end_year`: Ending year for processing (default: 2023)
- `source_db`: Path to raw database (auto-determined if not specified)
- `target_db`: Path to processed database (auto-determined if not specified)

# Returns
- Dictionary with processing results and statistics

# Example
```julia
# Process test data (2002 only)
results = process_raw_to_processed(use_test_mode=true)

# Process full dataset
results = process_raw_to_processed(start_year=2002, end_year=2023)

# Process specific years
results = process_raw_to_processed(start_year=2020, end_year=2022)
```
"""
function process_raw_to_processed(; kwargs...)
    # Create processing configuration with external parameters
    config = DataProcessor.create_processing_config(;
        external_params=CIRCULARITY_PARAMETERS,
        kwargs...
    )

    # Run the complete processing pipeline
    return DataProcessor.process_all_years(config)
end

"""
    process_single_year(year::Int; kwargs...)

Process a single year of raw data into the processed format.

# Arguments
- `year`: The year to process

# Keywords
- `use_test_mode`: Use test database (default: false)
- `source_db`: Path to raw database
- `target_db`: Path to processed database

# Returns
- Dictionary with processing results for the year

# Example
```julia
# Process year 2022
result = process_single_year(2022)

# Process year 2002 from test database
result = process_single_year(2002, use_test_mode=true)
```
"""
function process_single_year(year::Int; kwargs...)
    config = DataProcessor.create_processing_config(;
        external_params=CIRCULARITY_PARAMETERS,
        start_year=year,
        end_year=year,
        kwargs...
    )

    # Ensure database structure
    DataProcessor.ensure_processed_db_structure(config)

    # Process the single year
    return DataProcessor.process_year_complete(year, config)
end

# Export public API functions
export fetch_prodcom_data,
       fetch_prodcom_dataset,
       fetch_comext_data,
       fetch_comext_dataset,
       get_available_prodcom_datasets,
       get_available_comext_datasets,
       fetch_combined_data,
       convert_prodcom_to_tonnes,
       write_product_conversion_table,
       read_product_conversion_table,
       get_product_mapping_data,
       get_product_by_code,
       create_circularity_table,
       validate_circularity_table,
       create_circularity_tables_range,
       inspect_raw_tables,
       ensure_prql_installed,
       CIRCULARITY_PARAMETERS,
       DB_PATH_TEST,
       process_raw_to_processed,
       process_single_year

end # module CirQuant
