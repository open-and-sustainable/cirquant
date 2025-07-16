module CirQuant

using DataFrames, ProdcomAPI, ComextAPI

# Define the database paths as constants
const DB_PATH_RAW = "CirQuant-database/raw/CirQuant_2002-2023.duckdb"
const DB_PATH_PROCESSED = "CirQuant-database/processed/CirQuant_2002-2023.duckdb"

# Test database for development (contains only 2002 data)
const DB_PATH_TEST = "CirQuant-database/raw/test.duckdb"

# External parameters for circularity calculations - loaded from config/products.toml
# This will be initialized after module loading
global ANALYSIS_PARAMETERS = Dict{String,Any}()

# Include and use the modules
include("utils/DatabaseAccess.jl")
include("utils/AnalysisConfigLoader.jl")
include("DataFetch/ProdcomDataFetch.jl")
include("DataFetch/ComextDataFetch.jl")
include("DataTransform/CountryCodeMapper.jl")
include("DataTransform/DataProcessor.jl")

using .DatabaseAccess
using .AnalysisConfigLoader
using .ProdcomDataFetch
using .ComextDataFetch
using .CountryCodeMapper
using .DataProcessor

# Load analysis parameters from configuration file
global ANALYSIS_PARAMETERS = AnalysisConfigLoader.load_analysis_parameters()


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
                   If not provided, default datasets will be used.

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
    validate_product_config(config_path::String = joinpath(@__DIR__, "..", "config", "products.toml"))

Validate the product configuration file for completeness and consistency.

# Arguments
- `config_path::String`: Path to the products.toml configuration file (default: config/products.toml)

# Returns
- `Bool`: true if validation passes, false otherwise

# Example
```julia
# Validate the default configuration
is_valid = validate_product_config()

# Validate a custom configuration file
is_valid = validate_product_config("path/to/custom_products.toml")
```

The validation checks:
- Required fields are present for each product
- Data types are correct
- Values are within valid ranges
- Product IDs are unique
- Circularity rates are between 0-100%
- Potential rate ≥ current rate
"""
function validate_product_config(config_path::String = joinpath(@__DIR__, "..", "config", "products.toml"))
    return AnalysisConfigLoader.validate_product_config(config_path)
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
        analysis_params=ANALYSIS_PARAMETERS,
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
        analysis_params=ANALYSIS_PARAMETERS,
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
    fetch_comext_data,
    fetch_combined_data,
    ANALYSIS_PARAMETERS,
    DB_PATH_TEST,
    process_raw_to_processed,
    process_single_year,
    validate_product_config

end # module CirQuant
