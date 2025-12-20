module CirQuant

using DataFrames, ProdcomAPI, ComextAPI

# Define the database paths as constants
const DB_PATH_RAW = "CirQuant-database/raw/CirQuant_2002-2024.duckdb"
const DB_PATH_PROCESSED = "CirQuant-database/processed/CirQuant_2002-2024.duckdb"

# Test database for development (contains only 2002 data)
const DB_PATH_TEST = "CirQuant-database/raw/test.duckdb"

# External parameters for circularity calculations - loaded from config/products.toml
# This will be initialized after module loading
global ANALYSIS_PARAMETERS = Dict{String,Any}()

# Include and use the modules
include("utils/DatabaseAccess.jl")
include("utils/AnalysisConfigLoader.jl")
include("DataFetch/FetchUtils.jl")
include("DataFetch/ProdcomDataFetch.jl")
include("DataFetch/ComextDataFetch.jl")
include("DataTransform/CountryCodeMapper.jl")
include("DataFetch/MaterialCompositionFetch.jl")
include("DataFetch/MaterialRecyclingRatesFetch.jl")
include("DataFetch/ProductWeightsFetch.jl")
include("DataFetch/ProductCollectionRatesFetch.jl")
include("DataTransform/DataProcessor.jl")

using .DatabaseAccess
using .AnalysisConfigLoader
using .FetchUtils
using .ProdcomDataFetch
using .ComextDataFetch
using .MaterialCompositionFetch
using .MaterialRecyclingRatesFetch
using .ProductWeightsFetch
using .ProductCollectionRatesFetch
using .CountryCodeMapper
using .DataProcessor

# Load analysis parameters from configuration file
global ANALYSIS_PARAMETERS = AnalysisConfigLoader.load_analysis_parameters()


"""
    fetch_prodcom_data(years_str::String="2002-2024", custom_datasets=nothing; db_path=nothing, kwargs...)

Fetches PRODCOM data using the external ProdcomAPI package and saves it to DuckDB.
Defaults to the main raw DB, but `db_path` can override (e.g., for test fixtures).
Other keyword arguments are forwarded to `ProdcomDataFetch.fetch_prodcom_data`.

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR". Default: "2002-2024".
- `custom_datasets`: Optional array of dataset IDs to fetch. Default: module defaults (["ds-059358"]).
- `db_path`: Optional custom database path. Defaults to `DB_PATH_RAW` when `nothing`.
- Additional kwargs: passed through (e.g., `parallel_years`, `max_parallel_years`, rate limits, `product_keys_filter`).

Returns:
- Statistics about the fetching process including success/failure counts
"""
function fetch_prodcom_data(years_str::String="2002-2024", custom_datasets=nothing; db_path=nothing, kwargs...)
    target_db = isnothing(db_path) ? DB_PATH_RAW : db_path
    @info "Fetching PRODCOM data for years $years_str and saving to database $target_db"
    return ProdcomDataFetch.fetch_prodcom_data(years_str, custom_datasets; db_path=target_db, kwargs...)
end

"""
    fetch_comext_data(years_str::String="2002-2024", custom_datasets=nothing)

Fetches COMEXT data using the external ComextAPI package and saves it to the raw DuckDB database.
This function focuses on the data fetch and storage part of the workflow.
The years_str parameter should be in the format "START_YEAR-END_YEAR".

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "2002-2024".
- `custom_datasets`: Optional. An array of dataset IDs to fetch.
                   If not provided, uses available datasets from ComextAPI.

Returns:
- Statistics about the fetching process including success/failure counts
"""
function fetch_comext_data(years_str::String="2002-2024"; custom_datasets=nothing, db_path=nothing, product_keys_filter=nothing, prefer_cn8::Bool=false, kwargs...)
    target_db = isnothing(db_path) ? DB_PATH_RAW : db_path
    @info "Fetching COMEXT data for years $years_str and saving to database $target_db"
    return ComextDataFetch.fetch_comext_data(years_str, custom_datasets; db_path=target_db, product_keys_filter=product_keys_filter, prefer_cn8=prefer_cn8, kwargs...)
end

"""
    fetch_combined_data(years_str::String="2002-2024", prodcom_datasets=nothing, comext_datasets=nothing)

Fetches all data types for circular economy analysis for the same year range.
This includes PRODCOM, COMEXT, and all circular economy specific datasets.

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "2002-2024".
- `prodcom_datasets`: Optional. Array of Prodcom dataset IDs to fetch.
                     If not provided, defaults to ["ds-059358"].
- `comext_datasets`: Optional. Array of Comext dataset IDs to fetch.
                    If not provided, defaults to ["DS-059341"].

Returns:
- true on successful completion
"""
function fetch_combined_data(years_str::String="2002-2024", prodcom_datasets=nothing, comext_datasets=nothing)
    @info "Starting combined data fetch for all data sources for years $years_str"

    try
        # Step 1: Fetch PRODCOM data
        @info "Step 1/6: Fetching PRODCOM data..."
        fetch_prodcom_data(years_str, prodcom_datasets)
        @info "PRODCOM fetch completed."

        # Step 2: Fetch COMEXT data
        @info "Step 2/6: Fetching COMEXT data..."
        fetch_comext_data(years_str, comext_datasets)
        @info "COMEXT fetch completed."

        # Step 3: Fetch material composition data
        @info "Step 3/6: Fetching material composition data..."
        fetch_material_composition_data(years_str)
        @info "Material composition fetch completed (stub)."

        # Step 4: Fetch material recycling rates
        @info "Step 4/6: Fetching material recycling rates..."
        fetch_material_recycling_rates_data(years_str)
        @info "Material recycling rates fetch completed (stub)."

        # Step 5: Calculate product weights
        @info "Step 5/6: Calculating product weights from PRODCOM data..."
        fetch_product_weights_data(years_str)
        @info "Product weights calculation completed (stub)."

        # Step 6: Fetch product collection rates
        @info "Step 6/6: Fetching product collection rates..."
        fetch_product_collection_rates_data(years_str)
        @info "Product collection rates fetch completed (stub)."

        @info "All data fetching completed successfully!"
    catch e
        @error "Error in combined data fetch" exception = e
        return false
    end

    return true
end

"""
    fetch_material_composition_data(years_str::String="2002-2023")

Fetches product material composition data showing material breakdown (% by weight) for each product.
This data is essential for calculating material-specific recycling rates.
Currently a stub - data source needs to be identified.

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "2002-2023".

Returns:
- Nothing (stub implementation)
"""
function fetch_material_composition_data(years_str::String="2002-2023")
    @info "Fetching material composition data for years $years_str"
    return MaterialCompositionFetch.fetch_material_composition_data(years_str; db_path=DB_PATH_RAW)
end

"""
    fetch_material_recycling_rates_data(years_str::String="2002-2023")

Fetches material-specific recycling/recovery rates for each material type.
This data is needed to calculate actual material recovery from recycling processes.
Currently a stub - uses Eurostat env_wastrt dataset.

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "2002-2023".

Returns:
- Nothing (stub implementation)
"""
function fetch_material_recycling_rates_data(years_str::String="2002-2023")
    @info "Fetching material recycling rates data for years $years_str"
    return MaterialRecyclingRatesFetch.fetch_material_recycling_rates_data(years_str; db_path=DB_PATH_RAW)
end

"""
    fetch_product_weights_data(years_str::String="2002-2023")

Calculates average product weights from PRODCOM quantity/value data and writes
`product_average_weights_YYYY` tables to the processed DuckDB database.

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "2002-2023".

Returns:
- `true` if at least one table was written, otherwise `false`
"""
function fetch_product_weights_data(years_str::String="2002-2023")
    @info "Fetching product weights data for years $years_str"
    return ProductWeightsFetch.fetch_product_weights_data(years_str; db_path=DB_PATH_RAW, processed_db_path=DB_PATH_PROCESSED)
end

"""
    fetch_product_collection_rates_data(years_str::String="2002-2023")

Fetches product collection rates showing what percentage of end-of-life products
are collected for recycling. Currently a stub - uses Eurostat waste datasets.

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "2002-2023".

Returns:
- Nothing (stub implementation)
"""
function fetch_product_collection_rates_data(years_str::String="2002-2023")
    @info "Fetching product collection rates data for years $years_str"
    return ProductCollectionRatesFetch.fetch_product_collection_rates_data(years_str; db_path=DB_PATH_RAW)
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
    process_data(years_str::String="2002-2024"; kwargs...)

Process raw data into the processed database format for the specified year(s).
This function follows the same pattern as fetch_data functions, accepting either
a single year or a range of years.

# Arguments
- `years_str`: String specifying the year(s) to process. Can be:
  - Single year: "2022"
  - Year range: "2020-2023"
  Default is "2002-2024".

# Keywords
- `use_test_mode`: Use test database with only 2002 data (default: false)
- `source_db`: Path to raw database (auto-determined if not specified)
- `target_db`: Path to processed database (auto-determined if not specified)
- `prql_timeout`: Timeout for PRQL queries in seconds (default: 300)
- `cleanup_temp_tables`: Whether to remove temporary tables after processing (default: true)

# Returns
- Dictionary with processing results and statistics

# Examples
```julia
# Process a single year
results = process_data("2022")

# Process a range of years
results = process_data("2020-2023")

# Process all available years
results = process_data()

# Process test data (2002 only)
results = process_data(use_test_mode=true)

# Process with custom database paths
results = process_data("2020-2022", source_db="custom_raw.duckdb", target_db="custom_processed.duckdb")
```
"""
function process_data(years_str::String="2002-2024"; kwargs...)
    # Parse the years string to determine start and end years
    if contains(years_str, "-")
        # Range format: "YYYY-YYYY"
        parts = split(years_str, "-")
        if length(parts) != 2
            error("Invalid year range format. Expected 'YYYY-YYYY', got: $years_str")
        end
        start_year = parse(Int, strip(parts[1]))
        end_year = parse(Int, strip(parts[2]))
    else
        # Single year format: "YYYY"
        year = parse(Int, strip(years_str))
        start_year = year
        end_year = year
    end

    # Validate years
    if start_year > end_year
        error("Start year ($start_year) must be less than or equal to end year ($end_year)")
    end

    @info "Processing data for years: $start_year to $end_year"

    # Create processing configuration with external parameters
    config = DataProcessor.create_processing_config(;
        analysis_params=ANALYSIS_PARAMETERS,
        start_year=start_year,
        end_year=end_year,
        kwargs...
    )

    # Run the processing pipeline
    return DataProcessor.process_all_years(config)
end

# Export public API functions
export fetch_prodcom_data,
    fetch_comext_data,
    fetch_combined_data,
    fetch_material_composition_data,
    fetch_material_recycling_rates_data,
    fetch_product_weights_data,
    fetch_product_collection_rates_data,
    process_data,
    validate_product_config,
    ANALYSIS_PARAMETERS,
    DB_PATH_TEST

end # module CirQuant
