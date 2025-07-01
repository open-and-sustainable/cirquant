module CirQuant

using DataFrames, ProdcomAPI, ComextAPI

# Define the database paths as constants
const DB_PATH_RAW = "CirQuant-database/raw/CirQuant_1995-2023.duckdb"
const DB_PATH_PROCESSED = "CirQuant-database/processed/CirQuant_1995-2023.duckdb"

# Include and use the modules
include("utils/DatabaseAccess.jl")
include("DataFetch/ProdcomDataFetch.jl")
include("DataFetch/ComextDataFetch.jl")

using .DatabaseAccess
using .ProdcomDataFetch
using .ComextDataFetch


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

end # module CirQuant
