module CirQuant

using DataFrames, ProdcomAPI

# Define the database paths as constants
const DB_PATH_RAW = "CirQuant-database/raw/CirQuant_1995-2023.duckdb"
const DB_PATH_PROCESSED = "CirQuant-database/processed/CirQuant_1995-2023.duckdb"

# Include and use the modules
include("utils/DatabaseAccess.jl")
include("DataFetch/ProdcomDataFetch.jl")

using .DatabaseAccess
using .ProdcomDataFetch


"""
    fetch_prodcom_data(years_str::String="1995-2023", custom_datasets=nothing)

Fetches PRODCOM data using the external ProdcomAPI package and saves it to the raw DuckDB database.
This function focuses on the data fetch and storage part of the workflow.
The years_str parameter should be in the format "START_YEAR-END_YEAR".

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "1995-2023".
- `custom_datasets`: Optional. An array of dataset IDs to fetch.
                   If not provided, defaults to ["ds-056120", "ds-056121"].

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
    get_available_datasets()

Returns information about available PRODCOM datasets.

Returns:
- A DataFrame with dataset IDs and descriptions
"""
function get_available_datasets()
    return ProdcomAPI.get_available_datasets()
end

end # module CirQuant
