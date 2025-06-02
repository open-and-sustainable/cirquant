module CirQuant

using DataFrames

# Define the database paths as constants
const DB_PATH_RAW = "CirQuant-database/raw/CirQuant_1995-2023.duckdb"
const DB_PATH_PROCESSED = "CirQuant-database/processed/CirQuant_1995-2023.duckdb"

# Include and use the renamed modules
include("utils/DatabaseAccess.jl")
include("DataFetch/ProdcomDataFetch.jl")

using .DatabaseAccess
using .ProdcomDataFetch


"""
    fetch_prodcom_data(years_str::String="1995-2023", custom_datasets=nothing)

Fetches PRODCOM data from Eurostat API for datasets ds-056120 and ds-056121.
The years_str parameter should be in the format "START_YEAR-END_YEAR".

Parameters:
- `years_str`: String specifying the year range in format "START_YEAR-END_YEAR".
              Default is "1995-2023".
- `custom_datasets`: Optional. An array of dataset IDs to fetch. 
                   If not provided, defaults to ["ds-056120", "ds-056121"].
"""
function fetch_prodcom_data(years_str::String="1995-2023", custom_datasets=nothing)
    @info "Fetching PRODCOM data for years $years_str"
    ProdcomDataFetch.fetch_prodcom_data(years_str, custom_datasets)
end

end # module CirQuant
