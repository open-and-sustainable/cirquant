module ProductConversionTables

using DataFrames
using DuckDB, DBInterface
using ..DatabaseAccess

export write_product_conversion_table, get_product_mapping_data, PRODUCT_MAPPING_TABLE_NAME

# Constants
const PRODUCT_MAPPING_TABLE_NAME = "product_mapping_codes"

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
    return DataFrame(
        product_id=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        product=[
            "Heat pumps",
            "PV panels",
            "Printers",
            "ICT - Smartphones",
            "ICT - Other phones",
            "ICT - Portable computers",
            "ICT - Other computers",
            "ICT - Monitors, I/O units",
            "ICT - Storage units",
            "ICT - Other units",
            "Full ICT (PRODCOM 26.20)",
            "Batteries - Li-ion",
            "Batteries - Other"
        ],
        prodcom_code=[
            "28.21.13.30",
            "27.11.40.00",
            "26.20.12.30",
            "26.20.11.30",
            "26.20.11.50",
            "26.20.13.00",
            "26.20.14.00",
            "26.20.16.00",
            "26.20.17.00",
            "26.20.18.00",
            "26.20",
            "27.20.23.00",
            "27.20.24.00"
        ],
        hs_codes=[
            "8418.69",
            "8541.43",
            "8443.31",
            "8517.13",
            "8517.14",
            "8471.30",
            "8471.41,8471.49",
            "8528.52,8471.60",
            "8471.70",
            "8471.80",
            "8517.13, 8517.14, 8471.30, 8471.41, 8471.49, 8528.52, 8471.60, 8471.70, 8471.80",
            "8507.60",
            "8507.10,8507.20,8507.50,8507.80"
        ]
    )
end

"""
    write_product_conversion_table(db_path::String; table_name::String = PRODUCT_MAPPING_TABLE_NAME, replace::Bool = true)

Writes the product conversion mapping table to a DuckDB database.

# Arguments
- `db_path::String`: Path to the DuckDB database file
- `table_name::String`: Name of the table to create (default: "product_mapping_codes")
- `replace::Bool`: Whether to replace existing table if it exists (default: true)

# Returns
- `Bool`: true if successful, false otherwise

# Example
```julia
using CirQuant.ProductConversionTables

# Write to the processed database
db_path = "CirQuant-database/processed/CirQuant_1995-2023.duckdb"
success = write_product_conversion_table(db_path)
```
"""
function write_product_conversion_table(db_path::String; table_name::String=PRODUCT_MAPPING_TABLE_NAME, replace::Bool=true)
    try
        @info "Writing product conversion table to database at: $db_path"

        # Get the mapping data
        mapping_df = get_product_mapping_data()

        # Use DatabaseAccess utility to write the table
        DatabaseAccess.write_duckdb_table!(mapping_df, db_path, table_name)

        @info "Successfully created/updated table '$table_name' with $(nrow(mapping_df)) product mappings"
        return true

    catch e
        @error "Failed to write product conversion table" exception = e
        return false
    end
end

"""
    read_product_conversion_table(db_path::String; table_name::String = PRODUCT_MAPPING_TABLE_NAME)

Reads the product conversion table from a DuckDB database.

# Arguments
- `db_path::String`: Path to the DuckDB database file
- `table_name::String`: Name of the table to read (default: "product_mapping_codes")

# Returns
- `DataFrame`: The product mapping data from the database
- `nothing`: If the table doesn't exist or an error occurs

# Example
```julia
using CirQuant.ProductConversionTables

db_path = "CirQuant-database/processed/CirQuant_1995-2023.duckdb"
mapping_df = read_product_conversion_table(db_path)
```
"""
function read_product_conversion_table(db_path::String; table_name::String=PRODUCT_MAPPING_TABLE_NAME)
    try
        # Connect to database
        db_conn = DuckDB.DB(db_path)
        con = DBInterface.connect(db_conn)

        # Check if table exists
        result = DBInterface.execute(con,
            "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '$table_name'"
        ) |> DataFrame

        if result.cnt[1] == 0
            @warn "Table '$table_name' does not exist in database"
            DBInterface.close!(con)
            DBInterface.close!(db_conn)
            return nothing
        end

        # Read the table
        df = DBInterface.execute(con, "SELECT * FROM $table_name") |> DataFrame

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        @info "Successfully read $(nrow(df)) rows from '$table_name'"
        return df

    catch e
        @error "Failed to read product conversion table" exception = e
        return nothing
    end
end

"""
    validate_product_mapping(mapping_df::DataFrame)

Validates the structure and content of a product mapping DataFrame.

# Arguments
- `mapping_df::DataFrame`: DataFrame to validate

# Returns
- `Bool`: true if valid, false otherwise

# Throws
- Logs warnings for any validation issues found
"""
function validate_product_mapping(mapping_df::DataFrame)
    is_valid = true

    # Check required columns
    required_cols = [:product_id, :product, :prodcom_code, :hs_codes]
    for col in required_cols
        if !(col in names(mapping_df, Symbol))
            @warn "Missing required column: $col"
            is_valid = false
        end
    end

    if !is_valid
        return false
    end

    # Check for duplicate product IDs
    if length(unique(mapping_df[:, :product_id])) != nrow(mapping_df)
        @warn "Duplicate product IDs found"
        is_valid = false
    end

    # Check for empty values
    for col in [:product, :prodcom_code, :hs_codes]
        empty_count = sum(ismissing.(mapping_df[!, col]) .| (mapping_df[!, col] .== ""))
        if empty_count > 0
            @warn "Found $empty_count empty values in column $col"
        end
    end

    return is_valid
end

"""
    get_product_by_code(db_path::String, code::String, code_type::Symbol = :prodcom_code)

Look up product information by a specific code.

# Arguments
- `db_path::String`: Path to the DuckDB database file
- `code::String`: The code to search for
- `code_type::Symbol`: Type of code (:prodcom_code or :hs_codes)

# Returns
- `DataFrame`: Matching products
- `nothing`: If no matches found or error occurs
"""
function get_product_by_code(db_path::String, code::String, code_type::Symbol=:prodcom_code)
    valid_types = [:prodcom_code, :hs_codes]
    if !(code_type in valid_types)
        @error "Invalid code_type. Must be one of: $valid_types"
        return nothing
    end

    mapping_df = read_product_conversion_table(db_path)
    if isnothing(mapping_df)
        return nothing
    end

    if code_type == :prodcom_code
        matches = mapping_df[mapping_df.prodcom_code.==code, :]
    else  # :hs_codes
        # Handle comma-separated HS codes
        matches = mapping_df[occursin.(code, mapping_df.hs_codes), :]
    end

    return isempty(matches) ? nothing : matches
end

end # module ProductConversionTables
