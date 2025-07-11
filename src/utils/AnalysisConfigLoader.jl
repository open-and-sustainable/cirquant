module AnalysisConfigLoader

using DataFrames
using TOML
using DuckDB, DBInterface
using ..DatabaseAccess

export load_analysis_parameters, load_product_mappings, validate_product_config

# Path to the products configuration file
const PRODUCTS_CONFIG_PATH = joinpath(@__DIR__, "..", "..", "config", "products.toml")

"""
    load_analysis_parameters(config_path::String = PRODUCTS_CONFIG_PATH)

Load analysis parameters from the products.toml configuration file.

# Arguments
- `config_path::String`: Path to the products.toml file (default: config/products.toml)

# Returns
- `Dict{String, Any}`: Analysis parameters dictionary in the format expected by CirQuant

# TODO
- Parse current_circularity_rates from products.toml
- Parse potential_circularity_rates from products.toml
- Parse product_weights_tonnes from products.toml
- Format as ANALYSIS_PARAMETERS structure
"""
function load_analysis_parameters(config_path::String = PRODUCTS_CONFIG_PATH)
    # TODO: Implement loading of analysis parameters from TOML
    # For now, return empty placeholder
    return Dict{String, Any}()
end

"""
    load_product_mappings(config_path::String = PRODUCTS_CONFIG_PATH)

Load product mapping data from the products.toml configuration file.

# Arguments
- `config_path::String`: Path to the products.toml file (default: config/products.toml)

# Returns
- `DataFrame`: Product mapping data with columns:
  - product_id: Unique identifier for each product
  - product: Human-readable product name
  - prodcom_code: PRODCOM classification code
  - hs_codes: Harmonized System codes (comma-separated if multiple)

# Example
```julia
mapping_df = load_product_mappings()
```
"""
function load_product_mappings(config_path::String = PRODUCTS_CONFIG_PATH)
    # Read the TOML file
    config = TOML.parsefile(config_path)

    # Initialize vectors for DataFrame columns
    product_ids = Int[]
    product_names = String[]
    prodcom_codes_list = String[]
    hs_codes_list = String[]

    # Extract products section
    products = get(config, "products", Dict())

    # Iterate through each product
    for (key, product_data) in products
        # Extract basic information
        push!(product_ids, product_data["id"])
        push!(product_names, product_data["name"])

        # Handle PRODCOM codes - join multiple codes with dots
        prodcom_codes = product_data["prodcom_codes"]
        prodcom_code_str = join(prodcom_codes, ",")
        push!(prodcom_codes_list, prodcom_code_str)

        # Handle HS codes - join multiple codes with commas
        hs_codes = product_data["hs_codes"]
        hs_code_str = join(hs_codes, ",")
        push!(hs_codes_list, hs_code_str)
    end

    # Create DataFrame
    mapping_df = DataFrame(
        product_id = product_ids,
        product = product_names,
        prodcom_code = prodcom_codes_list,
        hs_codes = hs_codes_list
    )

    # Sort by product_id to ensure consistent ordering
    sort!(mapping_df, :product_id)

    return mapping_df
end

"""
    validate_product_config(config_path::String = PRODUCTS_CONFIG_PATH)

Validate the products.toml configuration file to ensure all required information is provided.

# Arguments
- `config_path::String`: Path to the products.toml file (default: config/products.toml)

# Returns
- `Bool`: true if configuration is valid, false otherwise
- Logs warnings/errors for any validation issues

# Validation checks
- All products have required fields (id, name, prodcom_codes, hs_codes)
- All products have parameters section with required fields
- No duplicate product IDs
- Valid data types for all fields
- Circularity rates are between 0 and 100
- Potential rates >= current rates
"""
function validate_product_config(config_path::String = PRODUCTS_CONFIG_PATH)
    # TODO: Implement validation logic
    # For now, return true as placeholder
    return true
end

"""
    write_product_conversion_table(db_path::String; table_name::String = "product_mapping_codes", config_path::String = PRODUCTS_CONFIG_PATH)

Write the product conversion mapping table to a DuckDB database using data from products.toml.

# Arguments
- `db_path::String`: Path to the DuckDB database file
- `table_name::String`: Name of the table to create (default: "product_mapping_codes")
- `config_path::String`: Path to the products.toml file (default: config/products.toml)

# Returns
- `Bool`: true if successful, false otherwise
"""
function write_product_conversion_table(db_path::String; table_name::String = "product_mapping_codes", config_path::String = PRODUCTS_CONFIG_PATH)
    try
        @info "Writing product conversion table to database at: $db_path"

        # Load mapping data from TOML
        mapping_df = load_product_mappings(config_path)

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
    read_product_conversion_table(db_path::String; table_name::String = "product_mapping_codes")

Read the product conversion table from a DuckDB database.

# Arguments
- `db_path::String`: Path to the DuckDB database file
- `table_name::String`: Name of the table to read (default: "product_mapping_codes")

# Returns
- `DataFrame`: The product mapping data from the database
- `nothing`: If the table doesn't exist or an error occurs
"""
function read_product_conversion_table(db_path::String; table_name::String = "product_mapping_codes")
    try
        # Check if table exists using centralized function
        if !DatabaseAccess.table_exists(db_path, table_name)
            @error "Table '$table_name' does not exist in database at: $db_path"
            return nothing
        end

        # Connect to database to read the table
        db_conn = DuckDB.DB(db_path)
        con = DBInterface.connect(db_conn)

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
function get_product_by_code(db_path::String, code::String, code_type::Symbol = :prodcom_code)
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
        matches = mapping_df[mapping_df.prodcom_code .== code, :]
    else  # :hs_codes
        # Handle comma-separated HS codes
        matches = mapping_df[occursin.(code, mapping_df.hs_codes), :]
    end

    return isempty(matches) ? nothing : matches
end

# Export additional functions that replicate ProductConversionTables functionality
export write_product_conversion_table, read_product_conversion_table, get_product_by_code

end # module AnalysisConfigLoader
