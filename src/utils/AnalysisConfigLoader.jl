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
    # Read the TOML file
    config = TOML.parsefile(config_path)

    # Initialize the dictionaries
    current_circularity_rates = Dict{String, Float64}()
    potential_circularity_rates = Dict{String, Float64}()
    product_weights_tonnes = Dict{String, Float64}()

    # Extract products section
    products = get(config, "products", Dict())

    # Process each product
    for (key, product_data) in products
        # Get PRODCOM codes and remove dots
        prodcom_codes = product_data["prodcom_codes"]

        for prodcom_code in prodcom_codes
            # Remove dots from PRODCOM code
            clean_code = replace(prodcom_code, "." => "")

            # Get parameters
            params = product_data["parameters"]

            # Add circularity rates
            current_circularity_rates[clean_code] = params["current_circularity_rate"]
            potential_circularity_rates[clean_code] = params["potential_circularity_rate"]

            # Convert weight from kg to tonnes
            weight_tonnes = params["weight_kg"] / 1000.0
            product_weights_tonnes[clean_code] = weight_tonnes
        end
    end

    # Create the final dictionary structure matching ANALYSIS_PARAMETERS
    return Dict{String, Any}(
        "current_circularity_rates" => current_circularity_rates,
        "potential_circularity_rates" => potential_circularity_rates,
        "product_weights_tonnes" => product_weights_tonnes,
        "placeholder_for_future_params" => Dict{String, Any}()
    )
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

        # Handle PRODCOM codes - take the first (and currently only) code
        prodcom_codes = product_data["prodcom_codes"]
        prodcom_code_str = prodcom_codes[1]  # Each product has exactly one PRODCOM code
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
    # Track validation results
    is_valid = true
    errors = String[]
    warnings = String[]

    # Try to parse the TOML file
    config = try
        TOML.parsefile(config_path)
    catch e
        push!(errors, "Failed to parse TOML file: $e")
        @error "Configuration file parsing failed" exception=e
        return false
    end

    # Check if products section exists
    if !haskey(config, "products")
        push!(errors, "Missing 'products' section in configuration file")
        @error "No products section found in configuration"
        return false
    end

    products = config["products"]

    # Track seen IDs to check for duplicates
    seen_ids = Set{Int}()
    seen_prodcom_codes = Set{String}()

    # Required fields for each product
    required_product_fields = ["id", "name", "prodcom_codes", "hs_codes", "parameters"]
    required_param_fields = ["weight_kg", "unit", "current_circularity_rate", "potential_circularity_rate"]

    # Validate each product
    for (product_key, product_data) in products
        # Check required fields
        for field in required_product_fields
            if !haskey(product_data, field)
                push!(errors, "Product '$product_key' missing required field: $field")
                is_valid = false
            end
        end

        # Skip further validation if basic fields are missing
        if !all(field -> haskey(product_data, field), required_product_fields)
            continue
        end

        # Validate ID
        id = product_data["id"]
        if !(id isa Integer)
            push!(errors, "Product '$product_key' has invalid id type (must be integer): $(typeof(id))")
            is_valid = false
        elseif id in seen_ids
            push!(errors, "Duplicate product ID found: $id")
            is_valid = false
        else
            push!(seen_ids, id)
        end

        # Validate name
        if !(product_data["name"] isa String) || isempty(product_data["name"])
            push!(errors, "Product '$product_key' has invalid or empty name")
            is_valid = false
        end

        # Validate PRODCOM codes
        prodcom_codes = product_data["prodcom_codes"]
        if !(prodcom_codes isa Vector)
            push!(errors, "Product '$product_key' prodcom_codes must be an array")
            is_valid = false
        elseif isempty(prodcom_codes)
            push!(errors, "Product '$product_key' has empty prodcom_codes array")
            is_valid = false
        else
            for code in prodcom_codes
                if !(code isa String) || isempty(code)
                    push!(errors, "Product '$product_key' has invalid PRODCOM code: $code")
                    is_valid = false
                end
                # Check for duplicate PRODCOM codes across products
                clean_code = replace(code, "." => "")
                if clean_code in seen_prodcom_codes
                    push!(warnings, "PRODCOM code '$code' appears in multiple products")
                else
                    push!(seen_prodcom_codes, clean_code)
                end
            end
        end

        # Validate HS codes
        hs_codes = product_data["hs_codes"]
        if !(hs_codes isa Vector)
            push!(errors, "Product '$product_key' hs_codes must be an array")
            is_valid = false
        elseif isempty(hs_codes)
            push!(errors, "Product '$product_key' has empty hs_codes array")
            is_valid = false
        else
            for code in hs_codes
                if !(code isa String) || isempty(code)
                    push!(errors, "Product '$product_key' has invalid HS code: $code")
                    is_valid = false
                end
            end
        end

        # Validate parameters section
        params = product_data["parameters"]
        if !(params isa Dict)
            push!(errors, "Product '$product_key' parameters must be a dictionary")
            is_valid = false
            continue
        end

        # Check required parameter fields
        for field in required_param_fields
            if !haskey(params, field)
                push!(errors, "Product '$product_key' missing parameter: $field")
                is_valid = false
            end
        end

        # Validate parameter values if they exist
        if haskey(params, "weight_kg")
            weight = params["weight_kg"]
            if !(weight isa Number)
                push!(errors, "Product '$product_key' weight_kg must be a number, got: $(typeof(weight))")
                is_valid = false
            elseif weight <= 0
                push!(errors, "Product '$product_key' weight_kg must be positive, got: $weight")
                is_valid = false
            end
        end

        if haskey(params, "unit")
            unit = params["unit"]
            if !(unit isa String) || isempty(unit)
                push!(errors, "Product '$product_key' unit must be a non-empty string")
                is_valid = false
            elseif unit != "piece"
                push!(warnings, "Product '$product_key' has unit '$unit' - verify conversion logic is appropriate")
            end
        end

        if haskey(params, "current_circularity_rate")
            current_rate = params["current_circularity_rate"]
            if !(current_rate isa Number)
                push!(errors, "Product '$product_key' current_circularity_rate must be a number")
                is_valid = false
            elseif current_rate < 0 || current_rate > 100
                push!(errors, "Product '$product_key' current_circularity_rate must be between 0 and 100, got: $current_rate")
                is_valid = false
            end
        end

        if haskey(params, "potential_circularity_rate")
            potential_rate = params["potential_circularity_rate"]
            if !(potential_rate isa Number)
                push!(errors, "Product '$product_key' potential_circularity_rate must be a number")
                is_valid = false
            elseif potential_rate < 0 || potential_rate > 100
                push!(errors, "Product '$product_key' potential_circularity_rate must be between 0 and 100, got: $potential_rate")
                is_valid = false
            end

            # Check that potential >= current
            if haskey(params, "current_circularity_rate") &&
               (current_rate isa Number) && (potential_rate isa Number) &&
               potential_rate < params["current_circularity_rate"]
                push!(errors, "Product '$product_key' potential_circularity_rate ($potential_rate) must be >= current_circularity_rate ($(params["current_circularity_rate"]))")
                is_valid = false
            end
        end
    end

    # Report results
    if !isempty(errors)
        @error "Configuration validation failed with $(length(errors)) error(s):"
        for error in errors
            @error "  - $error"
        end
    end

    if !isempty(warnings)
        @warn "Configuration validation produced $(length(warnings)) warning(s):"
        for warning in warnings
            @warn "  - $warning"
        end
    end

    if is_valid && isempty(errors)
        #@info "Configuration validation passed successfully for $(length(products)) products"
    end

    return is_valid
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
    write_product_conversion_table_with_connection(conn::DuckDB.Connection; table_name::String = "product_mapping_codes", config_path::String = PRODUCTS_CONFIG_PATH)

Write product conversion table using an existing database connection.
This avoids opening multiple connections which can cause corruption.

# Arguments
- `conn`: Existing DuckDB connection
- `table_name`: Name of the table to create (default: "product_mapping_codes")
- `config_path`: Path to the products configuration file

# Returns
- `true` if successful, `false` otherwise
"""
function write_product_conversion_table_with_connection(conn; table_name::String = "product_mapping_codes", config_path::String = PRODUCTS_CONFIG_PATH)
    try
        @info "Writing product conversion table using existing connection"

        # Load mapping data from TOML
        mapping_df = load_product_mappings(config_path)

        # Use DatabaseAccess utility with existing connection
        DatabaseAccess.write_duckdb_table_with_connection!(mapping_df, conn, table_name)

        @info "Successfully created/updated table '$table_name' with $(nrow(mapping_df)) product mappings"
        return true
    catch e
        @error "Failed to write product conversion table" exception=e
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
export write_product_conversion_table, write_product_conversion_table_with_connection, read_product_conversion_table, get_product_by_code

end # module AnalysisConfigLoader
