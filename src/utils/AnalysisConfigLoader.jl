"""
    AnalysisConfigLoader

Module for loading and managing analysis configuration from products.toml file.

Provides functionality to:
- Load analysis parameters (circularity rates, product weights)
- Load product mappings between PRODCOM and HS codes
- Validate configuration files
- Read/write product conversion tables to/from DuckDB

# Exports
- `load_analysis_parameters`: Load circularity rates and weights
- `load_product_mappings`: Load product code mappings
- `validate_product_config`: Validate configuration file structure
"""
module AnalysisConfigLoader

using DataFrames
using TOML
using DuckDB, DBInterface
using ..DatabaseAccess

export load_analysis_parameters, load_product_mappings, validate_product_config, prodcom_codes_for_year

# Path to the products configuration file
const PRODUCTS_CONFIG_PATH = joinpath(@__DIR__, "..", "..", "config", "products.toml")
const DEFAULT_PRODCOM_EPOCHS = Dict(
    "legacy" => (
        label = "Legacy PRODCOM (NACE Rev. 1.1)",
        start_year = 1995,
        end_year = 2007,
        description = "Codes used before the 2008 Prodcom revision."
    ),
    "nace_rev2" => (
        label = "PRODCOM List 2008+ (NACE Rev. 2)",
        start_year = 2008,
        end_year = 2100,
        description = "Codes introduced with the 2008 Prodcom list."
    )
)

"""
    _load_products_config(config_path)

Internal helper to parse the products configuration file.
"""
function _load_products_config(config_path::String)
    return TOML.parsefile(config_path)
end

"""
    _get_prodcom_epochs(config)

Return a dictionary mapping epoch keys to metadata named tuples.
Falls back to DEFAULT_PRODCOM_EPOCHS when the config does not define epochs.
"""
function _get_prodcom_epochs(config::Dict)
    epochs_config = get(config, "prodcom_epochs", nothing)
    if epochs_config === nothing
        return DEFAULT_PRODCOM_EPOCHS
    end

    epoch_map = Dict{String,NamedTuple}()
    for (epoch_key, epoch_data) in epochs_config
        start_year = get(epoch_data, "start_year", nothing)
        end_year = get(epoch_data, "end_year", nothing)
        if start_year === nothing || end_year === nothing
            @warn "Epoch '$epoch_key' is missing start_year/end_year; skipping and using defaults"
            continue
        end

        epoch_map[string(epoch_key)] = (
            label = get(epoch_data, "label", string(epoch_key)),
            start_year = Int(start_year),
            end_year = Int(end_year),
            description = get(epoch_data, "description", "")
        )
    end

    return isempty(epoch_map) ? DEFAULT_PRODCOM_EPOCHS : epoch_map
end

"""
    _normalize_code_list(entry)

Ensures the entry is returned as a vector of trimmed String codes.
"""
function _normalize_code_list(entry)
    if entry isa String
        return [strip(String(entry))]
    elseif entry isa Vector
        return [strip(String(code)) for code in entry if !isempty(strip(String(code)))]
    else
        error("Invalid PRODCOM code entry: expected String or Vector, got $(typeof(entry))")
    end
end

"""
    _collect_prodcom_code_sets(product_data)

Return a dictionary mapping epoch keys to arrays of PRODCOM codes.
Supports both legacy array syntax and the new epoch-aware tables.
"""
function _collect_prodcom_code_sets(product_data::Dict)
    prodcom_entry = product_data["prodcom_codes"]
    code_sets = Dict{String,Vector{String}}()

    if prodcom_entry isa Dict
        for (epoch_key, codes) in prodcom_entry
            code_sets[string(epoch_key)] = _normalize_code_list(codes)
        end
    elseif prodcom_entry isa Vector
        # Backwards compatibility: treat as legacy epoch
        code_sets["legacy"] = _normalize_code_list(prodcom_entry)
    else
        error("prodcom_codes must be a table of epochs or an array of strings")
    end

    return code_sets
end

"""
    _ordered_epoch_keys(epoch_map)

Return epoch keys sorted by their start_year (ascending).
"""
function _ordered_epoch_keys(epoch_map::Dict{String,NamedTuple})
    return sort(collect(keys(epoch_map)); by = key -> epoch_map[key].start_year)
end

"""
    _select_epoch_for_year(year, epoch_map)

Return the epoch key that covers the requested year.
"""
function _select_epoch_for_year(year::Int, epoch_map::Dict{String,NamedTuple})
    ordered = _ordered_epoch_keys(epoch_map)
    for epoch_key in ordered
        info = epoch_map[epoch_key]
        if info.start_year <= year <= info.end_year
            return epoch_key
        end
    end
    return nothing
end

"""
    _select_fallback_codes(code_sets, epochs, year, ordered_epochs)

Choose the best available code set when a product does not define codes for the requested epoch.
"""
function _select_fallback_codes(code_sets::Dict{String,Vector{String}},
                                epochs::Dict{String,NamedTuple},
                                year::Int,
                                ordered_epochs::Vector{String})
    # Prefer epochs that cover the requested year even if they are different keys
    for epoch_key in ordered_epochs
        if haskey(code_sets, epoch_key)
            epoch_info = get(epochs, epoch_key, nothing)
            if epoch_info !== nothing && epoch_info.start_year <= year <= epoch_info.end_year
                return code_sets[epoch_key]
            end
        end
    end

    # Otherwise return the last available epoch definition
    for epoch_key in reverse(ordered_epochs)
        if haskey(code_sets, epoch_key)
            return code_sets[epoch_key]
        end
    end

    return nothing
end

"""
    prodcom_codes_for_year(year::Int; config_path::String = PRODUCTS_CONFIG_PATH, products_filter=nothing)

Return the PRODCOM codes (with and without dots) that should be used for the requested year.

# Returns
- NamedTuple with:
  - `epoch_key`: Selected epoch key
  - `epoch_info`: Metadata about the epoch
  - `codes_clean`: Vector of codes without dots
  - `codes_original`: Vector of codes as written in the config
  - `clean_to_original`: Dict mapping cleaned codes to the original representation
  - `clean_to_product`: Dict mapping cleaned codes to `(product_id, product_name)`
"""
function prodcom_codes_for_year(year::Int; config_path::String = PRODUCTS_CONFIG_PATH, products_filter=nothing)
    config = _load_products_config(config_path)
    epochs = _get_prodcom_epochs(config)
    ordered_epochs = _ordered_epoch_keys(epochs)
    filter_keys = isnothing(products_filter) ? nothing : Set(string.(products_filter))

    epoch_key = _select_epoch_for_year(year, epochs)
    if epoch_key === nothing
        error("No PRODCOM epoch covers year $year; please update prodcom_epochs in products.toml")
    end

    products = get(config, "products", Dict())
    codes_clean = String[]
    codes_original = String[]
    clean_to_original = Dict{String,String}()
    clean_to_product = Dict{String,Tuple{Int,String}}()

    for (product_key, product_data) in products
        if filter_keys !== nothing && !(string(product_key) in filter_keys)
            continue
        end
        code_sets = _collect_prodcom_code_sets(product_data)
        selected_codes = get(code_sets, epoch_key, nothing)
        if selected_codes === nothing
            selected_codes = _select_fallback_codes(code_sets, epochs, year, ordered_epochs)
        end
        selected_codes === nothing && continue

        for code in selected_codes
            clean_code = replace(code, "." => "")
            push!(codes_clean, clean_code)
            push!(codes_original, code)
            clean_to_original[clean_code] = code
            clean_to_product[clean_code] = (product_data["id"], product_data["name"])
        end
    end

    return (
        epoch_key = epoch_key,
        epoch_info = epochs[epoch_key],
        codes_clean = unique(codes_clean),
        codes_original = unique(codes_original),
        clean_to_original = clean_to_original,
        clean_to_product = clean_to_product
    )
end

"""
    load_analysis_parameters(config_path::String = PRODUCTS_CONFIG_PATH)

Load analysis parameters from the products.toml configuration file.

# Arguments
- `config_path::String`: Path to the products.toml file (default: config/products.toml)

# Returns
- `Dict{String, Any}`: Analysis parameters dictionary containing:
  - `product_weights_tonnes`: Dict mapping PRODCOM codes to product weights in tonnes
  - `placeholder_for_future_params`: Empty Dict for future parameters

# Notes
- PRODCOM codes have dots removed (e.g., "10.11.11.40" becomes "10111140")
- Weights are converted from kg (in config) to tonnes
- Each product can have multiple PRODCOM codes, all inherit the same parameters
"""
function load_analysis_parameters(config_path::String = PRODUCTS_CONFIG_PATH)
    # Read the TOML file
    config = TOML.parsefile(config_path)

    # Initialize the dictionaries
    current_refurbishment_rates = Dict{String, Float64}()
    product_weights_tonnes = Dict{String, Float64}()

    # Extract products section
    products = get(config, "products", Dict())

    # Process each product
    for (key, product_data) in products
        code_sets = _collect_prodcom_code_sets(product_data)
        flattened_codes = unique(vcat(values(code_sets)...))

        for prodcom_code in flattened_codes
            # Remove dots from PRODCOM code
            clean_code = replace(prodcom_code, "." => "")

            # Get parameters
            params = product_data["parameters"]

            # Add refurbishment rates
            current_refurbishment_rates[clean_code] = params["current_refurbishment_rate"]

            # Convert weight from kg to tonnes
            weight_tonnes = params["weight_kg"] / 1000.0
            product_weights_tonnes[clean_code] = weight_tonnes
        end
    end

    # Create the final dictionary structure matching ANALYSIS_PARAMETERS
    # Load global circularity uplift (optional)
    uplift_config = get(config, "circularity_uplift", Dict())
    uplift_mean = get(uplift_config, "mean", 0.0)
    uplift_sd = get(uplift_config, "sd", 0.0)
    uplift_ci_lower = get(uplift_config, "ci_lower", 0.0)
    uplift_ci_upper = get(uplift_config, "ci_upper", 0.0)

    return Dict{String, Any}(
        "current_refurbishment_rates" => current_refurbishment_rates,
        "product_weights_tonnes" => product_weights_tonnes,
        "circularity_uplift" => Dict(
            "mean" => uplift_mean,
            "sd" => uplift_sd,
            "ci_lower" => uplift_ci_lower,
            "ci_upper" => uplift_ci_upper
        ),
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
  - prodcom_code: PRODCOM classification code (with dots)
  - prodcom_code_clean: PRODCOM code stripped of dots
  - hs_codes: Harmonized System codes (comma-separated if multiple)
  - prodcom_epoch: Epoch key (e.g. `legacy`, `nace_rev2`)
  - epoch_label: Human-readable label for the epoch
  - epoch_start_year / epoch_end_year: Year coverage for the epoch
  - weee_waste_codes: Eurostat WEEE category codes (for filtering env_waselee datasets)

# Example
```julia
mapping_df = load_product_mappings()
```
"""
function load_product_mappings(config_path::String = PRODUCTS_CONFIG_PATH)
    config = _load_products_config(config_path)
    epochs = _get_prodcom_epochs(config)
    ordered_epochs = _ordered_epoch_keys(epochs)

    # Initialize vectors for DataFrame columns
    product_ids = Int[]
    product_names = String[]
    prodcom_codes_list = String[]
    prodcom_codes_clean = String[]
    hs_codes_list = String[]
    weee_codes_list = String[]
    epoch_keys = String[]
    epoch_labels = String[]
    epoch_start_years = Int[]
    epoch_end_years = Int[]

    products = get(config, "products", Dict())
    for (_, product_data) in products
        product_id = product_data["id"]
        product_name = product_data["name"]
        hs_code_str = join(product_data["hs_codes"], ",")
        weee_codes = get(product_data, "weee_waste_codes", String[])
        weee_codes_str = isempty(weee_codes) ? "" : join(weee_codes, ",")

        code_sets = _collect_prodcom_code_sets(product_data)

        for epoch_key in ordered_epochs
            codes = get(code_sets, epoch_key, nothing)
            codes === nothing && continue

            epoch_info = get(epochs, epoch_key, (
                label = epoch_key,
                start_year = missing,
                end_year = missing,
                description = ""
            ))

            for prodcom_code in codes
                push!(product_ids, product_id)
                push!(product_names, product_name)
                push!(prodcom_codes_list, prodcom_code)
                push!(prodcom_codes_clean, replace(prodcom_code, "." => ""))
                push!(hs_codes_list, hs_code_str)
                push!(weee_codes_list, weee_codes_str)
                push!(epoch_keys, epoch_key)
                push!(epoch_labels, epoch_info.label)
                push!(epoch_start_years, epoch_info.start_year)
                push!(epoch_end_years, epoch_info.end_year)
            end
        end
    end

    mapping_df = DataFrame(
        product_id = product_ids,
        product = product_names,
        prodcom_code = prodcom_codes_list,
        prodcom_code_clean = prodcom_codes_clean,
        hs_codes = hs_codes_list,
        weee_waste_codes = weee_codes_list,
        prodcom_epoch = epoch_keys,
        epoch_label = epoch_labels,
        epoch_start_year = epoch_start_years,
        epoch_end_year = epoch_end_years
    )

    sort!(mapping_df, [:product_id, :prodcom_epoch, :prodcom_code])
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

    # Required fields for each product
    required_product_fields = ["id", "name", "prodcom_codes", "hs_codes", "parameters"]
    required_param_fields = [
        "weight_kg",
        "unit",
        "current_refurbishment_rate"
    ]

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
        code_sets = Dict{String,Vector{String}}()
        try
            code_sets = _collect_prodcom_code_sets(product_data)
        catch e
            push!(errors, "Product '$product_key' has invalid prodcom_codes format: $e")
            is_valid = false
        end

        if isempty(code_sets)
            push!(errors, "Product '$product_key' must specify at least one PRODCOM code")
            is_valid = false
        else
            for (epoch_key, codes) in code_sets
                if isempty(codes)
                    push!(errors, "Product '$product_key' epoch '$epoch_key' must include at least one PRODCOM code")
                    is_valid = false
                end

                for code in codes
                    if !(code isa AbstractString) || isempty(strip(String(code)))
                        push!(errors, "Product '$product_key' epoch '$epoch_key' has invalid PRODCOM code: '$code'")
                        is_valid = false
                    end
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



        if haskey(params, "current_refurbishment_rate")
            current_refurb = params["current_refurbishment_rate"]
            if !(current_refurb isa Number)
                push!(errors, "Product '$product_key' current_refurbishment_rate must be a number")
                is_valid = false
            elseif current_refurb < 0 || current_refurb > 100
                push!(errors, "Product '$product_key' current_refurbishment_rate must be between 0 and 100, got: $current_refurb")
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

    if haskey(config, "circularity_uplift")
        uplift = config["circularity_uplift"]
        for field in ["mean", "sd", "ci_lower", "ci_upper"]
            if !haskey(uplift, field)
                push!(errors, "Missing circularity_uplift.$field in configuration file")
                is_valid = false
            elseif !(uplift[field] isa Number)
                push!(errors, "circularity_uplift.$field must be a number")
                is_valid = false
            elseif uplift[field] < 0 || uplift[field] > 100
                push!(errors, "circularity_uplift.$field must be between 0 and 100, got: $(uplift[field])")
                is_valid = false
            end
        end
        if haskey(uplift, "ci_lower") && haskey(uplift, "ci_upper") &&
           uplift["ci_lower"] > uplift["ci_upper"]
            push!(errors, "circularity_uplift.ci_lower must be <= ci_upper")
            is_valid = false
        end
    else
        push!(warnings, "Missing circularity_uplift section; defaulting to 0.0 uplift")
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
- `DataFrame`: Product mapping data with columns:
  - product_id: Unique identifier
  - product: Product name
  - prodcom_code: PRODCOM classification code
  - hs_codes: Harmonized System codes
- `nothing`: If table doesn't exist or error occurs
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
export write_product_conversion_table, write_product_conversion_table_with_connection
export read_product_conversion_table, get_product_by_code
export write_product_conversion_table, write_product_conversion_table_with_connection, read_product_conversion_table, get_product_by_code

end # module AnalysisConfigLoader
