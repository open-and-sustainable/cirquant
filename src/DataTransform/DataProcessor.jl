module DataProcessor

using DataFrames
using DuckDB, DBInterface
using Dates
using Dates: now, format
using ..DatabaseAccess
using ..AnalysisConfigLoader
using ..CircularityProcessor
using ..CountryCodeMapper

"""
Main data processing orchestrator module that transforms raw data into processed analysis-ready tables.
This module coordinates the execution of PRQL queries and Julia processing functions to create
the complete processed database as documented in database-schema-processed.md.
"""

# Path to PRQL query files
const PRQL_DIR = joinpath(@__DIR__, "prql")

# Table name prefixes and patterns
const CIRCULARITY_TABLE_PREFIX = "circularity_indicators_"
const COUNTRY_AGGREGATES_PREFIX = "country_aggregates_"
const PRODUCT_AGGREGATES_PREFIX = "product_aggregates_"
const ANALYSIS_PARAMS_TABLE = "analysis_parameters"

"""
    ProcessingConfig

Configuration for data processing pipeline
"""
struct ProcessingConfig
    source_db::String  # Path to raw database
    target_db::String  # Path to processed database
    year_range::Tuple{Int,Int}
    use_test_mode::Bool
    analysis_params::Dict{String,Any}
    prql_timeout::Int  # Timeout for PRQL queries in seconds
    cleanup_temp_tables::Bool  # Whether to remove temporary tables after processing
end

"""
    create_processing_config(; kwargs...) -> ProcessingConfig

Create a processing configuration with sensible defaults.

# Keywords
- `source_db`: Path to raw database (default: uses test.duckdb if use_test_mode=true)
- `target_db`: Path to processed database
- `start_year`: Starting year for processing (default: 2002)
- `end_year`: Ending year for processing (default: 2023)
- `use_test_mode`: Use test database with only 2002 data (default: false)
- `analysis_params`: Analysis parameters for circularity calculations
- `prql_timeout`: Timeout for PRQL queries in seconds (default: 300)
- `cleanup_temp_tables`: Whether to remove temporary tables after processing (default: true)
"""
function create_processing_config(;
    source_db::String="",
    target_db::String="",
    start_year::Int=2002,
    end_year::Int=2023,
    use_test_mode::Bool=false,
    analysis_params::Dict{String,Any}=Dict{String,Any}(),
    prql_timeout::Int=300,
    cleanup_temp_tables::Bool=true
)
    # Determine database paths
    if isempty(source_db)
        source_db = use_test_mode ?
                    "CirQuant-database/raw/test.duckdb" :
                    "CirQuant-database/raw/CirQuant_2002-2023.duckdb"
    end

    if isempty(target_db)
        target_db = use_test_mode ?
                    "CirQuant-database/processed/test_processed.duckdb" :
                    "CirQuant-database/processed/CirQuant_2002-2023.duckdb"
    end

    # Use only 2002 if in test mode
    if use_test_mode
        start_year = 2002
        end_year = 2002
    end

    return ProcessingConfig(
        source_db,
        target_db,
        (start_year, end_year),
        use_test_mode,
        analysis_params,
        prql_timeout,
        cleanup_temp_tables
    )
end

"""
    ensure_processed_db_structure(config::ProcessingConfig)

Ensure the processed database exists and has the required extensions installed.
"""
function ensure_processed_db_structure(config::ProcessingConfig)
    # Create directory if needed
    db_dir = dirname(config.target_db)
    if !isdir(db_dir)
        mkpath(db_dir)
    end

    # Connect and install extensions
    conn = DBInterface.connect(DuckDB.DB, config.target_db)
    try
        # Install required extensions
        DBInterface.execute(conn, "INSTALL 'prql' FROM community;")
        DBInterface.execute(conn, "LOAD 'prql';")
    finally
        DBInterface.close!(conn)
    end
end

"""
    process_year_complete(year::Int, config::ProcessingConfig) -> Dict

Process a complete year of data through all transformation steps.

Returns a dictionary with processing statistics.
"""
function process_year_complete(year::Int, config::ProcessingConfig)
    # Step 0: Validate configuration before processing
    if !step0_validate_configuration()
        error("Configuration validation failed. Please check the products.toml file for errors.")
    end

    # Step 1: Ensure product mapping table exists
    step1_ensure_product_mapping(config)

    # Step 2: Process unit conversions for PRODCOM data
    step2_process_unit_conversions(year, config)

    # Step 3: Extract and transform production data
    step3_process_production_data(year, config)

    # Step 4: Extract and transform trade data
    step4_process_trade_data(year, config)

    # Step 5: Create main circularity indicators table
    step5_create_circularity_indicators(year, config)

    # Step 6: Calculate country aggregates
    step6_create_country_aggregates(year, config)

    # Step 7: Calculate product aggregates
    step7_create_product_aggregates(year, config)

    # Step 8: Apply circularity parameters
    step8_apply_circularity_parameters(year, config)

    # Step 9: Clean up temporary tables
    step9_cleanup_temp_tables(year, config)
end

"""
    step0_validate_configuration()

Step 0: Validate the products.toml configuration file before processing.
Returns true if valid, false otherwise.
"""
function step0_validate_configuration()
    @info "Step 0: Validating products configuration..."
    return AnalysisConfigLoader.validate_product_config()
end

"""
    step1_ensure_product_mapping(config::ProcessingConfig)

Step 1: Ensure product mapping table and analysis parameters table exist in the processed database.
"""
function step1_ensure_product_mapping(config::ProcessingConfig)
    # Ensure product mapping table exists
    if !has_product_mapping_table(config.target_db)
        write_product_conversion_table(config.target_db)
    end

    # Ensure country code mapping table exists
    @info "Creating country code mapping table..."
    CountryCodeMapper.create_country_mapping_table(config.target_db)

    # Ensure parameter tables exist
    ensure_circularity_parameters_table(config)
end

"""
    step2_process_unit_conversions(year::Int, config::ProcessingConfig)

Step 2: Process unit conversions for PRODCOM data using PRQL query.
"""
function step2_process_unit_conversions(year::Int, config::ProcessingConfig)
    prql_path = joinpath(PRQL_DIR, "unit_conversion.prql")
    table_name = "prodcom_converted_$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.source_db,
        config.target_db,
        prql_query,
        table_name
    )
end

"""
    step3_process_production_data(year::Int, config::ProcessingConfig)

Step 3: Process production data using PRQL query.
"""
function step3_process_production_data(year::Int, config::ProcessingConfig)
    prql_path = joinpath(PRQL_DIR, "production_data.prql")
    table_name = "production_temp_$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.source_db,
        config.target_db,
        prql_query,
        table_name
    )
end

"""
    step4_process_trade_data(year::Int, config::ProcessingConfig)

Step 4: Process trade data using PRQL query.
"""
function step4_process_trade_data(year::Int, config::ProcessingConfig)
    prql_path = joinpath(PRQL_DIR, "trade_data.prql")
    table_name = "trade_temp_$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.source_db,
        config.target_db,
        prql_query,
        table_name
    )
end

"""
    step5_create_circularity_indicators(year::Int, config::ProcessingConfig)

Step 5: Create the main circularity indicators table by combining production and trade data.
"""
function step5_create_circularity_indicators(year::Int, config::ProcessingConfig)
    prql_path = joinpath(PRQL_DIR, "circularity_indicators.prql")
    table_name = "$(CIRCULARITY_TABLE_PREFIX)$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)

    # Replace year placeholder only
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.target_db,
        config.target_db,
        prql_query,
        table_name
    )
end

"""
    step6_create_country_aggregates(year::Int, config::ProcessingConfig)

Step 6: Create country-level aggregates table.
"""
function step6_create_country_aggregates(year::Int, config::ProcessingConfig)
    prql_path = joinpath(PRQL_DIR, "country_aggregates.prql")
    table_name = "$(COUNTRY_AGGREGATES_PREFIX)$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.target_db,
        config.target_db,
        prql_query,
        table_name
    )
end

"""
    step7_create_product_aggregates(year::Int, config::ProcessingConfig)

Step 7: Create product-level EU aggregates table.
"""
function step7_create_product_aggregates(year::Int, config::ProcessingConfig)
    prql_path = joinpath(PRQL_DIR, "product_aggregates.prql")
    table_name = "$(PRODUCT_AGGREGATES_PREFIX)$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.target_db,
        config.target_db,
        prql_query,
        table_name
    )
end

"""
    step8_apply_circularity_parameters(year::Int, config::ProcessingConfig)

Step 8: Apply external circularity parameters to update circularity rates and estimates.

TODO: Implement logic to:
1. Load product-specific circularity rates from analysis_params
2. Calculate material savings based on rates and apparent consumption
3. Calculate monetary savings based on material savings and unit values
4. Update the circularity indicators table
"""
function step8_apply_circularity_parameters(year::Int, config::ProcessingConfig)
    prql_path = joinpath(PRQL_DIR, "update_circularity_parameters.prql")
    table_name = "$(CIRCULARITY_TABLE_PREFIX)$(year)"
    temp_table = "$(table_name)_updated"

    # Read PRQL query template
    prql_query = read(prql_path, String)

    # Replace year placeholder only
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query to create updated table
    execute_prql_to_table(
        config.target_db,
        config.target_db,
        prql_query,
        temp_table
    )

    # Replace original table with updated one
    conn = DBInterface.connect(DuckDB.DB, config.target_db)
    DBInterface.execute(conn, "DROP TABLE IF EXISTS $table_name")
    DBInterface.execute(conn, "ALTER TABLE $temp_table RENAME TO $table_name")
    DBInterface.close!(conn)
end

"""
    step9_cleanup_temp_tables(year::Int, config::ProcessingConfig)

Step 9: Remove temporary tables created during processing.
"""
function step9_cleanup_temp_tables(year::Int, config::ProcessingConfig)
    # Check if cleanup is enabled
    if !config.cleanup_temp_tables
        @info "Step 9: Skipping cleanup of temporary tables (cleanup_temp_tables=false)"
        return
    end

    #@info "Step 9: Cleaning up temporary tables for year $year in database: $(config.target_db)"

    # List of temporary table suffixes
    temp_tables = [
        "prodcom_converted_$(year)",
        "production_temp_$(year)",
        "trade_temp_$(year)"
    ]

    conn = DBInterface.connect(DuckDB.DB, config.target_db)

    try
        # First, list all tables to see what exists
        all_tables_result = DBInterface.execute(conn, "SHOW TABLES") |> DataFrame
        #@info "Current tables in database: $(all_tables_result.name)"

        for table_name in temp_tables
            try
                # Check if table exists before dropping
                exists_result = DBInterface.execute(conn, "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '$table_name'") |> DataFrame
                table_exists = exists_result.cnt[1] > 0

                if table_exists
                    #@info "Attempting to drop temporary table: $table_name (exists=true)"
                    DBInterface.execute(conn, "DROP TABLE IF EXISTS $table_name")
                    #@info "Successfully dropped temporary table: $table_name"
                else
                    @info "Temporary table $table_name does not exist, skipping"
                end
            catch e
                @error "Failed to drop table $table_name" exception = e
                # Try to get more details about the error
                try
                    # Check if we can query the table
                    test_result = DBInterface.execute(conn, "SELECT COUNT(*) FROM $table_name LIMIT 1")
                    @warn "Table $table_name exists and is accessible but couldn't be dropped"
                catch query_err
                    @warn "Table $table_name may not exist or is not accessible: $query_err"
                end
            end
        end

        # List tables again after cleanup
        remaining_tables = DBInterface.execute(conn, "SHOW TABLES") |> DataFrame
        @info "Tables after cleanup: $(remaining_tables.name)"
        #@info "Cleanup completed for year $year"
    finally
        DBInterface.close!(conn)
    end
end

"""
    execute_prql_to_table(source_db::String, target_db::String, prql_query::String, output_table::String)

Execute a PRQL query and save results to a table.
"""
function execute_prql_to_table(source_db::String, target_db::String, prql_query::String, output_table::String)
    # Write PRQL to temporary file
    temp_prql = tempname() * ".prql"
    open(temp_prql, "w") do f
        write(f, prql_query)
    end

    try
        # Execute PRQL query using DatabaseAccess
        result_df = DatabaseAccess.executePRQL(source_db, temp_prql)

        # Write result to target database
        if !isnothing(result_df) && nrow(result_df) > 0
            DatabaseAccess.write_duckdb_table!(result_df, target_db, output_table)
        else
            error("PRQL query returned no results")
        end
    finally
        # Clean up temp file
        rm(temp_prql, force=true)
    end
end




"""
    ensure_circularity_parameters_table(config::ProcessingConfig)

Create or update the circularity parameters table with values from external parameters.
"""
function ensure_circularity_parameters_table(config::ProcessingConfig)
    # Extract circularity rates from config
    current_rates = get(config.analysis_params, "current_circularity_rates", Dict())
    potential_rates = get(config.analysis_params, "potential_circularity_rates", Dict())

    # Create DataFrame with product-specific rates
    product_codes = String[]
    current_rates_vec = Float64[]
    potential_rates_vec = Float64[]

    # Collect all product codes from both dictionaries
    all_products = union(keys(current_rates), keys(potential_rates))

    for product_code in all_products
        push!(product_codes, product_code)
        push!(current_rates_vec, get(current_rates, product_code, 0.0))
        push!(potential_rates_vec, get(potential_rates, product_code, 30.0))
    end

    # Create DataFrame with one row per product
    circularity_params_df = DataFrame(
        product_code=product_codes,
        current_circularity_rate=current_rates_vec,
        potential_circularity_rate=potential_rates_vec,
        last_updated=fill(Dates.format(now(), "yyyy-mm-dd HH:MM:SS.sss"), length(product_codes))
    )

    # Write to database
    DatabaseAccess.write_duckdb_table!(circularity_params_df, config.target_db, "parameters_circularity_rate")
    @info "Created/updated circularity parameters table with $(length(product_codes)) product-specific rates"

    # If there are recovery efficiency parameters, create a separate table
    recovery_efficiency = get(config.analysis_params, "recovery_efficiency", Dict())
    if !isempty(recovery_efficiency)
        ensure_recovery_efficiency_table(config, recovery_efficiency)
    end
end

"""
    ensure_recovery_efficiency_table(config::ProcessingConfig, recovery_efficiency::Dict)

Create or update the recovery efficiency parameters table.
"""
function ensure_recovery_efficiency_table(config::ProcessingConfig, recovery_efficiency::Dict)
    # Create DataFrame with recovery methods as rows
    methods = String[]
    efficiencies = Float64[]

    for (method, efficiency) in recovery_efficiency
        push!(methods, method)
        push!(efficiencies, efficiency)
    end

    recovery_params_df = DataFrame(
        recovery_method=methods,
        efficiency=efficiencies,
        last_updated=fill(Dates.format(now(), "yyyy-mm-dd HH:MM:SS.sss"), length(methods))
    )

    # Write to database
    DatabaseAccess.write_duckdb_table!(recovery_params_df, config.target_db, "parameters_recovery_efficiency")
    @info "Created/updated recovery efficiency parameters table with $(length(methods)) methods"
end

"""
    has_product_mapping_table(db_path::String) -> Bool

Check if product mapping table exists in the database.
"""
function has_product_mapping_table(db_path::String)
    return DatabaseAccess.table_exists(db_path, "product_mapping_codes")
end



"""
    process_all_years(config::ProcessingConfig) -> Dict

Process all years in the configured range.
"""
function process_all_years(config::ProcessingConfig)
    start_year, end_year = config.year_range

    # Validate configuration before processing any years
    if !step0_validate_configuration()
        error("Configuration validation failed. Please check the products.toml file for errors.")
    end

    # Ensure database structure
    ensure_processed_db_structure(config)

    for year in start_year:end_year
        process_year_complete(year, config)
    end
end

# Export public functions
export ProcessingConfig,
    create_processing_config,
    process_year_complete,
    process_all_years,
    ensure_processed_db_structure,
    step0_validate_configuration,
    step1_ensure_product_mapping,
    step2_process_unit_conversions,
    step3_process_production_data,
    step4_process_trade_data,
    step5_create_circularity_indicators,
    step6_create_country_aggregates,
    step7_create_product_aggregates,
    step8_apply_circularity_parameters,
    step9_cleanup_temp_tables

end # module DataProcessor
