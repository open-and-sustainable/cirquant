module DataProcessor

using DataFrames
using DuckDB, DBInterface
using Dates
using ..DatabaseAccess
using ..ProductConversionTables
using ..CircularityProcessor

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

"""
    ProcessingConfig

Configuration for data processing pipeline
"""
struct ProcessingConfig
    source_db::String  # Path to raw database
    target_db::String  # Path to processed database
    year_range::Tuple{Int, Int}
    use_test_mode::Bool
    external_params::Dict{String, Any}
    prql_timeout::Int  # Timeout for PRQL queries in seconds
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
- `external_params`: External parameters for circularity calculations
- `prql_timeout`: Timeout for PRQL queries in seconds (default: 300)
"""
function create_processing_config(;
    source_db::String = "",
    target_db::String = "",
    start_year::Int = 2002,
    end_year::Int = 2023,
    use_test_mode::Bool = false,
    external_params::Dict{String, Any} = Dict{String, Any}(),
    prql_timeout::Int = 300
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
        external_params,
        prql_timeout
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
    # Step 1: Ensure product mapping table exists
    step1_ensure_product_mapping(config)

    # Step 2: Process unit conversions for PRODCOM data
    step2_process_unit_conversions(year, config)
    #=
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

    # Step 9: Data quality checks
    step9_run_data_quality_checks(year, config)
    =#
end

"""
    step1_ensure_product_mapping(config::ProcessingConfig)

Step 1: Ensure product mapping table exists in the processed database.
"""
function step1_ensure_product_mapping(config::ProcessingConfig)
    if !has_product_mapping_table(config.target_db)
        write_product_conversion_table(config.target_db)
    end
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
    table_name = "$(CIRCULARITY_TABLE_PREFIX)$(year)"

    conn = DBInterface.connect(DuckDB.DB, config.target_db)

    # TODO: Implement proper product mapping between PRODCOM and HS codes
    # For now, create a simplified version
    query = """
    CREATE OR REPLACE TABLE $table_name AS
    WITH production AS (
        SELECT * FROM production_temp_$(year)
    ),
    trade AS (
        SELECT * FROM trade_temp_$(year)
    ),
    combined AS (
        SELECT
            COALESCE(p.product_code, t.product_code) as product_code,
            COALESCE(p.year, t.year) as year,
            COALESCE(p.geo, t.geo) as geo,
            COALESCE(p.level, t.level) as level,
            p.production_volume_tonnes,
            p.production_value_eur,
            t.import_volume_tonnes,
            t.import_value_eur,
            t.export_volume_tonnes,
            t.export_value_eur
        FROM production p
        FULL OUTER JOIN trade t
            ON p.product_code = t.product_code
            AND p.geo = t.geo
    )
    SELECT
        *,
        -- Calculate apparent consumption
        COALESCE(production_volume_tonnes, 0) +
        COALESCE(import_volume_tonnes, 0) -
        COALESCE(export_volume_tonnes, 0) as apparent_consumption_tonnes,

        COALESCE(production_value_eur, 0) +
        COALESCE(import_value_eur, 0) -
        COALESCE(export_value_eur, 0) as apparent_consumption_value_eur,

        -- Placeholder circularity rates (to be updated with external parameters)
        0.0 as current_circularity_rate_pct,
        30.0 as potential_circularity_rate_pct,
        0.0 as estimated_material_savings_tonnes,
        0.0 as estimated_monetary_savings_eur
    FROM combined
    """

    DBInterface.execute(conn, query)
    DBInterface.close!(conn)
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
1. Load product-specific circularity rates from external_params
2. Calculate material savings based on rates and apparent consumption
3. Calculate monetary savings based on material savings and unit values
4. Update the circularity indicators table
"""
function step8_apply_circularity_parameters(year::Int, config::ProcessingConfig)
    table_name = "$(CIRCULARITY_TABLE_PREFIX)$(year)"

    conn = DBInterface.connect(DuckDB.DB, config.target_db)

    # Get circularity rates from external parameters
    current_rates = get(config.external_params, "current_circularity_rates", Dict())
    potential_rates = get(config.external_params, "potential_circularity_rates", Dict())
    default_current = get(current_rates, "default", 0.0)
    default_potential = get(potential_rates, "default", 30.0)

    # TODO: Implement product-specific rate application
    # For now, apply default rates
    query = """
    UPDATE $table_name
    SET
        current_circularity_rate_pct = $default_current,
        potential_circularity_rate_pct = $default_potential,
        estimated_material_savings_tonnes =
            apparent_consumption_tonnes * (potential_circularity_rate_pct - current_circularity_rate_pct) / 100,
        estimated_monetary_savings_eur =
            apparent_consumption_value_eur * (potential_circularity_rate_pct - current_circularity_rate_pct) / 100
    WHERE apparent_consumption_tonnes > 0
    """

    DBInterface.execute(conn, query)
    DBInterface.close!(conn)
end

"""
    step9_run_data_quality_checks(year::Int, config::ProcessingConfig)

Step 9: Run data quality checks on processed data.

TODO: Implement comprehensive quality checks:
1. Check for negative apparent consumption
2. Validate unit values are within reasonable ranges
3. Check data completeness
4. Identify outliers
5. Validate trade balance consistency
"""
function step9_run_data_quality_checks(year::Int, config::ProcessingConfig)
    # TODO: Implement quality checks
    # For now, this is a placeholder
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
    has_product_mapping_table(db_path::String) -> Bool

Check if product mapping table exists in the database.
"""
function has_product_mapping_table(db_path::String)
    conn = DBInterface.connect(DuckDB.DB, db_path)
    try
        result = DBInterface.execute(conn, """
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_name = 'product_mapping_codes'
        """) |> DataFrame

        return first(result).cnt > 0
    finally
        DBInterface.close!(conn)
    end
end



"""
    process_all_years(config::ProcessingConfig) -> Dict

Process all years in the configured range.
"""
function process_all_years(config::ProcessingConfig)
    start_year, end_year = config.year_range

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
       step1_ensure_product_mapping,
       step2_process_unit_conversions,
       step3_process_production_data,
       step4_process_trade_data,
       step5_create_circularity_indicators,
       step6_create_country_aggregates,
       step7_create_product_aggregates,
       step8_apply_circularity_parameters,
       step9_run_data_quality_checks

end # module DataProcessor
