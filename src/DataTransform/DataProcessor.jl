module DataProcessor

using DataFrames
using DuckDB, DBInterface
using Dates
using Dates: now, format
using ..DatabaseAccess
using ..AnalysisConfigLoader
using ..CountryCodeMapper
using DBInterface

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
- `end_year`: Ending year for processing (default: 2024)
- `use_test_mode`: Use test database with only 2002 data (default: false)
- `analysis_params`: Analysis parameters for circularity calculations
- `prql_timeout`: Timeout for PRQL queries in seconds (default: 300)
- `cleanup_temp_tables`: Whether to remove temporary tables after processing (default: true)
"""
function create_processing_config(;
    source_db::String="",
    target_db::String="",
    start_year::Int=2002,
    end_year::Int=2024,
    use_test_mode::Bool=false,
    analysis_params::Dict{String,Any}=Dict{String,Any}(),
    prql_timeout::Int=300,
    cleanup_temp_tables::Bool=true
)
    # Determine database paths
    if isempty(source_db)
        source_db = use_test_mode ?
                    "CirQuant-database/raw/test.duckdb" :
                    "CirQuant-database/raw/CirQuant_2002-2024.duckdb"
    end

    if isempty(target_db)
        target_db = use_test_mode ?
                    "CirQuant-database/processed/test_processed.duckdb" :
                    "CirQuant-database/processed/CirQuant_2002-2024.duckdb"
    end

    # Use only 2002 if in test mode
    if use_test_mode
        start_year = 2024
        end_year = 2024
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

    # Open a single connection for all operations
    db_conn = DuckDB.DB(config.target_db)
    target_conn = DBInterface.connect(db_conn)

    try
        # Step 1: Ensure product mapping table exists
        step1_ensure_product_mapping(config, target_conn)

        # Step 2: Process unit conversions for PRODCOM data
        step2_process_unit_conversions(year, config, target_conn)

        # Step 3: Extract and transform production data
        step3_process_production_data(year, config, target_conn)

        # Step 4: Extract and transform trade data
        # # Step 4b: Harmonize production and trade data using mappings
        # # Step 4c: Fill in PRODCOM trade data where COMEXT is zero
        step4_process_trade_data(year, config, target_conn)

        # Step 5: Create main circularity indicators table
        step5_create_circularity_indicators(year, config, target_conn)

        # Step 6: Calculate country aggregates
        step6_create_country_aggregates(year, config, target_conn)

        # Step 7: Calculate product aggregates
        step7_create_product_aggregates(year, config, target_conn)

        # Step 8: Apply circularity parameters
        # COMMENTED OUT - focusing on product/geo matching first
        step8_apply_circularity_parameters(year, config, target_conn)

        # Step 9: Clean up temporary tables
        step9_cleanup_temp_tables(year, config, target_conn)

        # Close the connection properly
        DBInterface.close!(target_conn)
        DBInterface.close!(db_conn)

    catch e
        # Make sure to close connection on error
        try
            DBInterface.close!(target_conn)
            DBInterface.close!(db_conn)
        catch
        end
        rethrow(e)
    end
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
    step1_ensure_product_mapping(config::ProcessingConfig, conn::DuckDB.Connection)

Step 1: Ensure product mapping and parameter tables exist.
"""
function step1_ensure_product_mapping(config::ProcessingConfig, conn::DuckDB.Connection)
    @info "Step 1: Ensuring mapping tables exist..."
    # Ensure product mapping table exists
    if !has_product_mapping_table(config.target_db)
        write_product_conversion_table_with_connection(conn)
    end

    # Ensure country code mapping table exists
    #@info "Creating country code mapping table..."
    CountryCodeMapper.create_country_mapping_table_with_connection(conn)

    # Ensure parameter tables exist
    ensure_circularity_parameters_table_with_connection(config, conn)
end

"""
    step2_process_unit_conversions(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)

Step 2: Convert PRODCOM units to tonnes for consistency.
Creates temporary table: prodcom_converted_YYYY
"""
function step2_process_unit_conversions(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)
    @info "Step 2: Converting units for quantities and volumes..."
    prql_path = joinpath(PRQL_DIR, "unit_conversion.prql")
    table_name = "prodcom_converted_$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.source_db,
        conn,
        prql_query,
        table_name
    )
end

"""
    step3_process_production_data(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)

Step 3: Extract and transform production data from PRODCOM.
Creates temporary table: production_temp_YYYY
"""
function step3_process_production_data(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)
    @info "Step 3: Processing production values and volumes..."
    prql_path = joinpath(PRQL_DIR, "production_data.prql")
    table_name = "production_temp_$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.source_db,
        conn,
        prql_query,
        table_name
    )
end

"""
    step4_process_trade_data(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)

Step 4: Extract and transform trade data from COMEXT (and PRODCOM as secondary source).
Creates temporary table: trade_temp_YYYY
"""
function step4_process_trade_data(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)
    @info "Step 4: Processing trade values and volumes..."
    prql_path = joinpath(PRQL_DIR, "trade_data.prql")
    table_name = "trade_temp_$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.source_db,
        conn,
        prql_query,
        table_name
    )
    step4b_harmonize_production_trade_data(year, config, conn)
    step4c_fill_prodcom_trade_fallback(year, config, conn)
end

"""
    step4b_harmonize_production_trade_data(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Harmonize production and trade data using country and product mappings.
Merges production_temp_YYYY and trade_temp_YYYY tables using the mapping tables.
Creates temporary table: production_trade_harmonized_YYYY
"""
function step4b_harmonize_production_trade_data(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    #@info "Harmonizing production and trade data for year $year..."

    # Load mapping tables from processed database using existing connection
    # Declare variables outside try block
    local country_mapping_df, product_mapping_df

    try
        # Load country mapping
        country_mapping_query = "SELECT prodcom_code, iso_code, country_name FROM country_code_mapping"
        country_mapping_df = DataFrame(DBInterface.execute(target_conn, country_mapping_query))

        # Load product mapping
        product_mapping_query = "SELECT product_id, product, prodcom_code, hs_codes FROM product_mapping_codes"
        product_mapping_df = DataFrame(DBInterface.execute(target_conn, product_mapping_query))

    catch e
        @error "Failed to load mapping tables" exception = e
        rethrow(e)
    end

    # Load intermediate tables using existing connection
    # Declare variables outside try block
    local production_df, trade_df

    try
        # Load production data
        production_query = "SELECT * FROM production_temp_$year"
        production_df = DataFrame(DBInterface.execute(target_conn, production_query))

        # Load trade data
        trade_query = "SELECT * FROM trade_temp_$year"
        trade_df = DataFrame(DBInterface.execute(target_conn, trade_query))

    catch e
        @error "Failed to load intermediate tables" exception = e
        rethrow(e)
    end

    # Harmonize country codes in production data
    production_harmonized = leftjoin(
        production_df,
        country_mapping_df,
        on=:geo => :prodcom_code,
        makeunique=true
    )

    # Use harmonized country code, fallback to original if no mapping
    production_harmonized.geo_harmonized = coalesce.(
        production_harmonized.iso_code,
        production_harmonized.geo
    )

    # Select relevant columns and rename
    select!(production_harmonized,
        :product_code => :prodcom_code,
        :year,
        :geo_harmonized => :geo,
        :level,
        :production_volume_tonnes,
        :production_value_eur
    )

    # For trade data, we need to expand based on product mappings
    # COMEXT uses HS codes, we need to map them to PRODCOM codes
    # Initialize with proper schema
    trade_expanded = DataFrame(
        prodcom_code=String[],
        year=Int[],
        geo=String[],
        level=String[],
        import_volume_tonnes=Float64[],
        import_value_eur=Float64[],
        export_volume_tonnes=Float64[],
        export_value_eur=Float64[],
        data_source=String[]
    )

    for row in eachrow(trade_df)
        product_code = row.product_code
        data_source = hasproperty(row, :data_source) ? row.data_source : "COMEXT"

        if data_source == "PRODCOM"
            # PRODCOM trade data already has PRODCOM codes - just normalize by removing dots
            push!(trade_expanded, (
                prodcom_code=replace(String(product_code), "." => ""),
                year=parse(Int, string(row.year)),
                geo=String(row.geo),  # PRODCOM uses numeric codes, will be mapped later
                level=String(row.level),
                import_volume_tonnes=Float64(row.import_volume_tonnes),
                import_value_eur=Float64(row.import_value_eur),
                export_volume_tonnes=Float64(row.export_volume_tonnes),
                export_value_eur=Float64(row.export_value_eur),
                data_source=data_source
            ))
        else
            # COMEXT data - need to map HS codes to PRODCOM codes
            hs_code = product_code

            # Find all PRODCOM codes that include this HS code
            matching_products = filter(product_mapping_df) do pm_row
                # Check if the HS code is in the comma-separated list
                hs_codes_list = ismissing(pm_row.hs_codes) ? "" : pm_row.hs_codes

                # Normalize HS codes by removing dots for comparison
                # COMEXT uses "841869" while mapping uses "8418.69"
                normalized_hs_code = replace(hs_code, "." => "")
                normalized_hs_list = replace(hs_codes_list, "." => "")

                contains_hs = occursin(normalized_hs_code, normalized_hs_list)
                return contains_hs
            end

            if nrow(matching_products) > 0
                # Create a row for each matching PRODCOM code
                for prod_match in eachrow(matching_products)
                    push!(trade_expanded, (
                        prodcom_code=replace(String(prod_match.prodcom_code), "." => ""),
                        year=parse(Int, string(row.year)),
                        geo=String(row.geo),  # COMEXT already uses ISO codes
                        level=String(row.level),
                        import_volume_tonnes=Float64(row.import_volume_tonnes),
                        import_value_eur=Float64(row.import_value_eur),
                        export_volume_tonnes=Float64(row.export_volume_tonnes),
                        export_value_eur=Float64(row.export_value_eur),
                        data_source=data_source
                    ))
                end
            else
                # No mapping found - keep with original code but flag it
                @warn "No PRODCOM mapping found for HS code: $hs_code"
            end
        end
    end

    # Now we can merge production and trade data on harmonized codes
    merged_data = outerjoin(
        production_harmonized,
        trade_expanded,
        on=[:prodcom_code, :geo, :year],
        makeunique=true
    )

    # Clean up the merged data
    merged_data.product_code = merged_data.prodcom_code
    # Ensure year is Int - handle both String and Int inputs
    if eltype(merged_data.year) <: AbstractString
        merged_data.year = parse.(Int, merged_data.year)
    elseif !(eltype(merged_data.year) <: Integer)
        merged_data.year = convert.(Int, merged_data.year)
    end
    merged_data.production_volume_tonnes = coalesce.(merged_data.production_volume_tonnes, 0.0)
    merged_data.production_value_eur = coalesce.(merged_data.production_value_eur, 0.0)
    merged_data.import_volume_tonnes = coalesce.(merged_data.import_volume_tonnes, 0.0)
    merged_data.import_value_eur = coalesce.(merged_data.import_value_eur, 0.0)
    merged_data.export_volume_tonnes = coalesce.(merged_data.export_volume_tonnes, 0.0)
    merged_data.export_value_eur = coalesce.(merged_data.export_value_eur, 0.0)
    merged_data.level = coalesce.(merged_data.level, merged_data.level_1, "country")

    # Select final columns
    select!(merged_data,
        :product_code,
        :year,
        :geo,
        :level,
        :production_volume_tonnes,
        :production_value_eur,
        :import_volume_tonnes,
        :import_value_eur,
        :export_volume_tonnes,
        :export_value_eur
    )

    # Write harmonized data back to processed database using existing connection
    try
        table_name = "production_trade_harmonized_$year"

        # Drop table if exists
        DBInterface.execute(target_conn, "DROP TABLE IF EXISTS $table_name")

        # Create new table with harmonized data using existing connection
        DatabaseAccess.write_duckdb_table_with_connection!(merged_data, target_conn, table_name)

        #@info "Created harmonized table '$table_name' with $(nrow(merged_data)) records"

    catch e
        @error "Failed to write harmonized data" exception = e
        rethrow(e)
    end

    return merged_data
end

"""
    step4c_fill_prodcom_trade_fallback(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Apply PRODCOM trade data as fallback where COMEXT has zero values.
Reads from production_trade_harmonized_YYYY and trade_temp_YYYY (PRODCOM records).
Creates final table: production_trade_YYYY
"""
function step4c_fill_prodcom_trade_fallback(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    #@info "Creating production_trade_$year table with PRODCOM fallback data..."

    final_table = "production_trade_$year"
    harmonized_table = "production_trade_harmonized_$year"

    try
        # Load harmonized data
        harmonized_df = DataFrame(DBInterface.execute(target_conn, "SELECT * FROM $harmonized_table"))

        # Get PRODCOM trade data from trade_temp table (already in processed database)
        prodcom_sql = """
            SELECT
                product_code,
                geo,
                import_volume_tonnes,
                import_value_eur,
                export_volume_tonnes,
                export_value_eur
            FROM trade_temp_$year
            WHERE data_source = 'PRODCOM'
                AND (import_volume_tonnes > 0 OR import_value_eur > 0
                     OR export_volume_tonnes > 0 OR export_value_eur > 0)
        """

        prodcom_df = DataFrame(DBInterface.execute(target_conn, prodcom_sql))

        if nrow(prodcom_df) > 0
            # Apply fallback values to harmonized data
            for row in eachrow(harmonized_df)
                # Find matching PRODCOM data
                prodcom_match = filter(r -> r.product_code == row.product_code &&
                        r.geo == row.geo, prodcom_df)

                if nrow(prodcom_match) > 0
                    p = prodcom_match[1, :]
                    # Replace zeros with PRODCOM values
                    if row.import_volume_tonnes == 0 && p.import_volume_tonnes > 0
                        row.import_volume_tonnes = p.import_volume_tonnes
                    end
                    if row.import_value_eur == 0 && p.import_value_eur > 0
                        row.import_value_eur = p.import_value_eur
                    end
                    if row.export_volume_tonnes == 0 && p.export_volume_tonnes > 0
                        row.export_volume_tonnes = p.export_volume_tonnes
                    end
                    if row.export_value_eur == 0 && p.export_value_eur > 0
                        row.export_value_eur = p.export_value_eur
                    end
                end
            end

            #@info "Applied PRODCOM fallback data to $(nrow(prodcom_match)) records"
        end

        # Write final table
        DBInterface.execute(target_conn, "DROP TABLE IF EXISTS $final_table")
        DatabaseAccess.write_duckdb_table_with_connection!(harmonized_df, target_conn, final_table)

        #@info "Created $final_table with $(nrow(harmonized_df)) records"

    catch e
        @error "Failed to create production_trade table" exception = e
        rethrow(e)
    end
end

"""
    step5_create_circularity_indicators(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Step 5: Create the main circularity indicators table.
Reads from production_trade_YYYY and adds circularity parameters.
Creates table: circularity_indicators_YYYY
"""
function step5_create_circularity_indicators(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Step 5: Computing circularity indicators..."
    prql_path = joinpath(PRQL_DIR, "circularity_indicators.prql")
    table_name = "$(CIRCULARITY_TABLE_PREFIX)$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)

    # Replace year placeholder only
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.target_db,
        target_conn,
        prql_query,
        table_name
    )
end

"""
    step6_create_country_aggregates(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Step 6: Create country-level aggregates table.
Reads from circularity_indicators_YYYY.
Creates table: country_aggregates_YYYY
"""
function step6_create_country_aggregates(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Step 6: Aggregating data by country..."
    prql_path = joinpath(PRQL_DIR, "country_aggregates.prql")
    table_name = "$(COUNTRY_AGGREGATES_PREFIX)$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.target_db,
        target_conn,
        prql_query,
        table_name
    )
end

"""
    step7_create_product_aggregates(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Step 7: Create product-level EU aggregates table.
Reads from circularity_indicators_YYYY.
Creates table: product_aggregates_YYYY
"""
function step7_create_product_aggregates(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Step 7: Aggregating data by product..."
    prql_path = joinpath(PRQL_DIR, "product_aggregates.prql")
    table_name = "$(PRODUCT_AGGREGATES_PREFIX)$(year)"

    # Read PRQL query template
    prql_query = read(prql_path, String)
    prql_query = replace(prql_query, "{{YEAR}}" => string(year))

    # Execute PRQL query
    execute_prql_to_table(
        config.target_db,
        target_conn,
        prql_query,
        table_name
    )
end

"""
    step8_apply_circularity_parameters(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Step 8: Apply external circularity parameters to update circularity rates and estimates.

TODO: Implement logic to:
1. Load product-specific circularity rates from analysis_params
2. Calculate material savings based on rates and apparent consumption
3. Calculate monetary savings based on material savings and unit values
4. Update the circularity indicators table
"""
function step8_apply_circularity_parameters(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Step 8: Applying circulariy parameters..."
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
        target_conn,
        prql_query,
        temp_table
    )

    # Replace original table with updated one
    #conn = DBInterface.connect(DuckDB.DB, config.target_db)
    DBInterface.execute(target_conn, "DROP TABLE IF EXISTS $table_name")
    DBInterface.execute(target_conn, "ALTER TABLE $temp_table RENAME TO $table_name")
    #DBInterface.close!(conn)
end

"""
    step9_cleanup_temp_tables(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Step 9: Clean up temporary tables created during processing.
Deletes: prodcom_converted_YYYY, production_temp_YYYY, trade_temp_YYYY, production_trade_harmonized_YYYY
"""
function step9_cleanup_temp_tables(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Step 9: Cleaning up temporary tables..."
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
        "trade_temp_$(year)",
        "production_trade_harmonized_$(year)"
    ]

    #conn = DBInterface.connect(DuckDB.DB, config.target_db)

    try
        # First, list all tables to see what exists
        all_tables_result = DBInterface.execute(target_conn, "SHOW TABLES") |> DataFrame
        #@info "Current tables in database: $(all_tables_result.name)"

        for table_name in temp_tables
            try
                # Check if table exists before dropping
                exists_result = DBInterface.execute(target_conn, "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '$table_name'") |> DataFrame
                table_exists = exists_result.cnt[1] > 0

                if table_exists
                    #@info "Attempting to drop temporary table: $table_name (exists=true)"
                    DBInterface.execute(target_conn, "DROP TABLE IF EXISTS $table_name")
                    #@info "Successfully dropped temporary table: $table_name"
                else
                    @info "Temporary table $table_name does not exist, skipping"
                end
            catch e
                @error "Failed to drop table $table_name" exception = e
                # Try to get more details about the error
                try
                    # Check if we can query the table
                    test_result = DBInterface.execute(target_conn, "SELECT COUNT(*) FROM $table_name LIMIT 1")
                    @warn "Table $table_name exists and is accessible but couldn't be dropped"
                catch query_err
                    @warn "Table $table_name may not exist or is not accessible: $query_err"
                end
            end
        end

        # List tables again after cleanup
        remaining_tables = DBInterface.execute(target_conn, "SHOW TABLES") |> DataFrame
        @info "Tables after cleanup: $(remaining_tables.name)"
        #@info "Cleanup completed for year $year"
    catch e
        @error "Failed to get table list" exception = e
    end
end

"""
    execute_prql_to_table(source_db::String, target_conn::DuckDB.Connection, prql_query::String, output_table::String)

Execute a PRQL query and save results to a table using an existing connection.
"""
function execute_prql_to_table(source_db::String, target_conn::DuckDB.Connection, prql_query::String, output_table::String)
    # Write PRQL to temporary file
    temp_prql = tempname() * ".prql"
    open(temp_prql, "w") do f
        write(f, prql_query)
    end

    try
        # Ensure any pending writes are visible to the PRQL query execution
        DBInterface.execute(target_conn, "CHECKPOINT")

        # Execute PRQL query using DatabaseAccess
        result_df = DatabaseAccess.executePRQL(source_db, temp_prql)

        # Write result to target database using existing connection
        if !isnothing(result_df) && nrow(result_df) > 0
            DatabaseAccess.write_duckdb_table_with_connection!(result_df, target_conn, output_table)
        else
            error("PRQL query returned no results")
        end
    finally
        # Clean up temp file
        rm(temp_prql, force=true)
    end
end




"""
    ensure_circularity_parameters_table_with_connection(config::ProcessingConfig, conn::DuckDB.Connection)

Create or update the circularity parameters table from ANALYSIS_PARAMETERS.
"""
function ensure_circularity_parameters_table_with_connection(config::ProcessingConfig, conn::DuckDB.Connection)
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
    DatabaseAccess.write_duckdb_table_with_connection!(circularity_params_df, conn, "parameters_circularity_rate")
    #@info "Created/updated circularity parameters table with $(length(product_codes)) product-specific rates"

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
    #@info "Created/updated recovery efficiency parameters table with $(length(methods)) methods"
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
