module DataProcessor

using DataFrames
using DuckDB, DBInterface
using Dates
using Dates: now, format
using Statistics: mean
using ..ProductWeightsBuilder
using ..DatabaseAccess
using ..AnalysisConfigLoader
using ..CountryCodeMapper
using ..UmpDataFetch
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
const PRODUCT_UNIT_VALUES_PREFIX = "product_unit_values_"
const ANALYSIS_PARAMS_TABLE = "analysis_parameters"
const UMP_COMPOSITION_FLOW_ID = "WEEE_categ_mechRec1"
const UMP_RECOVERY_FLOW_IDS = [
    "WEEE_2RM_mechRec1Other_other",
    "WEEE_2RM_mechRec2Other_other",
    "WEEE_2RM_mechRec2Smelter_AlScrap",
    "WEEE_2RM_mechRec2Smelter_CuScrap",
    "WEEE_2RM_mechRec2Smelter_ferrousScrap"
]
const UMP_LOSS_FLOW_ID = "WEEE_mechRec2_Landfill_or_Dissipated"
const UMP_GEO_FALLBACK = "EU27_2020"
const UMP_WEEE_CODE_MAP = UmpDataFetch.UMP_WEEE_CODE_MAP
const COLLECTION_WST_OPER = Set(["COL", "COL_HH", "COL_OTH"])
const COLLECTION_UNITS = Set(["PC", "AVG_3Y"])

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

        # Step 4d: Build material composition and recovery rate tables from UMP sankey data
        step4d_build_material_recovery_rates(year, config, target_conn)

        # Step 4e: Build product collection rates from Eurostat WEEE datasets
        step4e_build_collection_rates(year, config, target_conn)

        # Step 5: Create main circularity indicators table
        step5_create_circularity_indicators(year, config, target_conn)

        # Step 6: Calculate country aggregates
        step6_create_country_aggregates(year, config, target_conn)

        # Step 7: Calculate product aggregates
        step7_create_product_aggregates(year, config, target_conn)

    # Step 8: Apply circularity parameters
    # COMMENTED OUT - focusing on product/geo matching first
    step8_apply_circularity_parameters(year, config, target_conn)

    # Step 8b: Build product weights table using config weights and derived mass/counts
    step8b_build_product_weights(year, config, target_conn)

    # Step 8c: Build unit value table (EUR per unit/kg) using product weights and trade/production values
    step8c_build_unit_values(year, config, target_conn)

    # Step 8d: Build strategy-specific circularity indicators
    step8d_create_strategy_indicators(year, config, target_conn)

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
        product_mapping_query = """
            SELECT product_id, product, prodcom_code, prodcom_code_clean,
                   hs_codes, prodcom_epoch, epoch_start_year, epoch_end_year
            FROM product_mapping_codes
        """
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

        year_value = parse(Int, string(row.year))

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
                epoch_start = hasproperty(pm_row, :epoch_start_year) ? pm_row.epoch_start_year : missing
                epoch_end = hasproperty(pm_row, :epoch_end_year) ? pm_row.epoch_end_year : missing
                year_in_range = true
                if !(epoch_start === missing)
                    year_in_range &= year_value >= Int(epoch_start)
                end
                if !(epoch_end === missing)
                    year_in_range &= year_value <= Int(epoch_end)
                end

                return contains_hs && year_in_range
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
    step4d_build_material_recovery_rates(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Build material composition and material recovery rate tables from UMP sankey data.
Creates:
- product_material_composition_YYYY
- material_recycling_rates_YYYY
- product_material_recovery_rates_YYYY
"""
function step4d_build_material_recovery_rates(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Step 4d: Building material composition and recovery rates from UMP..."

    if !DatabaseAccess.table_exists(config.source_db, "ump_weee_sankey")
        @warn "UMP sankey table not found in raw DB; skipping material recovery rates"
        return
    end

    # Load UMP sankey data for the year (fallback to previous year if missing)
    raw_conn = DBInterface.connect(DuckDB.DB(config.source_db))
    sankey_df = DataFrame()
    fallback_year = nothing
    try
        sankey_df = DataFrame(DBInterface.execute(raw_conn, """
            SELECT year, location, layer_1, layer_4, stock_flow_id, value, unit, scenario
            FROM ump_weee_sankey
            WHERE year = $year
              AND (scenario IS NULL OR scenario = 'OBS')
        """))
        if nrow(sankey_df) == 0
            fallback = DataFrame(DBInterface.execute(raw_conn, """
                SELECT MAX(year) AS year
                FROM ump_weee_sankey
                WHERE year < $year
                  AND (scenario IS NULL OR scenario = 'OBS')
            """))
            if nrow(fallback) > 0 && !ismissing(fallback.year[1])
                fallback_year = Int(fallback.year[1])
                sankey_df = DataFrame(DBInterface.execute(raw_conn, """
                    SELECT year, location, layer_1, layer_4, stock_flow_id, value, unit, scenario
                    FROM ump_weee_sankey
                    WHERE year = $fallback_year
                      AND (scenario IS NULL OR scenario = 'OBS')
                """))
            end
        end
    finally
        DBInterface.close!(raw_conn)
    end

    if nrow(sankey_df) == 0
        @warn "No UMP sankey rows found for year $year; creating empty material recovery tables"
        _write_empty_material_tables(year, target_conn)
        return
    end

    if fallback_year !== nothing
        @warn "Using UMP sankey data from $fallback_year for year $year"
        sankey_df.year .= year
    end

    # Keep only material-level rows with WEEE categories
    sankey_df = filter(row ->
        !ismissing(row.layer_1) &&
        startswith(String(row.layer_1), "WEEE_Cat") &&
        !ismissing(row.layer_4) &&
        !isempty(String(row.layer_4)),
        sankey_df
    )

    if nrow(sankey_df) == 0
        @warn "UMP sankey data has no material rows for year $year; creating empty material recovery tables"
        _write_empty_material_tables(year, target_conn)
        return
    end

    sankey_df.weee_category = String.(sankey_df.layer_1)
    sankey_df.material = String.(sankey_df.layer_4)

    # Build category-level material composition from UMP
    comp_rows = filter(:stock_flow_id => (s -> s == UMP_COMPOSITION_FLOW_ID), sankey_df)
    if nrow(comp_rows) == 0
        @warn "UMP sankey lacks composition flow $UMP_COMPOSITION_FLOW_ID for year $year; creating empty material recovery tables"
        _write_empty_material_tables(year, target_conn)
        return
    end

    comp_by_cat = combine(
        groupby(comp_rows, [:year, :weee_category, :material]),
        :value => sum => :material_mass_mg
    )

    cat_totals = combine(
        groupby(comp_by_cat, [:year, :weee_category]),
        :material_mass_mg => sum => :category_mass_mg
    )

    comp_by_cat = leftjoin(comp_by_cat, cat_totals, on=[:year, :weee_category])
    comp_by_cat.material_weight_pct = ifelse.(
        comp_by_cat.category_mass_mg .> 0,
        comp_by_cat.material_mass_mg ./ comp_by_cat.category_mass_mg .* 100.0,
        missing
    )

    # Build material recovery rates by category/material from UMP flows
    recovered_rows = filter(:stock_flow_id => (s -> s in UMP_RECOVERY_FLOW_IDS), sankey_df)
    lost_rows = filter(:stock_flow_id => (s -> s == UMP_LOSS_FLOW_ID), sankey_df)

    recovered = combine(
        groupby(recovered_rows, [:year, :weee_category, :material]),
        :value => sum => :recovered_mass_mg
    )
    lost = combine(
        groupby(lost_rows, [:year, :weee_category, :material]),
        :value => sum => :lost_mass_mg
    )

    recovery_rates = outerjoin(recovered, lost, on=[:year, :weee_category, :material])
    recovery_rates.recovered_mass_mg = coalesce.(recovery_rates.recovered_mass_mg, 0.0)
    recovery_rates.lost_mass_mg = coalesce.(recovery_rates.lost_mass_mg, 0.0)
    denom = recovery_rates.recovered_mass_mg .+ recovery_rates.lost_mass_mg
    recovery_rates.recovery_rate_pct = ifelse.(denom .> 0, recovery_rates.recovered_mass_mg ./ denom .* 100.0, missing)

    recovery_rates.geo = fill(UMP_GEO_FALLBACK, nrow(recovery_rates))
    recovery_rates.source = fill("UMP_sankey", nrow(recovery_rates))

    # Map products to WEEE categories
    mapping_df = DataFrame(DBInterface.execute(target_conn, """
        SELECT DISTINCT prodcom_code_clean AS product_code, weee_waste_codes
        FROM product_mapping_codes
    """))

    product_weee = DataFrame(product_code=String[], weee_category=String[])
    for row in eachrow(mapping_df)
        codes_str = row.weee_waste_codes
        if ismissing(codes_str) || isempty(String(codes_str))
            continue
        end
        codes = split(String(codes_str), ",")
        for code in codes
            mapped = get(UMP_WEEE_CODE_MAP, strip(code), nothing)
            mapped === nothing && continue
            push!(product_weee, (product_code=String(row.product_code), weee_category=mapped))
        end
    end

    if nrow(product_weee) == 0
        @warn "No product-to-WEEE category mapping found; creating empty material recovery tables"
        _write_empty_material_tables(year, target_conn)
        return
    end

    # Product material composition (aggregate across mapped WEEE categories)
    prod_material_by_cat = innerjoin(comp_by_cat, product_weee, on=:weee_category)
    prod_material = combine(
        groupby(prod_material_by_cat, [:year, :product_code, :material]),
        :material_mass_mg => sum => :material_mass_mg
    )

    prod_totals = combine(
        groupby(prod_material, [:year, :product_code]),
        :material_mass_mg => sum => :product_mass_mg
    )

    prod_comp = leftjoin(prod_material, prod_totals, on=[:year, :product_code])
    prod_comp.material_weight_pct = ifelse.(
        prod_comp.product_mass_mg .> 0,
        prod_comp.material_mass_mg ./ prod_comp.product_mass_mg .* 100.0,
        missing
    )
    prod_comp.geo = fill(UMP_GEO_FALLBACK, nrow(prod_comp))
    prod_comp.source = fill("UMP_sankey", nrow(prod_comp))

    # Product material recovery rates (weighted by material masses and category recovery rates)
    prod_recovery = innerjoin(
        prod_material_by_cat,
        recovery_rates,
        on=[:year, :weee_category, :material]
    )

    if nrow(prod_recovery) > 0
        prod_recovery = filter(:recovery_rate_pct => r -> !ismissing(r), prod_recovery)
    end

    prod_rates = DataFrame(product_code=String[], year=Int[], material_recovery_rate_pct=Float64[], geo=String[], source=String[])
    if nrow(prod_recovery) > 0
        grouped = groupby(prod_recovery, [:year, :product_code])
        for group in grouped
            total_mass = sum(group.material_mass_mg)
            total_mass <= 0 && continue
            weighted_rate = sum(group.material_mass_mg .* group.recovery_rate_pct) / total_mass
            push!(prod_rates, (
                product_code=String(group.product_code[1]),
                year=Int(group.year[1]),
                material_recovery_rate_pct=weighted_rate,
                geo=UMP_GEO_FALLBACK,
                source="UMP_sankey"
            ))
        end
    end

    # Persist tables
    DatabaseAccess.write_duckdb_table_with_connection!(prod_comp, target_conn, "product_material_composition_$year")
    DatabaseAccess.write_duckdb_table_with_connection!(recovery_rates, target_conn, "material_recycling_rates_$year")
    if nrow(prod_rates) > 0
        DatabaseAccess.write_duckdb_table_with_connection!(prod_rates, target_conn, "product_material_recovery_rates_$year")
    else
        @warn "No product material recovery rates calculated for year $year; creating empty table"
        _write_empty_material_recovery_rates(year, target_conn)
    end
end

function _write_empty_material_tables(year::Int, target_conn::DuckDB.Connection)
    _write_empty_material_composition(year, target_conn)
    _write_empty_material_recycling_rates(year, target_conn)
    _write_empty_material_recovery_rates(year, target_conn)
end

function _write_empty_material_composition(year::Int, target_conn::DuckDB.Connection)
    empty = DataFrame(
        product_code=String[],
        year=Int[],
        geo=String[],
        material=String[],
        material_mass_mg=Float64[],
        product_mass_mg=Float64[],
        material_weight_pct=Float64[],
        source=String[]
    )
    DatabaseAccess.write_duckdb_table_with_connection!(empty, target_conn, "product_material_composition_$year")
end

function _write_empty_material_recycling_rates(year::Int, target_conn::DuckDB.Connection)
    empty = DataFrame(
        year=Int[],
        weee_category=String[],
        material=String[],
        recovered_mass_mg=Float64[],
        lost_mass_mg=Float64[],
        recovery_rate_pct=Float64[],
        geo=String[],
        source=String[]
    )
    DatabaseAccess.write_duckdb_table_with_connection!(empty, target_conn, "material_recycling_rates_$year")
end

function _write_empty_material_recovery_rates(year::Int, target_conn::DuckDB.Connection)
    empty = DataFrame(
        product_code=String[],
        year=Int[],
        geo=String[],
        material_recovery_rate_pct=Float64[],
        source=String[]
    )
    DatabaseAccess.write_duckdb_table_with_connection!(empty, target_conn, "product_material_recovery_rates_$year")
end

"""
    step4e_build_collection_rates(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Build product collection rate table from Eurostat WEEE datasets.
Creates:
- product_collection_rates_YYYY
"""
function step4e_build_collection_rates(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Step 4e: Building product collection rates..."

    raw_table = ""
    if DatabaseAccess.table_exists(config.source_db, "env_waseleeos_$year")
        raw_table = "env_waseleeos_$year"
    elseif DatabaseAccess.table_exists(config.source_db, "env_waselee_$year")
        raw_table = "env_waselee_$year"
    else
        @warn "No WEEE collection table found for year $year; creating empty product collection table"
        _write_empty_collection_rates(year, target_conn)
        return
    end

    raw_conn = DBInterface.connect(DuckDB.DB(config.source_db))
    weee_df = DataFrame()
    try
        weee_df = DataFrame(DBInterface.execute(raw_conn, """
            SELECT geo, waste, wst_oper, unit, value
            FROM "$raw_table"
            WHERE unit IN ('PC', 'AVG_3Y')
              AND value IS NOT NULL
        """))
    finally
        DBInterface.close!(raw_conn)
    end

    if nrow(weee_df) == 0
        @warn "No collection rate rows found in $raw_table; creating empty product collection table"
        _write_empty_collection_rates(year, target_conn)
        return
    end

    # Filter to collection-related operations
    weee_df = filter(:wst_oper => (o -> o in COLLECTION_WST_OPER), weee_df)
    if nrow(weee_df) == 0
        @warn "No collection operations found in $raw_table; creating empty product collection table"
        _write_empty_collection_rates(year, target_conn)
        return
    end

    weee_rates = combine(
        groupby(weee_df, [:geo, :waste]),
        :value => mean => :collection_rate_pct
    )

    mapping_df = DataFrame(DBInterface.execute(target_conn, """
        SELECT DISTINCT prodcom_code_clean AS product_code, weee_waste_codes
        FROM product_mapping_codes
    """))

    product_rates = DataFrame(
        product_code=String[],
        year=Int[],
        geo=String[],
        collection_rate_pct=Float64[],
        source=String[]
    )

    for row in eachrow(mapping_df)
        codes_str = row.weee_waste_codes
        if ismissing(codes_str) || isempty(String(codes_str))
            continue
        end

        codes = split(String(codes_str), ",")
        filtered = filter(r -> r.waste in codes, weee_rates)
        if nrow(filtered) == 0
            continue
        end

        grouped = groupby(filtered, :geo)
        for group in grouped
            rate = mean(group.collection_rate_pct)
            push!(product_rates, (
                product_code=String(row.product_code),
                year=year,
                geo=String(group.geo[1]),
                collection_rate_pct=rate,
                source=raw_table
            ))
        end
    end

    if nrow(product_rates) == 0
        @warn "No product collection rates computed for year $year; creating empty table"
        _write_empty_collection_rates(year, target_conn)
        return
    end

    DatabaseAccess.write_duckdb_table_with_connection!(product_rates, target_conn, "product_collection_rates_$year")
end

function _write_empty_collection_rates(year::Int, target_conn::DuckDB.Connection)
    empty = DataFrame(
        product_code=String[],
        year=Int[],
        geo=String[],
        collection_rate_pct=Float64[],
        source=String[]
    )
    DatabaseAccess.write_duckdb_table_with_connection!(empty, target_conn, "product_collection_rates_$year")
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
    step8b_build_product_weights(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

    Build product_weights_<year> table in the processed DB using config weights and derived mass/counts.
"""
function step8b_build_product_weights(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    try
        ProductWeightsBuilder.build_product_weights_table_with_conn(
            string(year);
            db_path_raw=config.source_db,
            conn_processed=target_conn
        )
    catch e
        @warn "Failed to build product_weights_$year" exception=e
    end
end

"""
    step8c_build_unit_values(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Build `product_unit_values_<year>` using existing processed tables:
- Pull production/import/export values and masses from `production_trade_<year>`
- Pull weights and any observed/derived counts from `product_weights_<year>`
- Compute value per kg and value per unit (preferring observed counts; otherwise derive counts from weight and mass)
"""
function step8c_build_unit_values(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Step 8c: Building unit value table for $year..."
    pt_table = "production_trade_$(year)"
    weights_table = "product_weights_$(year)"

    if !DatabaseAccess.table_exists(config.target_db, pt_table)
        @warn "Cannot build unit values: missing $pt_table"
        return
    end
    if !DatabaseAccess.table_exists(config.target_db, weights_table)
        @warn "Cannot build unit values: missing $weights_table"
        return
    end

    # Helpers
    _to_float(x) = x === missing ? nothing :
                   x isa Number ? Float64(x) :
                   try parse(Float64, replace(string(x), "," => "")) catch; nothing end

    # Load data
    pt_df = DataFrame(DBInterface.execute(target_conn, """
        SELECT product_code, geo,
               production_value_eur, production_volume_tonnes,
               import_value_eur, import_volume_tonnes,
               export_value_eur, export_volume_tonnes
        FROM "$pt_table"
    """))

    weights_df = DataFrame(DBInterface.execute(target_conn, """
        SELECT product_code, geo, weight_kg_config, unit_counts
        FROM "$weights_table"
    """))

    weights_lookup = Dict{Tuple{String,String},NamedTuple{(:weight_kg, :unit_counts),Tuple{Float64,Union{Missing,Float64}}}}()
    for row in eachrow(weights_df)
        weights_lookup[(String(row.product_code), String(row.geo))] =
            (Float64(row.weight_kg_config), row.unit_counts)
    end

    flows = [
        (:production_value_eur, :production_volume_tonnes, "production"),
        (:import_value_eur, :import_volume_tonnes, "import"),
        (:export_value_eur, :export_volume_tonnes, "export")
    ]

    result = DataFrame(
        product_code = String[],
        geo = String[],
        year = Int[],
        flow = String[],
        value_eur = Union{Float64,Missing}[],
        mass_tonnes = Union{Float64,Missing}[],
        unit_counts = Union{Float64,Missing}[],
        value_per_unit_eur = Union{Float64,Missing}[],
        value_per_kg_eur = Union{Float64,Missing}[],
        source = String[]
    )

    for row in eachrow(pt_df)
        key = (String(row.product_code), String(row.geo))
        weight_info = get(weights_lookup, key, get(weights_lookup, (key[1], "EU27_2020"), (0.0, missing)))
        weight = weight_info.weight_kg
        counts_hint = weight_info.unit_counts

        for (val_sym, mass_sym, flow_label) in flows
            value = _to_float(row[val_sym])
            mass_tonnes = _to_float(row[mass_sym])

            # Skip rows with no monetary value
            if value === nothing
                continue
            end

            counts = counts_hint
            source = "value_only"

            if counts !== missing && counts !== nothing && counts > 0
                source = "counts_from_product_weights"
            elseif mass_tonnes !== nothing && mass_tonnes > 0 && weight > 0
                counts = mass_tonnes * 1000.0 / weight
                source = "derived_from_weight"
            elseif mass_tonnes !== nothing && mass_tonnes > 0
                source = "mass_only"
            end

            value_per_unit = (value !== nothing && counts !== nothing && counts !== missing && counts > 0) ?
                             value / counts : missing
            value_per_kg = (value !== nothing && mass_tonnes !== nothing && mass_tonnes > 0) ?
                           value / (mass_tonnes * 1000.0) : missing

            push!(result, (
                product_code = key[1],
                geo = key[2],
                year = year,
                flow = flow_label,
                value_eur = value === nothing ? missing : value,
                mass_tonnes = mass_tonnes === nothing ? missing : mass_tonnes,
                unit_counts = counts === nothing ? missing : counts,
                value_per_unit_eur = value_per_unit,
                value_per_kg_eur = value_per_kg,
                source = source
            ))
        end
    end

    table_name = "$(PRODUCT_UNIT_VALUES_PREFIX)$(year)"
    DatabaseAccess.write_duckdb_table_with_connection!(result, target_conn, table_name)
end

"""
    step8d_create_strategy_indicators(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)

Create strategy-specific circularity indicators table.
Creates table: circularity_indicators_by_strategy_YYYY
"""
function step8d_create_strategy_indicators(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Step 8d: Building strategy-specific indicators for $year..."

    ci_table = "circularity_indicators_$(year)"
    params_table = "parameters_circularity_rate"
    coll_table = "product_collection_rates_$(year)"
    rec_table = "product_material_recovery_rates_$(year)"

    if !DatabaseAccess.table_exists(config.target_db, ci_table)
        @warn "Missing $ci_table; skipping strategy indicators"
        return
    end

    if !DatabaseAccess.table_exists(config.target_db, params_table)
        @warn "Missing $params_table; skipping strategy indicators"
        return
    end

    ci_df = DataFrame(DBInterface.execute(target_conn, "SELECT * FROM \"$ci_table\""))
    params_df = DataFrame(DBInterface.execute(target_conn, """
        SELECT product_code, current_refurbishment_rate
        FROM "$params_table"
    """))

    coll_df = DatabaseAccess.table_exists(config.target_db, coll_table) ?
              DataFrame(DBInterface.execute(target_conn, "SELECT product_code, geo, collection_rate_pct FROM \"$coll_table\"")) :
              DataFrame(product_code=String[], geo=String[], collection_rate_pct=Float64[])

    rec_df = DatabaseAccess.table_exists(config.target_db, rec_table) ?
             DataFrame(DBInterface.execute(target_conn, "SELECT product_code, geo, material_recovery_rate_pct FROM \"$rec_table\"")) :
             DataFrame(product_code=String[], geo=String[], material_recovery_rate_pct=Float64[])

    params_lookup = Dict{String,Float64}()
    for row in eachrow(params_df)
        params_lookup[String(row.product_code)] = Float64(row.current_refurbishment_rate)
    end

    coll_lookup = Dict{Tuple{String,String},Float64}()
    for row in eachrow(coll_df)
        coll_lookup[(String(row.product_code), String(row.geo))] = Float64(row.collection_rate_pct)
    end

    rec_lookup = Dict{String,Float64}()
    for row in eachrow(rec_df)
        if String(row.geo) == UMP_GEO_FALLBACK
            rec_lookup[String(row.product_code)] = Float64(row.material_recovery_rate_pct)
        end
    end

    result = DataFrame(
        product_code=String[],
        year=Int[],
        geo=String[],
        level=String[],
        strategy=String[],
        rate_pct=Union{Float64,Missing}[],
        material_recovery_rate_pct=Union{Float64,Missing}[],
        apparent_consumption_tonnes=Union{Float64,Missing}[],
        apparent_consumption_value_eur=Union{Float64,Missing}[],
        material_savings_tonnes=Union{Float64,Missing}[],
        material_savings_eur=Union{Float64,Missing}[],
        production_reduction_tonnes=Union{Float64,Missing}[],
        production_reduction_eur=Union{Float64,Missing}[]
    )

    for row in eachrow(ci_df)
        product_code = String(row.product_code)
        geo = String(row.geo)
        level = String(row.level)
        apparent_tonnes = row.apparent_consumption_tonnes
        apparent_value = row.apparent_consumption_value_eur

        refurb_rate = get(params_lookup, product_code, 0.0)

        refurb_savings_tonnes = (apparent_tonnes isa Number) ? Float64(apparent_tonnes) * refurb_rate / 100.0 : missing
        refurb_savings_eur = (apparent_value isa Number) ? Float64(apparent_value) * refurb_rate / 100.0 : missing

        push!(result, (
            product_code=product_code,
            year=year,
            geo=geo,
            level=level,
            strategy="refurbishment",
            rate_pct=refurb_rate,
            material_recovery_rate_pct=missing,
            apparent_consumption_tonnes=apparent_tonnes,
            apparent_consumption_value_eur=apparent_value,
            material_savings_tonnes=refurb_savings_tonnes,
            material_savings_eur=refurb_savings_eur,
            production_reduction_tonnes=refurb_savings_tonnes,
            production_reduction_eur=refurb_savings_eur
        ))

        collection_rate = get(coll_lookup, (product_code, geo), get(coll_lookup, (product_code, UMP_GEO_FALLBACK), missing))
        recovery_rate = get(rec_lookup, product_code, missing)

        if collection_rate isa Number && recovery_rate isa Number && apparent_tonnes isa Number
            recycle_savings_tonnes = Float64(apparent_tonnes) * Float64(collection_rate) / 100.0 * Float64(recovery_rate) / 100.0
        else
            recycle_savings_tonnes = missing
        end

        if collection_rate isa Number && recovery_rate isa Number && apparent_value isa Number
            recycle_savings_eur = Float64(apparent_value) * Float64(collection_rate) / 100.0 * Float64(recovery_rate) / 100.0
        else
            recycle_savings_eur = missing
        end

        push!(result, (
            product_code=product_code,
            year=year,
            geo=geo,
            level=level,
            strategy="recycling",
            rate_pct=collection_rate,
            material_recovery_rate_pct=recovery_rate,
            apparent_consumption_tonnes=apparent_tonnes,
            apparent_consumption_value_eur=apparent_value,
            material_savings_tonnes=recycle_savings_tonnes,
            material_savings_eur=recycle_savings_eur,
            production_reduction_tonnes=0.0,
            production_reduction_eur=0.0
        ))
    end

    table_name = "circularity_indicators_by_strategy_$(year)"
    DatabaseAccess.write_duckdb_table_with_connection!(result, target_conn, table_name)
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
    current_refurb = get(config.analysis_params, "current_refurbishment_rates", Dict())
    uplift = get(config.analysis_params, "circularity_uplift", Dict())
    uplift_mean = Float64(get(uplift, "mean", 0.0))
    uplift_sd = Float64(get(uplift, "sd", 0.0))
    uplift_ci_lower = Float64(get(uplift, "ci_lower", 0.0))
    uplift_ci_upper = Float64(get(uplift, "ci_upper", 0.0))

    # Create DataFrame with product-specific rates
    product_codes = String[]
    uplift_mean_vec = Float64[]
    uplift_sd_vec = Float64[]
    uplift_ci_lower_vec = Float64[]
    uplift_ci_upper_vec = Float64[]
    current_refurb_vec = Float64[]

    # Collect all product codes from both dictionaries
    all_products = keys(current_refurb)

    for product_code in all_products
        push!(product_codes, product_code)
        push!(uplift_mean_vec, uplift_mean)
        push!(uplift_sd_vec, uplift_sd)
        push!(uplift_ci_lower_vec, uplift_ci_lower)
        push!(uplift_ci_upper_vec, uplift_ci_upper)
        push!(current_refurb_vec, get(current_refurb, product_code, 0.0))
    end

    # Create DataFrame with one row per product
    circularity_params_df = DataFrame(
        product_code=product_codes,
        circularity_uplift_mean=uplift_mean_vec,
        circularity_uplift_sd=uplift_sd_vec,
        circularity_uplift_ci_lower=uplift_ci_lower_vec,
        circularity_uplift_ci_upper=uplift_ci_upper_vec,
        current_refurbishment_rate=current_refurb_vec,
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
    step8b_build_product_weights,
    step8c_build_unit_values,
    step9_cleanup_temp_tables

end # module DataProcessor
