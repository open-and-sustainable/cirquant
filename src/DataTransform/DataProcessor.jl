module DataProcessor

using DataFrames
using DuckDB, DBInterface
using Dates
using Dates: now, format
using ..DatabaseAccess
using ..AnalysisConfigLoader
using ..CircularityProcessor
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
        step4_process_trade_data(year, config, target_conn)

        # Step 4b: Harmonize production and trade data using mappings
        step4b_harmonize_production_trade_data(year, config, target_conn)

        # Step 4c: Fill in PRODCOM trade data where COMEXT is zero
        step4c_fill_prodcom_trade_fallback(year, config, target_conn)

        # Step 5: Create main circularity indicators table
        #step5_create_circularity_indicators(year, config)

        # Step 6: Calculate country aggregates
        #step6_create_country_aggregates(year, config)

        # Step 7: Calculate product aggregates
        #step7_create_product_aggregates(year, config)

        # Step 8: Apply circularity parameters
        # COMMENTED OUT - focusing on product/geo matching first
        # step8_apply_circularity_parameters(year, config)

        # Step 9: Clean up temporary tables
        #step9_cleanup_temp_tables(year, config)

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
    # Ensure product mapping table exists
    if !has_product_mapping_table(config.target_db)
        write_product_conversion_table_with_connection(conn)
    end

    # Ensure country code mapping table exists
    @info "Creating country code mapping table..."
    CountryCodeMapper.create_country_mapping_table_with_connection(conn)

    # Ensure parameter tables exist
    ensure_circularity_parameters_table_with_connection(config, conn)
end

"""
    step2_process_unit_conversions(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)

Step 2: Process unit conversions for PRODCOM data using PRQL query.
"""
function step2_process_unit_conversions(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)
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

Step 3: Process production data using PRQL query.
"""
function step3_process_production_data(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)
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

Step 4: Process trade data using PRQL query.
"""
function step4_process_trade_data(year::Int, config::ProcessingConfig, conn::DuckDB.Connection)
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
end

"""
    step4b_fill_prodcom_trade_fallback(year::Int, config::ProcessingConfig)

Fill in PRODCOM trade data where COMEXT has zero values.
This provides a fallback for products/countries not covered by COMEXT.
"""
function step4c_fill_prodcom_trade_fallback(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Filling in PRODCOM trade data where COMEXT is zero for year $year..."

    # Connect to source database only (target connection passed as parameter)
    conn_source = DBInterface.connect(DuckDB.DB, config.source_db)

    try
        # Load country mapping for PRODCOM numeric codes directly
        country_mapping_df = CountryCodeMapper.get_country_code_mapping()

        # Load harmonized data
        harmonized_table = "production_trade_harmonized_$year"

        # Check if table exists before trying to read
        table_exists_query = "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '$harmonized_table'"
        exists_result = DBInterface.execute(target_conn, table_exists_query) |> DataFrame

        if exists_result.cnt[1] == 0
            @error "Table $harmonized_table does not exist!"
            DBInterface.close!(conn_source)
            return
        end

        harmonized_query = "SELECT * FROM $harmonized_table"
        harmonized_df = DataFrame(DBInterface.execute(target_conn, harmonized_query))

        @info "Loaded harmonized data: $(nrow(harmonized_df)) rows, $(ncol(harmonized_df)) columns"

        # Query PRODCOM trade data and units
        prodcom_trade_query = """
            SELECT
                prccode as product_code,
                decl as geo,
                indicators,
                TRY_CAST(value AS DOUBLE) as value
            FROM prodcom_ds_056120_$year
            WHERE indicators IN ('IMPQNT', 'EXPQNT', 'IMPVAL', 'EXPVAL')
            AND value NOT IN ('kg', 'p/st', 'm', 'm2', 'm3', 'l', 'hl', 'ct/l')
            AND TRY_CAST(value AS DOUBLE) IS NOT NULL
        """
        prodcom_trade_df = DataFrame(DBInterface.execute(conn_source, prodcom_trade_query))

        # Query units for the products
        prodcom_units_query = """
            SELECT DISTINCT
                prccode as product_code,
                value as unit
            FROM prodcom_ds_056120_$year
            WHERE indicators = 'QNTUNIT'
            AND prccode IN (SELECT DISTINCT prccode FROM prodcom_ds_056120_$year WHERE indicators IN ('IMPQNT', 'EXPQNT'))
        """
        prodcom_units_df = DataFrame(DBInterface.execute(conn_source, prodcom_units_query))

        # Process PRODCOM trade data
        if nrow(prodcom_trade_df) > 0
            # Remove dots from product codes to match harmonized format
            prodcom_trade_df.product_code = replace.(prodcom_trade_df.product_code, "." => "")
            prodcom_units_df.product_code = replace.(prodcom_units_df.product_code, "." => "")

            # Map PRODCOM country codes to ISO codes
            prodcom_trade_df = leftjoin(
                prodcom_trade_df,
                country_mapping_df,
                on = :geo => :prodcom_code,
                makeunique = true
            )
            prodcom_trade_df.geo_iso = coalesce.(prodcom_trade_df.iso_code, prodcom_trade_df.geo)

            # Join with units data
            prodcom_trade_with_units = leftjoin(
                prodcom_trade_df,
                prodcom_units_df,
                on = :product_code,
                makeunique = true
            )

            # Apply conversion factors based on unit type
            prodcom_trade_with_units.conversion_factor = map(eachrow(prodcom_trade_with_units)) do row
                unit = ismissing(row.unit) ? "kg" : row.unit
                product_code = row.product_code

                factor = if unit == "kg"
                    0.001  # kg to tonnes
                elseif unit == "t"
                    1.0  # already tonnes
                elseif unit == "p/st"
                    # Product-specific conversions
                    if product_code == "28211330"
                        0.100  # Heat pumps ~100kg per unit
                    elseif product_code == "27114000"
                        0.020  # PV panels ~20kg per panel
                    elseif startswith(product_code, "2720")
                        0.025  # Batteries ~25kg average
                    elseif startswith(product_code, "2620")
                        0.005  # ICT equipment ~5kg average
                    else
                        0.010  # 10kg default
                    end
                elseif unit == "ce/el"
                    0.0003  # Battery cells ~300g per cell
                else
                    1.0  # Unknown unit: preserve value
                end

                # Validate factor
                if isnan(factor) || isinf(factor) || factor < 0 || factor > 1000
                    @warn "Invalid conversion factor for $product_code unit $unit: $factor, using 1.0"
                    1.0
                else
                    factor
                end
            end

            # Convert volumes to tonnes for quantity indicators
            prodcom_trade_with_units.value_converted = map(eachrow(prodcom_trade_with_units)) do row
                if row.indicators in ["IMPQNT", "EXPQNT"]
                    # Ensure valid numeric values
                    val = ismissing(row.value) ? 0.0 : row.value
                    factor = ismissing(row.conversion_factor) ? 1.0 : row.conversion_factor
                    result = val * factor
                    # Check for NaN or Inf
                    if isnan(result) || isinf(result)
                        @warn "Invalid conversion result for $(row.product_code): value=$val, factor=$factor"
                        0.0
                    else
                        result
                    end
                else
                    # Keep monetary values as-is, but ensure they're valid
                    val = ismissing(row.value) ? 0.0 : row.value
                    if isnan(val) || isinf(val)
                        @warn "Invalid monetary value for $(row.product_code): value=$val"
                        0.0
                    else
                        val
                    end
                end
            end

            # Pivot PRODCOM data to get import/export volumes and values
            prodcom_pivot = combine(groupby(prodcom_trade_with_units, [:product_code, :geo_iso])) do group
                # Calculate sums with safety checks
                import_vol = 0.0
                import_val = 0.0
                export_vol = 0.0
                export_val = 0.0

                for row in eachrow(group)
                    val = ismissing(row.value_converted) ? 0.0 : row.value_converted
                    if !isnan(val) && !isinf(val) && val >= 0 && val < 1e12  # Reasonable upper bound
                        if row.indicators == "IMPQNT"
                            import_vol += val
                        elseif row.indicators == "IMPVAL"
                            import_val += val
                        elseif row.indicators == "EXPQNT"
                            export_vol += val
                        elseif row.indicators == "EXPVAL"
                            export_val += val
                        end
                    elseif val < 0 || val >= 1e12
                        @warn "Skipping unreasonable value for $(row.product_code): $val"
                    end
                end

                # Final validation before returning
                import_vol = isnan(import_vol) || isinf(import_vol) || import_vol < 0 ? 0.0 : import_vol
                import_val = isnan(import_val) || isinf(import_val) || import_val < 0 ? 0.0 : import_val
                export_vol = isnan(export_vol) || isinf(export_vol) || export_vol < 0 ? 0.0 : export_vol
                export_val = isnan(export_val) || isinf(export_val) || export_val < 0 ? 0.0 : export_val

                DataFrame(
                    import_volume_prodcom = import_vol,
                    import_value_prodcom = import_val,
                    export_volume_prodcom = export_vol,
                    export_value_prodcom = export_val
                )
            end

            # Join with harmonized data
            harmonized_with_prodcom = leftjoin(
                harmonized_df,
                prodcom_pivot,
                on = [:product_code => :product_code, :geo => :geo_iso]
            )

            # Fill in zeros with PRODCOM data where applicable
            records_updated = 0
            updated_records = Set{Tuple{String, String, Int}}()  # Track which records were updated

            for row in eachrow(harmonized_with_prodcom)
                record_updated = false

                # Fill import volume if COMEXT is zero but PRODCOM has data
                if row.import_volume_tonnes == 0.0 && !ismissing(row.import_volume_prodcom) && row.import_volume_prodcom > 0
                    row.import_volume_tonnes = row.import_volume_prodcom
                    record_updated = true
                end

                # Fill import value
                if row.import_value_eur == 0.0 && !ismissing(row.import_value_prodcom) && row.import_value_prodcom > 0
                    row.import_value_eur = row.import_value_prodcom
                    record_updated = true
                end

                # Fill export volume
                if row.export_volume_tonnes == 0.0 && !ismissing(row.export_volume_prodcom) && row.export_volume_prodcom > 0
                    row.export_volume_tonnes = row.export_volume_prodcom
                    record_updated = true
                end

                # Fill export value
                if row.export_value_eur == 0.0 && !ismissing(row.export_value_prodcom) && row.export_value_prodcom > 0
                    row.export_value_eur = row.export_value_prodcom
                    record_updated = true
                end

                if record_updated
                    push!(updated_records, (row.product_code, row.geo, row.year))
                    records_updated += 1
                end
            end

            # Create a clean DataFrame with only the original columns
            clean_harmonized = DataFrame()
            for col in names(harmonized_df)
                if hasproperty(harmonized_with_prodcom, Symbol(col))
                    clean_harmonized[!, Symbol(col)] = harmonized_with_prodcom[!, Symbol(col)]
                else
                    # This shouldn't happen, but add safety
                    @warn "Column $col missing after join, using original data"
                    clean_harmonized[!, Symbol(col)] = harmonized_df[!, Symbol(col)]
                end
            end
            harmonized_with_prodcom = clean_harmonized

            # Ensure year column is Integer type before writing
            if hasproperty(harmonized_with_prodcom, :year)
                if eltype(harmonized_with_prodcom.year) <: AbstractString
                    harmonized_with_prodcom.year = parse.(Int, harmonized_with_prodcom.year)
                elseif !(eltype(harmonized_with_prodcom.year) <: Integer)
                    harmonized_with_prodcom.year = convert.(Int, harmonized_with_prodcom.year)
                end
            end

            # Ensure all numeric columns are valid (no NaN or Inf)
            for col in [:import_volume_tonnes, :import_value_eur, :export_volume_tonnes, :export_value_eur,
                        :production_volume_tonnes, :production_value_eur]
                if hasproperty(harmonized_with_prodcom, col)
                    harmonized_with_prodcom[!, col] = map(harmonized_with_prodcom[!, col]) do x
                        if ismissing(x) || isnan(x) || isinf(x) || x < 0 || x > 1e12
                            if !ismissing(x) && (isnan(x) || isinf(x) || x < 0 || x > 1e12)
                                @warn "Invalid value in column $col: $x, replacing with 0.0"
                            end
                            0.0
                        else
                            x
                        end
                    end
                end
            end

            # Close source connection
            DBInterface.close!(conn_source)

            # Write to a TEST table instead of updating the harmonized table
            test_table_name = "prodcom_fallback_test_$year"
            @info "Writing PRODCOM fallback data to test table: $test_table_name"

            # Only write the records that were actually updated
            updated_df = DataFrame()
            for (idx, row) in enumerate(eachrow(harmonized_with_prodcom))
                key = (row.product_code, row.geo, row.year)
                if key in updated_records
                    push!(updated_df, row)
                end
            end

            @info "Writing $(nrow(updated_df)) updated records to test table"

            # Write to test table using existing connection
            DatabaseAccess.write_duckdb_table_with_connection!(updated_df, target_conn, test_table_name)

            @info "Successfully wrote PRODCOM fallback data to test table $test_table_name"
            @info "Original harmonized table remains unchanged"
            @info "Updated $records_updated records with PRODCOM trade data (in test table)"
        else
            @info "No PRODCOM trade data available for fallback"
            # Close source connection
            DBInterface.close!(conn_source)
        end

        # Connections already closed above

    catch e
        # Ensure connections are closed even on error
        try
            DBInterface.close!(conn_source)
        catch close_err
            @warn "Failed to close source connection" exception=close_err
        end
        @error "Failed to fill PRODCOM trade fallback" exception=e
        rethrow(e)
    end
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
    harmonize_production_trade_data(year::Int, config::ProcessingConfig)

Harmonize production and trade data using country and product mappings.
This function merges production_temp and trade_temp tables using the mapping tables.
"""
function step4b_harmonize_production_trade_data(year::Int, config::ProcessingConfig, target_conn::DuckDB.Connection)
    @info "Harmonizing production and trade data for year $year..."

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
        @error "Failed to load mapping tables" exception=e
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
        @error "Failed to load intermediate tables" exception=e
        rethrow(e)
    end

    # Harmonize country codes in production data
    production_harmonized = leftjoin(
        production_df,
        country_mapping_df,
        on = :geo => :prodcom_code,
        makeunique = true
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
        prodcom_code = String[],
        year = Int[],
        geo = String[],
        level = String[],
        import_volume_tonnes = Float64[],
        import_value_eur = Float64[],
        export_volume_tonnes = Float64[],
        export_value_eur = Float64[],
        data_source = String[]
    )

    for row in eachrow(trade_df)
        product_code = row.product_code
        data_source = hasproperty(row, :data_source) ? row.data_source : "COMEXT"

        if data_source == "PRODCOM"
            # PRODCOM trade data already has PRODCOM codes - just normalize by removing dots
            push!(trade_expanded, (
                prodcom_code = replace(String(product_code), "." => ""),
                year = parse(Int, string(row.year)),
                geo = String(row.geo),  # PRODCOM uses numeric codes, will be mapped later
                level = String(row.level),
                import_volume_tonnes = Float64(row.import_volume_tonnes),
                import_value_eur = Float64(row.import_value_eur),
                export_volume_tonnes = Float64(row.export_volume_tonnes),
                export_value_eur = Float64(row.export_value_eur),
                data_source = data_source
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
                        prodcom_code = replace(String(prod_match.prodcom_code), "." => ""),
                        year = parse(Int, string(row.year)),
                        geo = String(row.geo),  # COMEXT already uses ISO codes
                        level = String(row.level),
                        import_volume_tonnes = Float64(row.import_volume_tonnes),
                        import_value_eur = Float64(row.import_value_eur),
                        export_volume_tonnes = Float64(row.export_volume_tonnes),
                        export_value_eur = Float64(row.export_value_eur),
                        data_source = data_source
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
        on = [:prodcom_code, :geo, :year],
        makeunique = true
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

        @info "Created harmonized table '$table_name' with $(nrow(merged_data)) records"

    catch e
        @error "Failed to write harmonized data" exception=e
        rethrow(e)
    end

    return merged_data
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
        "trade_temp_$(year)",
        "production_trade_harmonized_$(year)"
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
    step4b_fill_prodcom_trade_fallback,
    step5_create_circularity_indicators,
    step6_create_country_aggregates,
    step7_create_product_aggregates,
    step8_apply_circularity_parameters,
    step9_cleanup_temp_tables,
    harmonize_production_trade_data

end # module DataProcessor
