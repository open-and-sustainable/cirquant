module CircularityProcessor

using DataFrames
using DuckDB, DBInterface
using Dates

# Import necessary modules from parent
using ..DatabaseAccess
using ..ProductConversionTables

# Include the unit converter modules
include("UnitConversion/UnitConverter.jl")
include("ProdcomUnitConverter.jl")
using .UnitConverter
using .ProdcomUnitConverter

export create_circularity_table, get_circularity_table_name, validate_circularity_table, create_circularity_tables_range
export process_year_data, execute_prql_for_year, load_product_mapping, inspect_raw_tables, ensure_prql_installed

"""
    get_circularity_table_name(year::Int)

Generate the table name for circularity data for a specific year.

# Arguments
- `year::Int`: The year for which to generate the table name

# Returns
- `String`: Table name in format "circularity_indicators_YYYY"
"""
function get_circularity_table_name(year::Int)
    return "circularity_indicators_$(year)"
end

"""
    create_circularity_table(year::Int; db_path::String = DB_PATH_PROCESSED, replace::Bool = false)

Creates the main circularity indicators table structure in the processed database for a specific year.
The table contains dimensions, key indicators, and circularity metrics.

# Arguments
- `year::Int`: The year for which to create the table
- `db_path::String`: Path to the DuckDB database (default: DB_PATH_PROCESSED)
- `replace::Bool`: Whether to replace existing table if it exists (default: false)

# Returns
- `Bool`: true if successful, false otherwise

# Table Structure
## Dimensions (identifiers for each record)
- product_code: PRODCOM or combined code
- product_name: Human-readable product label
- year: Reference year
- geo: Spatial level (EU country code like "DE", "FR" or "EU27")
- level: Indicates whether data is "country" or "EU" aggregate

## Key indicators
### Production
- production_volume_tonnes: Quantity produced (tonnes)
- production_value_eur: Production value (€)

### Trade
- import_volume_tonnes: Quantity imported (tonnes)
- import_value_eur: Value of imports (€)
- export_volume_tonnes: Quantity exported (tonnes)
- export_value_eur: Value of exports (€)

### Apparent Consumption
- apparent_consumption_tonnes: Calculated as production + imports - exports
- apparent_consumption_value_eur: Approximate monetary value of apparent consumption

## Circularity indicators (from secondary data/literature)
- current_circularity_rate_pct: % of material currently recirculated
- potential_circularity_rate_pct: % achievable with digital innovations
- estimated_material_savings_tonnes: Potential tonnes saved by increasing circularity
- estimated_monetary_savings_eur: Estimated € saved from improved circularity
"""
function create_circularity_table(year::Int; db_path::String, replace::Bool=false)
    table_name = get_circularity_table_name(year)

    try
        @info "Creating circularity indicators table '$table_name' for year $year in database: $db_path"

        # Connect to database
        db_conn = DuckDB.DB(db_path)
        con = DBInterface.connect(db_conn)

        # Check if table exists
        result = DBInterface.execute(con,
            "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '$table_name'"
        ) |> DataFrame

        if result.cnt[1] > 0
            if replace
                @info "Table '$table_name' exists. Dropping and recreating..."
                DBInterface.execute(con, "DROP TABLE IF EXISTS $table_name")
            else
                @warn "Table '$table_name' already exists. Use replace=true to overwrite."
                DBInterface.close!(con)
                DBInterface.close!(db_conn)
                return false
            end
        end

        # Create the table with all required columns
        create_sql = """
        CREATE TABLE $table_name (
            -- Dimensions (identifiers)
            product_code VARCHAR NOT NULL,
            product_name VARCHAR NOT NULL,
            year INTEGER NOT NULL,
            geo VARCHAR NOT NULL,
            level VARCHAR NOT NULL,

            -- Production indicators
            production_volume_tonnes DOUBLE,
            production_value_eur DOUBLE,

            -- Trade indicators
            import_volume_tonnes DOUBLE,
            import_value_eur DOUBLE,
            export_volume_tonnes DOUBLE,
            export_value_eur DOUBLE,

            -- Apparent consumption indicators
            apparent_consumption_tonnes DOUBLE,
            apparent_consumption_value_eur DOUBLE,

            -- Circularity indicators
            current_circularity_rate_pct DOUBLE,
            potential_circularity_rate_pct DOUBLE,
            estimated_material_savings_tonnes DOUBLE,
            estimated_monetary_savings_eur DOUBLE,

            -- Metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- Constraints
            PRIMARY KEY (product_code, geo, year),
            CHECK (year = $year),
            CHECK (level IN ('country', 'EU')),
            CHECK (LENGTH(geo) >= 2 AND LENGTH(geo) <= 4)
        )
        """

        DBInterface.execute(con, create_sql)

        # Create indexes for common query patterns
        DBInterface.execute(con, "CREATE INDEX idx_$(table_name)_product ON $table_name(product_code)")
        DBInterface.execute(con, "CREATE INDEX idx_$(table_name)_geo ON $table_name(geo)")
        DBInterface.execute(con, "CREATE INDEX idx_$(table_name)_level ON $table_name(level)")

        @info "Successfully created table '$table_name' with circularity indicators structure"

        DBInterface.close!(con)
        DBInterface.close!(db_conn)
        return true

    catch e
        @error "Failed to create circularity indicators table '$table_name'" exception = e
        return false
    end
end

"""
    validate_circularity_table(year::Int; db_path::String = DB_PATH_PROCESSED)

Validates that the circularity table for a given year exists and has the correct structure.

# Arguments
- `year::Int`: The year to validate
- `db_path::String`: Path to the DuckDB database (default: DB_PATH_PROCESSED)

# Returns
- `Dict`: Dictionary with validation results including:
  - exists: Bool indicating if table exists
  - has_correct_columns: Bool indicating if all required columns are present
  - missing_columns: Array of missing column names
  - row_count: Number of rows in the table
"""
function validate_circularity_table(year::Int; db_path::String)
    table_name = get_circularity_table_name(year)
    validation_result = Dict(
        :exists => false,
        :has_correct_columns => false,
        :missing_columns => String[],
        :row_count => 0
    )

    try
        # Connect to database
        db_conn = DuckDB.DB(db_path)
        con = DBInterface.connect(db_conn)

        # Check if table exists
        result = DBInterface.execute(con,
            "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '$table_name'"
        ) |> DataFrame

        if result.cnt[1] == 0
            @warn "Table '$table_name' does not exist"
            DBInterface.close!(con)
            DBInterface.close!(db_conn)
            return validation_result
        end

        validation_result[:exists] = true

        # Get table columns
        columns_df = DBInterface.execute(con,
            "SELECT column_name FROM information_schema.columns WHERE table_name = '$table_name'"
        ) |> DataFrame

        existing_columns = Set(columns_df.column_name)

        # Define required columns
        required_columns = Set([
            "product_code", "product_name", "year", "geo", "level",
            "production_volume_tonnes", "production_value_eur",
            "import_volume_tonnes", "import_value_eur",
            "export_volume_tonnes", "export_value_eur",
            "apparent_consumption_tonnes", "apparent_consumption_value_eur",
            "current_circularity_rate_pct", "potential_circularity_rate_pct",
            "estimated_material_savings_tonnes", "estimated_monetary_savings_eur"
        ])

        # Check for missing columns
        missing = setdiff(required_columns, existing_columns)
        validation_result[:missing_columns] = collect(missing)
        validation_result[:has_correct_columns] = isempty(missing)

        # Get row count
        count_result = DBInterface.execute(con, "SELECT COUNT(*) as cnt FROM $table_name") |> DataFrame
        validation_result[:row_count] = count_result.cnt[1]

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        # Log validation results
        if validation_result[:has_correct_columns]
            @info "Table '$table_name' validation passed with $(validation_result[:row_count]) rows"
        else
            @warn "Table '$table_name' is missing columns: $(join(validation_result[:missing_columns], ", "))"
        end

        return validation_result

    catch e
        @error "Failed to validate table '$table_name'" exception = e
        return validation_result
    end
end

"""
    create_circularity_tables_range(start_year::Int, end_year::Int;
                                  db_path::String = DB_PATH_PROCESSED,
                                  replace::Bool = false)

Creates circularity indicator tables for a range of years.

# Arguments
- `start_year::Int`: Starting year
- `end_year::Int`: Ending year (inclusive)
- `db_path::String`: Path to the DuckDB database (default: DB_PATH_PROCESSED)
- `replace::Bool`: Whether to replace existing tables (default: false)

# Returns
- `Dict`: Summary of results with counts of successful and failed table creations
"""
function create_circularity_tables_range(start_year::Int, end_year::Int;
    db_path::String,
    replace::Bool=false)
    results = Dict(:successful => 0, :failed => 0, :skipped => 0)

    @info "Creating circularity tables for years $start_year to $end_year"

    for year in start_year:end_year
        success = create_circularity_table(year; db_path=db_path, replace=replace)

        if success
            results[:successful] += 1
        else
            # Check if it was skipped due to existing table
            validation = validate_circularity_table(year; db_path=db_path)
            if validation[:exists] && !replace
                results[:skipped] += 1
            else
                results[:failed] += 1
            end
        end
    end

    @info "Table creation completed: $(results[:successful]) successful, $(results[:failed]) failed, $(results[:skipped]) skipped"
    return results
end

"""
    ensure_prql_installed()

Ensures the PRQL extension is installed for DuckDB.
This should be called once before using PRQL queries.

# Returns
- `Bool`: true if PRQL is available, false otherwise
"""
function ensure_prql_installed()
    return DatabaseAccess.installPRQL_DuckDBextension()
end

"""
    load_product_mapping(db_path::String)

Loads the product conversion/mapping table from the processed database.

# Arguments
- `db_path::String`: Path to the DuckDB database containing the product mapping table

# Returns
- `DataFrame`: Product mapping data with columns:
  - product_id: Unique identifier
  - product: Human-readable product name
  - prodcom_code: PRODCOM classification code
  - hs_codes: Harmonized System codes (comma-separated)
- `nothing`: If loading fails
"""
function load_product_mapping(db_path::String)
    try
        @info "Loading product mapping table from database"
        mapping_df = ProductConversionTables.read_product_conversion_table(db_path)

        if isnothing(mapping_df)
            @error "Failed to load product mapping table"
            return nothing
        end

        @info "Successfully loaded $(nrow(mapping_df)) product mappings"
        return mapping_df
    catch e
        @error "Error loading product mapping" exception = e
        return nothing
    end
end

"""
    execute_prql_for_year(prql_path::String, db_path::String, year::Int)

Executes a PRQL query file with year parameter substitution.

# Arguments
- `prql_path::String`: Path to the PRQL file
- `db_path::String`: Path to the DuckDB database
- `year::Int`: Year to use for filtering/processing

# Returns
- `DataFrame`: Query results
- `nothing`: If execution fails

# Note
The PRQL file can use the year parameter by including it in the query.
"""
function execute_prql_for_year(prql_path::String, db_path::String, year::Int)
    try
        @info "Executing PRQL query from $prql_path for year $year"

        # Read the PRQL file
        if !isfile(prql_path)
            @error "PRQL file not found: $prql_path"
            return nothing
        end

        prql_content = read(prql_path, String)

        # Replace year placeholder if exists (e.g., {{YEAR}} with actual year)
        prql_content_year = replace(prql_content, "{{YEAR}}" => string(year))

        # Create temporary PRQL file with year substitution
        temp_prql = tempname() * ".prql"
        open(temp_prql, "w") do f
            write(f, prql_content_year)
        end

        # Execute the PRQL query using DatabaseAccess
        result_df = DatabaseAccess.executePRQL(db_path, temp_prql)

        # Clean up temp file
        rm(temp_prql, force=true)

        @info "Query executed successfully, returned $(nrow(result_df)) rows"

        # Debug: Show what the PRQL query returned
        if contains(prql_path, "production") && nrow(result_df) > 0
            @info "DEBUG: PRQL production query results:"
            println("Columns: ", names(result_df))
            println("First 5 rows:")
            println(first(result_df, min(5, nrow(result_df))))

            # Check for non-zero values
            if "production_volume_tonnes" in names(result_df)
                non_zero_vol = count(x -> !ismissing(x) && x > 0, result_df.production_volume_tonnes)
                @info "DEBUG: Non-zero production volumes: $non_zero_vol out of $(nrow(result_df))"
            end
            if "production_value_eur" in names(result_df)
                non_zero_val = count(x -> !ismissing(x) && x > 0, result_df.production_value_eur)
                @info "DEBUG: Non-zero production values: $non_zero_val out of $(nrow(result_df))"
            end
        end

        return result_df

    catch e
        @error "Error executing PRQL query" exception = e
        return nothing
    end
end

"""
    process_year_data(year::Int;
                     raw_db_path::String,
                     processed_db_path::String,
                     prql_files::Dict{String, String} = Dict(),
                     replace::Bool = false)

Main processing method that orchestrates the data transformation for a specific year.
This function:
1. Creates the circularity table structure
2. Loads product conversion mappings
3. Executes PRQL queries to extract and transform data
4. Populates the circularity indicators table

# Arguments
- `year::Int`: Year to process
- `raw_db_path::String`: Path to raw data DuckDB database
- `processed_db_path::String`: Path to processed data DuckDB database
- `prql_files::Dict{String, String}`: Dictionary mapping data types to PRQL file paths
  Example: Dict("production" => "path/to/production.prql", "trade" => "path/to/trade.prql")
- `replace::Bool`: Whether to replace existing table (default: false)

# Returns
- `Dict`: Processing results including success status and statistics
"""
function process_year_data(year::Int;
    raw_db_path::String,
    processed_db_path::String,
    prql_files::Dict{String,String}=Dict(),
    replace::Bool=false)

    results = Dict(
        :success => false,
        :table_created => false,
        :product_mapping_loaded => false,
        :queries_executed => Dict{String,Bool}(),
        :rows_processed => 0,
        :errors => String[]
    )

    try
        @info "Starting data processing for year $year"

        # Step 1: Create circularity table structure
        @info "Step 1/3: Creating circularity table structure"
        table_created = create_circularity_table(year; db_path=processed_db_path, replace=replace)

        if !table_created
            push!(results[:errors], "Failed to create circularity table")
            return results
        end
        results[:table_created] = true

        # Step 2: Load product conversion mapping
        @info "Step 2/3: Loading product conversion mappings"
        product_mapping = load_product_mapping(processed_db_path)

        if isnothing(product_mapping)
            push!(results[:errors], "Failed to load product mapping")
            return results
        end
        results[:product_mapping_loaded] = true

        # Step 3: Execute PRQL queries and process data
        @info "Step 3/3: Executing PRQL queries and processing data"

        # Example of how to process different data types
        for (data_type, prql_path) in prql_files
            @info "Processing $data_type data from $prql_path"

            # Execute PRQL query
            query_result = execute_prql_for_year(prql_path, raw_db_path, year)

            if isnothing(query_result)
                push!(results[:errors], "Failed to execute query for $data_type")
                results[:queries_executed][data_type] = false
                continue
            end

            @info "Query result for $data_type: $(nrow(query_result)) rows"
            if data_type == "production" && nrow(query_result) > 0
                # Show first few rows for debugging
                @info "Sample production data:"
                println(first(query_result, min(5, nrow(query_result))))

                # Check for non-zero values
                non_zero_volume = count(x -> !ismissing(x) && x > 0, query_result.production_volume_tonnes)
                non_zero_value = count(x -> !ismissing(x) && x > 0, query_result.production_value_eur)
                @info "Non-zero production counts: volume=$non_zero_volume, value=$non_zero_value"
            end

            results[:queries_executed][data_type] = true
            results[:rows_processed] += nrow(query_result)

            # Store query results for later processing
            if data_type == "production"
                production_data = query_result
                @info "Stored production data with $(nrow(production_data)) rows"
            elseif data_type == "trade"
                trade_data = query_result
                @info "Stored trade data with $(nrow(trade_data)) rows"
            end
        end

        # Step 4: Combine and transform data
        @info "Step 4/4: Combining and inserting data into circularity table"

        # Initialize DataFrames if they don't exist
        if !@isdefined(production_data)
            production_data = DataFrame()
        end
        if !@isdefined(trade_data)
            trade_data = DataFrame()
        end

        # Process and insert the data
        rows_inserted = combine_and_insert_data(
            year,
            production_data,
            trade_data,
            product_mapping,
            processed_db_path
        )

        if rows_inserted > 0
            @info "Successfully inserted $rows_inserted rows into circularity table"
            results[:rows_inserted] = rows_inserted
        else
            push!(results[:errors], "No data was inserted into the circularity table")
        end

        results[:success] = true
        @info "Data processing completed for year $year"

    catch e
        @error "Error in process_year_data" exception = e
        push!(results[:errors], string(e))
    end

    return results
end

"""
    map_product_codes(data_df::DataFrame, mapping_df::DataFrame;
                     source_code_col::Symbol = :product_code,
                     source_type::Symbol = :prodcom_code)

Maps product codes in a DataFrame using the product conversion table.

# Arguments
- `data_df::DataFrame`: Data containing product codes to map
- `mapping_df::DataFrame`: Product mapping/conversion table
- `source_code_col::Symbol`: Column name in data_df containing product codes
- `source_type::Symbol`: Type of source codes (:prodcom_code or :hs_codes)

# Returns
- `DataFrame`: Original data with additional mapped columns
"""
function map_product_codes(data_df::DataFrame, mapping_df::DataFrame;
    source_code_col::Symbol=:product_code,
    source_type::Symbol=:prodcom_code)

    # Create a copy to avoid modifying original
    result_df = copy(data_df)

    # Initialize new columns
    result_df[!, :product_name] = Vector{Union{String,Missing}}(missing, nrow(result_df))
    result_df[!, :product_id] = Vector{Union{Int,Missing}}(missing, nrow(result_df))

    # Map each row
    for i in 1:nrow(result_df)
        code = result_df[i, source_code_col]

        if !ismissing(code) && !isnothing(code)
            # Find matching product
            if source_type == :prodcom_code
                matches = mapping_df[mapping_df[!, :prodcom_code].==code, :]
            else  # :hs_codes
                matches = mapping_df[occursin.(string(code), mapping_df[!, :hs_codes]), :]
            end

            if nrow(matches) > 0
                result_df[i, :product_name] = matches[1, :product]
                result_df[i, :product_id] = matches[1, :product_id]
            end
        end
    end

    return result_df
end

"""
    inspect_raw_tables(db_path::String, year::Int; show_sample::Bool = false)

Inspects the structure of raw database tables for a given year.
Shows column names and types for PRODCOM and COMEXT tables.

# Arguments
- `db_path::String`: Path to the raw DuckDB database
- `year::Int`: Year to inspect
- `show_sample::Bool`: Whether to show sample data (default: false)

# Returns
- `Dict`: Dictionary containing table information for each dataset
"""
function inspect_raw_tables(db_path::String, year::Int; show_sample::Bool=false)
    results = Dict{String,Any}()

    try
        @info "Inspecting raw database tables for year $year"

        # Connect to database
        db_conn = DuckDB.DB(db_path)
        con = DBInterface.connect(db_conn)

        # Table names to inspect
        table_names = [
            "prodcom_ds_056120_$year",
            "prodcom_ds_056121_$year",
            "comext_DS_045409_$year"
        ]

        for table_name in table_names
            @info "Inspecting table: $table_name"

            # Check if table exists
            exists_query = """
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_name = '$table_name'
            """

            exists_result = DBInterface.execute(con, exists_query) |> DataFrame

            if exists_result.cnt[1] == 0
                @warn "Table $table_name does not exist"
                results[table_name] = Dict("exists" => false)
                continue
            end

            # Get column information
            columns_query = """
            SELECT
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_name = '$table_name'
            ORDER BY ordinal_position
            """

            columns_df = DBInterface.execute(con, columns_query) |> DataFrame

            # Get row count
            count_query = "SELECT COUNT(*) as row_count FROM $table_name"
            count_result = DBInterface.execute(con, count_query) |> DataFrame

            table_info = Dict(
                "exists" => true,
                "columns" => columns_df,
                "row_count" => count_result.row_count[1]
            )

            # Get sample data if requested
            if show_sample && count_result.row_count[1] > 0
                sample_query = "SELECT * FROM $table_name LIMIT 5"
                sample_df = DBInterface.execute(con, sample_query) |> DataFrame
                table_info["sample_data"] = sample_df
            end

            results[table_name] = table_info

            # Print summary
            println("\nTable: $table_name")
            println("Rows: $(table_info["row_count"])")
            println("Columns:")
            for row in eachrow(columns_df)
                println("  - $(row.column_name): $(row.data_type) $(row.is_nullable == "YES" ? "(nullable)" : "")")
            end
        end

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        return results

    catch e
        @error "Error inspecting raw tables" exception = e
        return results
    end
end

"""
    combine_and_insert_data(year::Int, production_df::DataFrame, trade_df::DataFrame,
                           mapping_df::DataFrame, db_path::String)

Combines production and trade data, maps product codes, calculates indicators,
and inserts into the circularity table.

# Arguments
- `year::Int`: Year being processed
- `production_df::DataFrame`: Production data from PRQL query
- `trade_df::DataFrame`: Trade data from PRQL query
- `mapping_df::DataFrame`: Product mapping table
- `db_path::String`: Path to the processed database

# Returns
- `Int`: Number of rows inserted
"""
function combine_and_insert_data(year::Int, production_df::DataFrame, trade_df::DataFrame,
                                mapping_df::DataFrame, db_path::String)
    try
        @info "Combining production and trade data for year $year"

        # Get table name
        table_name = get_circularity_table_name(year)

        # Initialize results DataFrame
        results_df = DataFrame()

        # Process production data if available
        if nrow(production_df) > 0
            @info "Processing $(nrow(production_df)) production records"

            # Debug: Show incoming production data
            @info "DEBUG: Incoming production_df summary:"
            println("Columns: ", names(production_df))
            println("First 5 rows:")
            println(first(production_df, min(5, nrow(production_df))))

            # Check for non-zero values before mapping
            non_zero_vol_before = count(x -> !ismissing(x) && x > 0, production_df.production_volume_tonnes)
            non_zero_val_before = count(x -> !ismissing(x) && x > 0, production_df.production_value_eur)
            @info "DEBUG: Before mapping - Non-zero counts: volume=$non_zero_vol_before, value=$non_zero_val_before"

            # Show some specific examples of non-zero records
            if non_zero_vol_before > 0
                non_zero_examples = filter(row -> !ismissing(row.production_volume_tonnes) && row.production_volume_tonnes > 0, production_df)
                @info "DEBUG: Example non-zero volume records:"
                println(first(non_zero_examples, min(3, nrow(non_zero_examples))))
            end

            # Map product codes to standardized products
            prod_mapped = map_product_codes(
                production_df,
                mapping_df,
                source_code_col=:product_code,
                source_type=:prodcom_code
            )

            # Debug: Show mapping results
            @info "DEBUG: After mapping - $(nrow(prod_mapped)) records"
            println("Columns after mapping: ", names(prod_mapped))

            # Check for non-zero values after mapping
            non_zero_vol_after = count(x -> !ismissing(x) && x > 0, prod_mapped.production_volume_tonnes)
            non_zero_val_after = count(x -> !ismissing(x) && x > 0, prod_mapped.production_value_eur)
            @info "DEBUG: After mapping - Non-zero counts: volume=$non_zero_vol_after, value=$non_zero_val_after"

            # Select and rename columns for insertion
            prod_for_insert = select(prod_mapped,
                :product_code => :product_code,
                :product_name => :product_name,
                :year => :year,
                :geo => :geo,
                :level => :level,
                :production_volume_tonnes => :production_volume_tonnes,
                :production_value_eur => :production_value_eur
            )

            # Debug: Final check before assignment
            @info "DEBUG: Final prod_for_insert - $(nrow(prod_for_insert)) records"
            non_zero_vol_final = count(x -> !ismissing(x) && x > 0, prod_for_insert.production_volume_tonnes)
            non_zero_val_final = count(x -> !ismissing(x) && x > 0, prod_for_insert.production_value_eur)
            @info "DEBUG: Final non-zero counts: volume=$non_zero_vol_final, value=$non_zero_val_final"

            # Show sample of final data
            println("Sample of final production data:")
            println(first(prod_for_insert, min(5, nrow(prod_for_insert))))

            results_df = prod_for_insert
        end

        # Process trade data if available
        if nrow(trade_df) > 0
            @info "Processing $(nrow(trade_df)) trade records"

            # Map product codes for trade data (HS codes)
            trade_mapped = map_product_codes(
                trade_df,
                mapping_df,
                source_code_col=:product_code,
                source_type=:hs_codes
            )

            # If we have production data, merge with trade data
            if nrow(results_df) > 0
                # Merge on product_code, geo, year, level
                results_df = outerjoin(
                    results_df,
                    trade_mapped,
                    on = [:product_code, :geo, :year, :level],
                    makeunique = true
                )

                # Handle duplicate columns from join
                for col in [:product_name, :product_id]
                    if "$(col)_1" in names(results_df)
                        results_df[!, col] = coalesce.(results_df[!, col], results_df[!, "$(col)_1"])
                        select!(results_df, Not("$(col)_1"))
                    end
                end
            else
                # Only trade data available
                trade_for_insert = select(trade_mapped,
                    :product_code => :product_code,
                    :product_name => :product_name,
                    :year => :year,
                    :geo => :geo,
                    :level => :level,
                    :import_volume_tonnes => :import_volume_tonnes,
                    :import_value_eur => :import_value_eur,
                    :export_volume_tonnes => :export_volume_tonnes,
                    :export_value_eur => :export_value_eur
                )
                results_df = trade_for_insert
            end
        end

        # Calculate apparent consumption
        if nrow(results_df) > 0
            # Initialize missing columns with appropriate default values
            for col in [:production_volume_tonnes, :production_value_eur,
                       :import_volume_tonnes, :import_value_eur,
                       :export_volume_tonnes, :export_value_eur]
                if !(col in names(results_df))
                    results_df[!, col] = fill(0.0, nrow(results_df))
                else
                    # Replace missing with 0
                    results_df[!, col] = coalesce.(results_df[!, col], 0.0)
                end
            end

            # Calculate apparent consumption
            results_df[!, :apparent_consumption_tonnes] =
                results_df.production_volume_tonnes .+
                results_df.import_volume_tonnes .-
                results_df.export_volume_tonnes

            results_df[!, :apparent_consumption_value_eur] =
                results_df.production_value_eur .+
                results_df.import_value_eur .-
                results_df.export_value_eur

            # Add placeholder circularity indicators (to be calculated later)
            results_df[!, :current_circularity_rate_pct] = fill(missing, nrow(results_df))
            results_df[!, :potential_circularity_rate_pct] = fill(missing, nrow(results_df))
            results_df[!, :estimated_material_savings_tonnes] = fill(missing, nrow(results_df))
            results_df[!, :estimated_monetary_savings_eur] = fill(missing, nrow(results_df))

            # Ensure product_name is not missing
            results_df[!, :product_name] = coalesce.(results_df.product_name, "Unknown Product")

            # Filter to only include mapped products (where we have product names)
            results_df = results_df[results_df.product_name .!= "Unknown Product", :]

            @info "Prepared $(nrow(results_df)) rows for insertion"

            # Insert data into database
            if nrow(results_df) > 0
                db_conn = DuckDB.DB(db_path)
                con = DBInterface.connect(db_conn)

                try
                    # Use DatabaseAccess function to write the data
                    DatabaseAccess.write_duckdb_table!(results_df, db_path, table_name)

                    # Get actual row count
                    count_result = DBInterface.execute(con,
                        "SELECT COUNT(*) as cnt FROM $table_name"
                    ) |> DataFrame

                    rows_inserted = count_result.cnt[1]
                    @info "Successfully inserted $rows_inserted rows into $table_name"

                    return rows_inserted

                finally
                    DBInterface.close!(con)
                    DBInterface.close!(db_conn)
                end
            end
        end

        return 0

    catch e
        @error "Error combining and inserting data" exception = e
        return 0
    end
end

# Arguments:
# - `db_path::String`: Path to the raw database
# - `year::Int`: Year to debug
# - `product_code::Union{String,Nothing}`: Optional specific product code to filter
#
# Returns:
# - `Dict`: Debug information showing data at each stage of processing
function debug_production_pipeline(db_path::String, year::Int; product_code::Union{String,Nothing}=nothing)
    debug_info = Dict{String,Any}()

    try
        @info "Debugging production pipeline for year $year"

        # Step 1: Check raw table structure and sample data
        db_conn = DuckDB.DB(db_path)
        con = DBInterface.connect(db_conn)

        table_name = "prodcom_ds_056120_$year"

        # Get sample of raw data
        sample_query = """
        SELECT *
        FROM $table_name
        WHERE indicators IN ('PRODQNT', 'PRODVAL')
        $(product_code !== nothing ? "AND prccode = '$product_code'" : "")
        LIMIT 20
        """

        raw_sample = DBInterface.execute(con, sample_query) |> DataFrame
        debug_info["raw_sample"] = raw_sample

        @info "Raw data sample ($(nrow(raw_sample)) rows):"
        println(raw_sample)

        # Step 2: Check distinct values for key columns
        distinct_query = """
        SELECT
            indicators,
            COUNT(*) as count,
            COUNT(DISTINCT value) as distinct_values,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM $table_name
        WHERE indicators IN ('PRODQNT', 'PRODVAL')
        $(product_code !== nothing ? "AND prccode = '$product_code'" : "")
        GROUP BY indicators
        """

        distinct_values = DBInterface.execute(con, distinct_query) |> DataFrame
        debug_info["distinct_values"] = distinct_values

        @info "Value distribution by indicator:"
        println(distinct_values)

        # Step 3: Execute PRQL and check result
        prql_path = joinpath(dirname(@__FILE__), "prql", "production_data.prql")
        production_df = execute_prql_for_year(prql_path, db_path, year)

        if product_code !== nothing
            production_df = filter(row -> row.product_code == product_code, production_df)
        end

        debug_info["prql_result"] = production_df
        debug_info["prql_result_rows"] = nrow(production_df)

        @info "PRQL result ($(nrow(production_df)) rows):"
        if nrow(production_df) > 0
            println(first(production_df, min(10, nrow(production_df))))
        else
            println("No data returned from PRQL query")
        end

        # Step 4: Check specific product codes
        if nrow(production_df) > 0
            prod_summary = combine(
                groupby(production_df, :product_code),
                :production_volume_tonnes => (x -> sum(skipmissing(x))) => :total_volume,
                :production_value_eur => (x -> sum(skipmissing(x))) => :total_value,
                nrow => :count
            )
            debug_info["product_summary"] = prod_summary

            @info "Production summary by product code:"
            println(first(prod_summary, min(10, nrow(prod_summary))))
        end

        # Step 5: Check the value column content more closely
        value_check_query = """
        SELECT
            prccode,
            indicators,
            value,
            COUNT(*) as count
        FROM $table_name
        WHERE indicators IN ('PRODQNT', 'PRODVAL')
        $(product_code !== nothing ? "AND prccode = '$product_code'" : "")
        AND value NOT IN ('kg', 'p/st', 'm', 'm2', 'm3', 'l', 'hl', 'ct/l')
        GROUP BY prccode, indicators, value
        ORDER BY count DESC
        LIMIT 20
        """

        value_distribution = DBInterface.execute(con, value_check_query) |> DataFrame
        debug_info["value_distribution"] = value_distribution

        @info "Value column distribution:"
        println(value_distribution)

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        return debug_info

    catch e
        @error "Error in debug_production_pipeline" exception = e
        return debug_info
    end
end

end # module CircularityProcessor
