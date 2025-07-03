module CircularityProcessor

using DataFrames
using DuckDB, DBInterface
using Dates

# Import necessary modules from parent
using ..DatabaseAccess
using ..ProductConversionTables

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

            results[:queries_executed][data_type] = true
            results[:rows_processed] += nrow(query_result)

            # TODO: Transform and insert data into circularity table
            # This would involve mapping the query results to the circularity table structure
            # and handling product code conversions using the product_mapping DataFrame
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

end # module CircularityProcessor
