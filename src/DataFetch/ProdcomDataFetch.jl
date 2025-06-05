module ProdcomDataFetch

using HTTP, JSON3, DataFrames, Dates, DuckDB, CSV
using ..DatabaseAccess: write_large_duckdb_table!, recreate_duckdb_database

export fetch_prodcom_data

"""
    fetch_prodcom_data(years_range="1995-2023")

Fetches PRODCOM data from Eurostat API for specified datasets and year range.
Data is saved to DuckDB tables in the raw database.
"""
function fetch_prodcom_data(years_range="1995-2023", custom_datasets=nothing)
    # Parse years
    years = split(years_range, "-")
    start_year = parse(Int, years[1])
    end_year = parse(Int, years[2])

    # Datasets to fetch
    datasets = isnothing(custom_datasets) ? ["ds-056120", "ds-056121"] : custom_datasets

    # Database path - use absolute path for reliability
    db_path = abspath(joinpath(dirname(dirname(dirname(@__FILE__))), "CirQuant-database/raw/CirQuant_1995-2023.duckdb"))
    @info "Using database path: $db_path"

    # Track success/failure statistics
    stats = Dict(
        :total_datasets => length(datasets) * (end_year - start_year + 1),
        :successful => 0,
        :failed => 0,
        :rows_processed => 0
    )

    # Ensure the database directory exists
    db_dir = dirname(db_path)
    if !isdir(db_dir)
        @info "Creating database directory: $db_dir"
        mkpath(db_dir)
    end

    # Create a log directory for error dumps
    log_dir = abspath(joinpath(dirname(dirname(dirname(@__FILE__))), "CirQuant-database/logs"))
    if !isdir(log_dir)
        @info "Creating log directory: $log_dir"
        mkpath(log_dir)
    end

    # Check if database exists and try to validate it
    if isfile(db_path)
        @info "Checking database integrity: $db_path"
        db_valid = try
            con = DuckDB.DB(db_path)
            # Test query to check if database is accessible
            DuckDB.query(con, "SELECT 1")
            DBInterface.close!(con)
            true
        catch e
            @warn "Database appears to be corrupted or inaccessible" exception = e
            false
        end

        # If database seems corrupted, log a warning but continue without recreation
        # to avoid losing existing data
        if !db_valid
            @warn "Database appears to have issues, but will continue without recreation to preserve existing tables"
            @info "New data will be added to backup CSV files if database writes fail"
            # Note: We intentionally avoid recreating the database to prevent data loss
        end
    end

    # Process each dataset and year
    for dataset in datasets
        @info "Processing dataset: $dataset"

        for year in start_year:end_year
            @info "Fetching data for year: $year"

            # Construct API URL
            url = "https://ec.europa.eu/eurostat/api/comext/dissemination/statistics/1.0/data/$dataset?time=$year"

            try
                # Make the API request with timeout and retries
                response = nothing
                retries = 3

                for attempt in 1:retries
                    try
                        response = HTTP.get(url, readtimeout=120, retries=2)
                        break
                    catch retry_err
                        if attempt < retries
                            @warn "Attempt $attempt failed, retrying in 5 seconds..." exception = retry_err
                            sleep(5)
                        else
                            rethrow(retry_err)
                        end
                    end
                end

                if response !== nothing && response.status == 200
                    # Parse JSON
                    data = JSON3.read(response.body)

                    # Convert to DataFrame
                    start_time = time()
                    df = process_eurostat_data(data, dataset, year)
                    processing_time = round(time() - start_time, digits=2)

                    # Save to DuckDB
                    table_name = "prodcom_$(replace(dataset, "-" => "_"))_$year"
                    if !isempty(df)
                        # Record row count for statistics
                        stats[:rows_processed] += nrow(df)

                        # Write to database
                        @info "Writing $(nrow(df)) rows to database (processed in $processing_time seconds)"

                        # Try writing with error handling
                        success = false
                        try
                            # Check if the database appears to be valid
                            db_valid = try
                                con = DuckDB.DB(db_path)
                                DuckDB.query(con, "SELECT 1")
                                DBInterface.close!(con)
                                true
                            catch db_check_error
                                @warn "Database seems inaccessible, but will continue without recreation" exception = db_check_error
                                @info "Will attempt to write data with fallback to CSV backup if needed"
                                false
                            end
                            
                            # We intentionally do NOT recreate the database here to preserve existing tables

                            # Handle both possible outcomes (success or error)
                            success = try
                                # Try to write to DuckDB but catch any error
                                write_large_duckdb_table!(df, db_path, table_name)
                            catch db_error
                                @error "Failed to write to DuckDB through main function" exception = db_error table = table_name
                                # Signal failure but don't propagate error
                                false
                            end

                            if success === true  # Explicit comparison to ensure boolean true
                                @info "✓ Successfully saved data to table $table_name"
                                stats[:successful] += 1
                            else
                                # Manual fallback if automatic ones didn't work
                                @warn "Automatic methods failed, saving to backup CSV"
                                stats[:failed] += 1

                                # Save problematic dataframe for inspection and backup
                                backup_dir = joinpath(log_dir, "backups")
                                mkpath(backup_dir)
                                backup_file = joinpath(backup_dir, "backup_$(dataset)_$(year)_$(round(Int, time())).csv")

                                try
                                    # Save complete dataset as backup
                                    @info "Saving emergency backup to $backup_file"
                                    CSV.write(backup_file, df)
                                    @info "✓ Successfully saved backup data to $backup_file"
                                    @info "Data can be manually imported later with: COPY \"$(table_name)\" FROM '$(backup_file)' (FORMAT CSV, HEADER);"
                                catch csv_err
                                    @warn "Failed to save complete backup data" exception = csv_err

                                    # Last resort: try to save a small sample
                                    sample_file = joinpath(log_dir, "sample_$(dataset)_$(year)_$(round(Int, time())).csv")
                                    try
                                        CSV.write(sample_file, df[1:min(1000, nrow(df)), :])
                                        @info "Saved sample of problematic data to $sample_file"
                                    catch sample_err
                                        @error "All backup methods failed" exception = sample_err
                                    end
                                end
                            end
                        catch outer_err
                            # Catch any unexpected errors to prevent the entire process from failing
                            @error "Unexpected error in database writing process" exception = outer_err
                            stats[:failed] += 1
                        end
                    else
                        @warn "No data to save for $dataset, year $year"
                        stats[:failed] += 1
                    end
                else
                    status = response !== nothing ? response.status : "no response"
                    @error "HTTP error: status $status for $dataset, year $year"
                    @info "Continuing with next year/dataset to avoid losing progress"
                    stats[:failed] += 1
                end
            catch e
                @error "Error processing $dataset for year $year" exception = e
                @info "Continuing with next year/dataset to avoid losing progress"
                stats[:failed] += 1

                # Create an error log with details
                error_log = joinpath(log_dir, "error_$(dataset)_$(year)_$(round(Int, time())).txt")
                try
                    open(error_log, "w") do f
                        println(f, "Error processing $dataset for year $year")
                        println(f, "URL: $url")
                        println(f, "Exception: $e")
                        println(f, "Stacktrace:")
                        for (i, frame) in enumerate(stacktrace())
                            println(f, "  [$i] $frame")
                        end
                    end
                    @info "Error details saved to $error_log"
                catch log_err
                    @warn "Failed to save error log" exception = log_err
                end
            end
        end
    end

    # Report final statistics
    @info "PRODCOM data fetching completed:"
    @info "  Total datasets: $(stats[:total_datasets])"
    @info "  Successfully processed: $(stats[:successful]) ($(round(stats[:successful]/stats[:total_datasets]*100, digits=1))%)"
    @info "  Failed: $(stats[:failed])"
    @info "  Total rows processed: $(stats[:rows_processed])"

    return stats
end

"""
    linear_index_to_nd_indices(idx, dimensions)

Convert a linear index to n-dimensional indices based on the provided dimensions.
This is essential for correctly interpreting EUROSTAT API keys.

Parameters:
- `idx`: Linear index (1-based)
- `dimensions`: Array of dimension sizes in the order they appear in the API

Returns an array of indices corresponding to each dimension.
"""
function linear_index_to_nd_indices(idx, dimensions)
    if idx < 1 || idx > prod(dimensions)
        @warn "Index $idx out of bounds for dimensions $dimensions"
        return fill(1, length(dimensions))  # Return default indices
    end
    
    indices = Int[]
    remaining = idx - 1  # Convert to 0-based for calculation
    
    # Column-major order (Julia standard)
    stride = 1
    for dim_size in dimensions
        index = (remaining ÷ stride) % dim_size + 1
        push!(indices, index)
        stride *= dim_size
    end
    
    return indices
end

"""
    process_eurostat_data(data, dataset, year)

Process Eurostat API JSON response into a properly structured DataFrame.
Maintains all dimensions as columns with one value per combination.

Returns a DataFrame with the processed data. Each row represents a unique
combination of dimension values, with values organized in columns.

The processing has two stages:
1. Initial extraction where each API value becomes one row
2. Optional pivoting where certain dimensions (like units) become columns
"""
function process_eurostat_data(data, dataset, year)
    # Safety check for malformed data
    if !haskey(data, :dimension) || !haskey(data, :value)
        @error "Malformed API response: missing dimension or value keys"
        return DataFrame()
    end

    # Extract dimensions and values
    dimensions = data.dimension
    values = data.value

    # Safety check for empty data
    if isempty(values)
        @warn "Empty dataset received from API for $dataset, year $year"
        return DataFrame()
    end

    # Count total values for verification
    value_count = length(values)
    @info "Processing $value_count values from Eurostat API"

    # Extract dimension information
    dimension_info = Dict{String,Dict{Int,String}}()
    dimension_names = String[]
    dimension_sizes = Int[]

    for (dim_name, dim_data) in pairs(dimensions)
        dim_str = string(dim_name)
        push!(dimension_names, dim_str)

        # Create index mapping for this dimension
        if haskey(dim_data, :category) && haskey(dim_data.category, :index)
            dimension_info[dim_str] = Dict{Int,String}()
            
            # Get dimension size (count of unique values)
            dim_size = length(dim_data.category.index)
            push!(dimension_sizes, dim_size)
            
            for (cat_label, cat_idx) in pairs(dim_data.category.index)
                # Convert from 0-based to 1-based indexing
                idx = Int(cat_idx) + 1
                dimension_info[dim_str][idx] = string(cat_label)
            end
        else
            # Default size of 1 if dimension structure is unknown
            push!(dimension_sizes, 1)
        end
    end
    
    @info "Dimensions in order: $(join(dimension_names, ", "))"
    @info "Dimension sizes: $(join(dimension_sizes, ", "))"

    # Process each value directly to rows (no intermediate grouping)
    rows = []
    processed = 0

    for (key, value) in pairs(values)
        # Create a new row for this observation
        row = Dict{Symbol,Any}()

        # Add metadata
        row[:dataset] = dataset
        row[:year] = year
        row[:fetch_date] = now()
        row[:data_source] = "Eurostat PRODCOM API"
        row[:original_key] = string(key)

        # Add the actual value, handling special values
        if value isa String && (value == ":C" || value == ":c" || value == ":" || value == "-")
            row[:value] = missing
            row[:original_value] = value  # Preserve original for reference
        else
            row[:value] = value
        end

        # Parse the key as a linear index in a multi-dimensional array
        key_int = try
            parse(Int, string(key))
        catch e
            @warn "Failed to parse key: $key" exception = e
            continue  # Skip this value if key can't be parsed
        end

        # Convert linear index to multi-dimensional indices
        indices = linear_index_to_nd_indices(key_int, dimension_sizes)

        # Map indices to dimension values
        for (i, dim_name) in enumerate(dimension_names)
            if i <= length(indices) && haskey(dimension_info, dim_name)
                idx = indices[i]
                # Get the dimension value, or use a placeholder if unknown
                if haskey(dimension_info[dim_name], idx)
                    row[Symbol(dim_name)] = dimension_info[dim_name][idx]
                else
                    row[Symbol(dim_name)] = "unknown_$(idx)"
                end
            else
                # Dimension not found or index out of range
                row[Symbol(dim_name)] = "missing"
            end
        end

        push!(rows, row)

        # Track progress
        processed += 1
        if processed % 10000 == 0
            @info "Processed $processed/$value_count values ($(round(processed/value_count*100, digits=1))%)"
        end
    end

    if isempty(rows)
        @warn "No data processed for $dataset, year $year"
        return DataFrame()
    end

    # Convert to DataFrame
    raw_df = DataFrame(rows)

    # Clean up any problematic columns for DuckDB compatibility
    for col in names(raw_df)
        # Handle columns with mixed types
        if eltype(raw_df[!, col]) == Any
            @info "Converting Any-type column $col to String for DuckDB compatibility"
            raw_df[!, col] = [ismissing(x) ? missing : string(x) for x in raw_df[!, col]]
        end
    end

    @info "Created raw dataframe with $(nrow(raw_df)) rows and $(ncol(raw_df)) columns"

    # Validate data preservation
    if nrow(raw_df) == value_count
        @info "✓ All values preserved: $value_count values in original data, $(nrow(raw_df)) rows in dataframe"
    else
        @error "Data loss detected: $value_count values in original data, but only $(nrow(raw_df)) rows in dataframe"
    end

    # Second-stage processing: Pivot the dataframe to create a more analysis-friendly structure
    # Pivot the dataframe to create a more analysis-friendly structure
    # Identify dimensions that should be used for column headers versus row identifiers

    # For DuckDB compatibility, check if the dataframe has problematic types
    for col in names(raw_df)
        if eltype(raw_df[!, col]) == Any
            # Convert Any columns to String to avoid DuckDB serialization issues
            @info "Converting Any-type column $col to String for DuckDB compatibility"
            raw_df[!, col] = [ismissing(x) ? missing : string(x) for x in raw_df[!, col]]
        end

        # Handle special string values that may cause DuckDB problems
        if eltype(raw_df[!, col]) >: String
            # Replace problematic values with missing
            mask = [x isa String && (x == ":C" || x == ":c" || x == ":" || x == "-" ||
                                     x == "null" || x == "NULL" || x == "NaN" || x == "Inf" || x == "-Inf") for x in raw_df[!, col]]
            if any(mask)
                @info "Replacing $(sum(mask)) special values in column $col with missing"
                raw_df[mask, col] .= missing
            end
        end
    end

    # Check if pivoting would create a reasonable number of columns
    # Skip pivoting for very large datasets - DuckDB has issues with too many columns
    if nrow(raw_df) > 100000  # Lower threshold to avoid DuckDB issues
        @warn "Large dataset detected ($(nrow(raw_df)) rows) - skipping pivoting to avoid DuckDB issues"
        return raw_df
    end

    # Try to determine the best dimensions to pivot on
    # Typically 'unit' is used for column headers, but we should detect other measure dimensions
    possible_pivot_cols = ["unit", "indic_bt"]  # Potential column dimensions

    # Check which potential pivot columns exist in the dataframe
    available_pivot_cols = filter(col -> Symbol(col) in propertynames(raw_df), possible_pivot_cols)

    # Analyze cardinality of dimensions to make intelligent pivot decisions
    dimension_counts = Dict()
    for dim in available_pivot_cols
        if Symbol(dim) in propertynames(raw_df)
            unique_values = unique(filter(!ismissing, raw_df[!, Symbol(dim)]))
            dimension_counts[dim] = length(unique_values)
            @info "Dimension '$dim' has $(length(unique_values)) unique values"
        end
    end

    # Choose dimensions with reasonable cardinality for pivoting (not too many unique values)
    # This prevents explosion of columns - stricter limit to avoid DuckDB issues
    pivot_cols = [dim for (dim, count) in dimension_counts if count <= 5]  # Stricter limit

    if isempty(pivot_cols)
        # If no suitable pivot columns found, try using just unit as fallback
        if "unit" in available_pivot_cols && dimension_counts["unit"] <= 8  # Stricter limit
            pivot_cols = ["unit"]
            @info "Using 'unit' dimension for pivoting"
        else
            @info "No suitable dimensions for pivoting - all dimensions kept as rows"
            return raw_df
        end
    else
        @info "Selected pivot dimensions: $(join(pivot_cols, ", "))"
    end

    # Identify row dimensions (all except pivot columns and value/metadata columns)
    metadata_cols = [:dataset, :year, :fetch_date, :data_source, :original_key, :value]
    row_dims = [col for col in propertynames(raw_df)
                if !(col in metadata_cols) &&
                !(string(col) in pivot_cols)]

    @info "Pivoting data: Using $(length(row_dims)) dimensions for rows, $(length(pivot_cols)) for columns"

    # Group by row dimensions
    grouped = Dict()
    row_count = 0

    # Track unique values for each pivot dimension to create consistent column names
    pivot_values = Dict(dim => Set{String}() for dim in pivot_cols)

    for row in eachrow(raw_df)
        # Create a key from row dimensions
        row_key = Tuple(row[dim] for dim in row_dims)

        # Create a column key from pivot dimensions, handling missing values
        col_key_parts = []
        for dim in pivot_cols
            val = row[Symbol(dim)]
            # Replace missing or empty values with "unknown" for column naming
            push!(col_key_parts, ismissing(val) || val == "" ? "unknown" : string(val))
            # Track this value for the dimension
            if !ismissing(val) && val != ""
                push!(pivot_values[dim], string(val))
            end
        end
        col_key = Tuple(col_key_parts)

        # Initialize this group if it doesn't exist
        if !haskey(grouped, row_key)
            grouped[row_key] = Dict()
            # Add metadata
            grouped[row_key][:metadata] = Dict(
                :dataset => row.dataset,
                :year => row.year,
                :fetch_date => row.fetch_date,
                :data_source => row.data_source
            )
            row_count += 1
        end

        # Create a descriptive column name with dimension names included
        col_parts = ["$(dim)_$(col_key[i])" for (i, dim) in enumerate(pivot_cols)]
        col_name = join(col_parts, "_")

        # Store the value with its column key
        # Only store non-missing values
        if !ismissing(row.value)
            if haskey(grouped[row_key], col_name) && grouped[row_key][col_name] != row.value
                @warn "Duplicate value found for dimension combination" row_key col_key existing = grouped[row_key][col_name] new = row.value
            end
            grouped[row_key][col_name] = row.value
        end
    end

    @info "Created $row_count grouped rows from $(nrow(raw_df)) original rows"
    for (dim, values) in pivot_values
        @info "Pivot dimension '$dim' has $(length(values)) values: $(join(collect(values)[1:min(5, length(values))], ", "))$(length(values) > 5 ? "..." : "")"
    end

    # Find all possible column keys to ensure all rows have the same columns
    all_column_keys = Set{String}()
    for (_, data) in grouped
        for col_key in keys(data)
            if col_key != :metadata
                push!(all_column_keys, col_key)
            end
        end
    end
    @info "Found $(length(all_column_keys)) unique value columns after pivoting"

    # Convert grouped data to rows
    pivoted_rows = []

    for (row_key, data) in grouped
        # Create a new row
        pivoted_row = Dict{Symbol,Any}()

        # Add metadata
        for (k, v) in data[:metadata]
            pivoted_row[k] = v
        end

        # Add row dimensions
        for (i, dim) in enumerate(row_dims)
            pivoted_row[dim] = row_key[i]
        end

        # Initialize all possible columns with missing values
        for col_key in all_column_keys
            pivoted_row[Symbol("value_$(col_key)")] = missing
        end

        # Add the values this row actually has
        for (col_key, val) in data
            if col_key != :metadata
                # Clean up problematic values for DuckDB
                if val isa String && (val == ":C" || val == ":c" || val == ":" || val == "-")
                    pivoted_row[Symbol("value_$(col_key)")] = missing
                else
                    pivoted_row[Symbol("value_$(col_key)")] = val
                end
            end
        end

        push!(pivoted_rows, pivoted_row)
    end

    # Convert to DataFrame
    if isempty(pivoted_rows)
        @warn "No data after pivoting for $dataset, year $year"
        return raw_df  # Return the original dataframe if pivoting failed
    end

    pivoted_df = DataFrame(pivoted_rows)

    @info "Created pivoted dataframe with $(nrow(pivoted_df)) rows and $(ncol(pivoted_df)) columns"

    # Final data preservation check
    value_cols = [col for col in propertynames(pivoted_df) if startswith(string(col), "value_")]
    total_values = sum(count(!ismissing, pivoted_df[!, col]) for col in value_cols)

    # Count special values that were converted to missing
    special_value_count = count(rows) do row
        haskey(row, :original_value) && !ismissing(row[:original_value])
    end

    if total_values + special_value_count >= value_count
        @info "✓ Data fully preserved after pivoting: $value_count values in original data, $total_values numeric values + $special_value_count special values in pivoted dataframe"
    else
        @warn "Possible data discrepancy after pivoting: $value_count values in original data, $total_values values + $special_value_count special values in pivoted dataframe"
    end

    # Ensure dataframe is compatible with DuckDB
    @info "Preparing dataframe for DuckDB storage"
    for col in names(pivoted_df)
        col_type = eltype(pivoted_df[!, col])
        if col_type == Any
            # Convert Any columns to strings for DuckDB compatibility
            pivoted_df[!, col] = [ismissing(x) ? missing : string(x) for x in pivoted_df[!, col]]
        end
    end

    return pivoted_df
end

end # module
