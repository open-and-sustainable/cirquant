module ProdcomDataFetch

using HTTP, JSON3, DataFrames, Dates, DuckDB
using ..DatabaseAccess: write_large_duckdb_table!

export fetch_prodcom_data

"""
    fetch_prodcom_data(years_range="1995-2023")

Fetches PRODCOM data from Eurostat API for specified datasets and year range.
Data is saved to DuckDB tables in the raw database.
"""
function fetch_prodcom_data(years_range="1995-2023")
    # Parse years
    years = split(years_range, "-")
    start_year = parse(Int, years[1])
    end_year = parse(Int, years[2])
    
    # Datasets to fetch
    datasets = ["ds-056120", "ds-056121"]
    
    # Database path - use absolute path for reliability
    db_path = abspath(joinpath(dirname(dirname(dirname(@__FILE__))), "CirQuant-database/raw/CirQuant_1995-2023.duckdb"))
    @info "Using database path: $db_path"
    
    # Ensure the database directory exists
    db_dir = dirname(db_path)
    if !isdir(db_dir)
        @info "Creating database directory: $db_dir"
        mkpath(db_dir)
    end
    
    # Process each dataset and year
    for dataset in datasets
        @info "Processing dataset: $dataset"
        
        for year in start_year:end_year
            @info "Fetching data for year: $year"
            
            # Construct API URL
            url = "https://ec.europa.eu/eurostat/api/comext/dissemination/statistics/1.0/data/$dataset?time=$year"
            
            try
                # Make the API request
                response = HTTP.get(url)
                
                if response.status == 200
                    # Parse JSON
                    data = JSON3.read(response.body)
                    
                    # Convert to DataFrame
                    df = process_eurostat_data(data, dataset, year)
                    
                    # Save to DuckDB
                    table_name = "prodcom_$(replace(dataset, "-" => "_"))_$year"
                    if !isempty(df)
                        try
                            write_large_duckdb_table!(df, db_path, table_name)
                            @info "Saved $(nrow(df)) rows to table $table_name"
                        catch db_error
                            @error "Failed to write to DuckDB" exception=db_error table=table_name
                        end
                    else
                        @warn "No data to save for $dataset, year $year"
                    end
                else
                    @error "HTTP error: status $(response.status)"
                end
            catch e
                @error "Error processing $dataset for year $year" exception=e
            end
        end
    end
    
    @info "Completed PRODCOM data fetching"
end

"""
    process_eurostat_data(data, dataset, year)

Process Eurostat API JSON response into a properly structured DataFrame.
Maintains all dimensions as columns with one value per combination.
"""
function process_eurostat_data(data, dataset, year)
    # Extract dimensions and values
    dimensions = data.dimension
    values = data.value
    
    # Count total values for verification
    value_count = length(values)
    @info "Processing $value_count values from Eurostat API"
    
    # Extract dimension information
    dimension_info = Dict{String, Dict{Int, String}}()
    dimension_names = String[]
    
    for (dim_name, dim_data) in pairs(dimensions)
        dim_str = string(dim_name)
        push!(dimension_names, dim_str)
        
        # Create index mapping for this dimension
        if haskey(dim_data, :category) && haskey(dim_data.category, :index)
            dimension_info[dim_str] = Dict{Int, String}()
            for (cat_label, cat_idx) in pairs(dim_data.category.index)
                # Convert from 0-based to 1-based indexing
                idx = Int(cat_idx) + 1
                dimension_info[dim_str][idx] = string(cat_label)
            end
        end
    end
    
    # Process each value directly to rows (no intermediate grouping)
    rows = []
    processed = 0
    
    for (key, value) in pairs(values)
        # Create a new row for this observation
        row = Dict{Symbol, Any}()
        
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
        
        # Parse the key to get dimension indices
        indices = try
            parse.(Int, split(string(key), ":"))
        catch e
            @warn "Failed to parse key: $key" exception=e
            continue  # Skip this value if key can't be parsed
        end
        
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
    # Identify dimensions that should be used for column headers versus row identifiers
    
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
    # This prevents explosion of columns
    pivot_cols = [dim for (dim, count) in dimension_counts if count <= 20]
    
    if isempty(pivot_cols)
        # If no suitable pivot columns found, try using just unit as fallback
        if "unit" in available_pivot_cols
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
                @warn "Duplicate value found for dimension combination" row_key col_key existing=grouped[row_key][col_name] new=row.value
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
        pivoted_row = Dict{Symbol, Any}()
        
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