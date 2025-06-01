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

Process Eurostat API JSON response into a DataFrame with properly organized rows.
Each row contains a complete set of dimension values and their associated data.
"""
function process_eurostat_data(data, dataset, year)
    # Extract dimensions and values
    dimensions = data.dimension
    values = data.value
    
    # Identify all dimension names and their possible values
    dim_names = []
    dim_maps = Dict()
    
    for (dim_name, dim_data) in pairs(dimensions)
        if haskey(dim_data, :category) && haskey(dim_data.category, :index)
            push!(dim_names, dim_name)
            
            # Create a map from index to label for this dimension
            index_to_label = Dict()
            for (cat_label, cat_idx) in pairs(dim_data.category.index)
                index_to_label[cat_idx + 1] = string(cat_label)  # +1 to convert from 0-based to 1-based
            end
            dim_maps[dim_name] = index_to_label
        end
    end
    
    # Parse all keys to understand the dimension values
    value_map = Dict{String, Dict{String, Any}}()  # Will hold organized data with meaningful dimensions
    dimension_values = Dict{String, Set{String}}()  # Track all possible values for each dimension
    
    # Track how many items we've processed
    count = 0
    total = length(values)
    
    # Process each value and organize dimensions
    for (key, value) in pairs(values)
        key_str = string(key)
        indices = parse.(Int, split(key_str, ":"))
        
        # Create a record with dimension names mapped to their values
        record = Dict{String, Any}()
        
        # Map indices to dimension values
        for (i, dim_name) in enumerate(dim_names)
            if i <= length(indices)
                idx = indices[i]
                if haskey(dim_maps[dim_name], idx)
                    dim_value = dim_maps[dim_name][idx]
                    record[string(dim_name)] = dim_value
                    
                    # Track all values for this dimension
                    if !haskey(dimension_values, string(dim_name))
                        dimension_values[string(dim_name)] = Set{String}()
                    end
                    push!(dimension_values[string(dim_name)], dim_value)
                else
                    record[string(dim_name)] = "unknown"
                end
            else
                record[string(dim_name)] = "unknown"
            end
        end
        
        # Store the record with its value
        record["value"] = value
        
        # We create a signature that uniquely identifies this data point
        value_map[key_str] = record
        
        # Show progress periodically
        count += 1
        if count % 10000 == 0
            @info "Processed $count/$total items ($(round(count/total*100, digits=1))%)"
        end
    end
    
    # Determine which dimensions to use for row grouping and which for columns
    # This is a heuristic and may need to be adjusted for different datasets
    
    # Typically, geographical and time dimensions are used for rows
    # Product codes, indicators, and units are used for columns
    row_dimensions = ["geo"]  # Dimensions that should define rows
    col_dimensions = [string(dim) for dim in dim_names if string(dim) != "geo"]  # All other dimensions become columns
    
    # Group data by row dimensions
    grouped_data = Dict()
    
    for (key_str, record) in value_map
        # Create a row key based on row dimensions
        row_key_parts = [get(record, dim, "unknown") for dim in row_dimensions]
        row_key = Tuple(row_key_parts)
        
        # Initialize this row group if it doesn't exist
        if !haskey(grouped_data, row_key)
            grouped_data[row_key] = Dict()
        end
        
        # Create a column key based on column dimensions
        col_key_parts = [get(record, dim, "unknown") for dim in col_dimensions]
        col_key = Tuple(col_key_parts)
        
        # Store the value using the column key
        grouped_data[row_key][col_key] = record["value"]
    end
    
    # Create rows from the grouped data
    rows = []
    
    for (row_key, col_values) in grouped_data
        # Create a new row
        row = Dict{Symbol, Any}()
        
        # Add metadata
        row[:dataset] = dataset
        row[:year] = year
        row[:fetch_date] = now()
        row[:data_source] = "Eurostat PRODCOM API"
        
        # Add row dimension values
        for (i, dim) in enumerate(row_dimensions)
            row[Symbol(dim)] = row_key[i]
        end
        
        # Add column values
        for (col_key, value) in col_values
            # Create a descriptive column name from the column dimensions
            col_parts = ["$(dim)_$(col_key[i])" for (i, dim) in enumerate(col_dimensions)]
            col_name = join(col_parts, "_")
            row[Symbol("value_$(col_name)")] = value
        end
        
        push!(rows, row)
    end
    
    # Return empty DataFrame if no rows
    if isempty(rows)
        return DataFrame()
    end
    
    # Convert to DataFrame
    df = DataFrame(rows)
    
    return df
end

end # module