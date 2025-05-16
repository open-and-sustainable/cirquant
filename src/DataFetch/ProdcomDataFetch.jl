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

Process Eurostat API JSON response into a DataFrame.
"""
function process_eurostat_data(data, dataset, year)
    # Extract dimensions and values
    dimensions = data.dimension
    values = data.value
    
    # Create rows
    rows = []
    
    # Track how many items we've processed
    count = 0
    total = length(values)
    
    for (key, value) in pairs(values)
        # Create a new row
        row = Dict{Symbol, Any}()
        
        # Add metadata
        row[:dataset] = dataset
        row[:year] = year
        row[:fetch_date] = now()
        
        # Convert key to string and parse indices
        key_str = string(key)
        indices = parse.(Int, split(key_str, ":"))
        
        # Map indices to dimension values
        dim_i = 1
        for (dim_name, dim_data) in pairs(dimensions)
            if haskey(dim_data, :category) && haskey(dim_data.category, :index)
                if dim_i <= length(indices)
                    idx = indices[dim_i]
                    
                    # Find the category label for this index
                    for (cat_label, cat_idx) in pairs(dim_data.category.index)
                        if cat_idx == (idx - 1)  # API uses 0-based indexing
                            row[Symbol(dim_name)] = string(cat_label)
                            break
                        end
                    end
                end
                dim_i += 1
            end
        end
        
        # Add the value
        row[:value] = value
        
        push!(rows, row)
        
        # Show progress periodically
        count += 1
        if count % 10000 == 0
            @info "Processed $count/$total items ($(round(count/total*100, digits=1))%)"
        end
    end
    
    # Return empty DataFrame if no rows
    if isempty(rows)
        return DataFrame()
    end
    
    # Convert to DataFrame
    df = DataFrame(rows)
    
    # Add source information
    df.data_source .= "Eurostat PRODCOM API"
    
    return df
end

end # module