module DatabaseAccess

using DuckDB, DBInterface
using DataFrames, CSV
import Dates: Date, DateTime

export write_duckdb_table!, write_large_duckdb_table!, executePRQL

# --- helpers ---------------------------------------------------------------

escape_sql_string(s::String) = replace("'" * s * "'", "'" => "''")

sql_value(x) = x === nothing || x === missing ? "NULL" :
               x isa String ? escape_sql_string(x) : string(x)

"""
    table_info(db_path, table_name) → (rows, cols)

Returns the number of rows and columns of `table_name`
in the DuckDB database at `db_path`.
If the table does not exist it returns `missing, missing`.
"""
function table_info(db_path::AbstractString, table_name::AbstractString)
    con = DuckDB.DB(db_path)
    
    # Check if table exists
    result = DuckDB.query(con, "SELECT count(*) AS n FROM information_schema.tables WHERE table_name = '$table_name'")
    result_df = DataFrames.DataFrame(result)
    exists = result_df[1, :n] > 0
    
    if !exists
        DBInterface.close!(con)
        return (missing, missing)
    end
    
    # Get row count
    result = DuckDB.query(con, "SELECT count(*) AS n FROM \"$table_name\"")
    result_df = DataFrames.DataFrame(result)
    rows = result_df[1, :n]
    
    # Get column count
    result = DuckDB.query(con, "SELECT count(*) AS n FROM information_schema.columns WHERE table_name = '$table_name'")
    result_df = DataFrames.DataFrame(result)
    cols = result_df[1, :n]
    
    DBInterface.close!(con)
    return (rows, cols)
end


# --- core ------------------------------------------------------------------

function create_table_with_types!(df::DataFrame, con::DuckDB.DB, table::String)
    type_map = Dict(
        Int32 => "INTEGER", Int64 => "BIGINT",
        Float32 => "FLOAT", Float64 => "DOUBLE",
        String => "VARCHAR",
        Bool => "BOOLEAN",
        Date => "DATE",
        DateTime => "TIMESTAMP",
        Symbol => "VARCHAR",
        Missing => "VARCHAR",
        Any => "VARCHAR"
    )
    
    # Determine types for each column, handling mixed types safely
    cols = []
    for (n, col) in zip(names(df), eachcol(df))
        T = eltype(col)
        
        # Handle Union types (like Union{Missing, Int64})
        if T >: Missing
            # Extract the non-missing type
            nonmissing_type = filter(t -> t != Missing, Base.uniontypes(T))
            if !isempty(nonmissing_type)
                base_type = first(nonmissing_type)
                duck_type = get(type_map, base_type, "VARCHAR")
                push!(cols, "\"$n\" $duck_type")
            else
                push!(cols, "\"$n\" VARCHAR")
            end
        else
            duck_type = get(type_map, T, "VARCHAR")
            push!(cols, "\"$n\" $duck_type")
        end
    end
    
    DBInterface.execute(con, "DROP TABLE IF EXISTS $table")
    sql_columns = join(cols, ", ")
    create_sql = "CREATE TABLE \"$table\" ($sql_columns)"
    
    @info "Creating table with SQL: $create_sql"
    DBInterface.execute(con, create_sql)
end

function create_and_load_table_directly!(df, con, table)
    create_table_with_types!(df, con, table)
    for row in eachrow(df)
        vals = join(sql_value.(row), ", ")
        DBInterface.execute(con, "INSERT INTO $table VALUES ($vals)")
    end
end

function clean_dataframe_for_duckdb(df)
    # Make a copy to avoid modifying the original
    clean_df = deepcopy(df)
    
    # Replace problematic values in all columns
    for col in names(clean_df)
        # Convert Any columns to strings for safety
        if eltype(clean_df[!, col]) == Any
            clean_df[!, col] = [ismissing(x) ? missing : string(x) for x in clean_df[!, col]]
        end
        
        # Handle special string values
        if eltype(clean_df[!, col]) >: String
            clean_df[!, col] = [ismissing(x) ? missing : 
                                (x == ":C" || x == ":c" || x == ":" || x == "-" || 
                                 x == ":" || x == "null" || x == "NULL") ? missing : x 
                                for x in clean_df[!, col]]
        end
        
        # Handle problematic numeric values
        if eltype(clean_df[!, col]) >: Number
            clean_df[!, col] = [ismissing(x) ? missing : 
                                (x isa String || !isfinite(x)) ? missing : x 
                                for x in clean_df[!, col]]
        end
    end
    
    return clean_df
end

function create_and_load_table_throughCSV!(df, con, table)
    # Clean the dataframe to ensure DuckDB compatibility
    clean_df = clean_dataframe_for_duckdb(df)
    
    # Create the table with proper types
    create_table_with_types!(clean_df, con, table)
    
    # Write to CSV with careful handling of special characters
    tmp_path, tmp_io = mktemp()
    close(tmp_io)
    tmp = "$tmp_path.csv"
    
    try
        @info "Writing $(nrow(clean_df)) rows to temporary CSV: $tmp"
        CSV.write(tmp, clean_df, delim=',', quotestrings=true, missingstring="NULL")
        
        # Load CSV into DuckDB with explicit error handling
        @info "Copying data from CSV to DuckDB table: $table"
        DBInterface.execute(con, 
            "COPY \"$table\" FROM '$tmp' (FORMAT CSV, HEADER TRUE, NULL 'NULL', IGNORE_ERRORS TRUE)")
        @info "Successfully loaded data into table: $table"
    catch e
        @error "Error during CSV processing or DuckDB loading" exception=e
        # Try to provide more context about the error
        if isfile(tmp)
            csv_size = filesize(tmp)
            @info "CSV file exists with size: $csv_size bytes"
            if csv_size > 0
                # Sample the first few lines to help diagnose
                @info "CSV sample: $(read(tmp, String)[1:min(500, csv_size)])"
            end
        end
        rethrow(e)
    finally
        # Always clean up the temporary file
        if isfile(tmp)
            rm(tmp)
        end
    end
end

function create_and_load_table_chunked!(df, con, table, chunk_size=10000)
    # Clean the dataframe to ensure DuckDB compatibility
    clean_df = clean_dataframe_for_duckdb(df)
    
    # Create the table with proper types
    create_table_with_types!(clean_df, con, table)
    
    # Process in smaller chunks to avoid memory issues
    total_rows = nrow(clean_df)
    @info "Processing $total_rows rows in chunks of $chunk_size"
    
    for chunk_start in 1:chunk_size:total_rows
        chunk_end = min(chunk_start + chunk_size - 1, total_rows)
        chunk = clean_df[chunk_start:chunk_end, :]
        
        # Write chunk to CSV
        tmp_path, tmp_io = mktemp()
        close(tmp_io)
        tmp = "$tmp_path.csv"
        
        try
            @info "Writing chunk $(chunk_start)-$(chunk_end) to CSV"
            CSV.write(tmp, chunk, delim=',', quotestrings=true, missingstring="NULL")
            
            # Load chunk into DuckDB
            DBInterface.execute(con, 
                "COPY \"$table\" FROM '$tmp' (FORMAT CSV, HEADER TRUE, NULL 'NULL', IGNORE_ERRORS TRUE)")
        catch e
            @warn "Error processing chunk $(chunk_start)-$(chunk_end)" exception=e
            # Continue with next chunk even if this one fails
        finally
            isfile(tmp) && rm(tmp)
        end
        
        @info "Processed $(chunk_end)/$(total_rows) rows ($(round(chunk_end/total_rows*100, digits=1))%)"
    end
    
    @info "Completed chunked processing for table: $table"
end

function create_and_load_table_direct!(df, con, table)
    # Clean and create table as before
    clean_df = clean_dataframe_for_duckdb(df)
    create_table_with_types!(clean_df, con, table)
    
    # Insert data row by row with error handling
    total_rows = nrow(clean_df)
    successful_inserts = 0
    
    @info "Inserting $total_rows rows directly"
    
    for (i, row) in enumerate(eachrow(clean_df))
        try
            # Build INSERT statement
            cols = join(["\"$c\"" for c in names(clean_df)], ", ")
            vals = []
            
            for val in row
                if ismissing(val)
                    push!(vals, "NULL")
                elseif val isa String
                    # Escape string values
                    push!(vals, "'$(replace(val, "'" => "''"))'")
                elseif val isa Number
                    push!(vals, string(val))
                elseif val isa Bool
                    push!(vals, val ? "TRUE" : "FALSE")
                elseif val isa DateTime || val isa Date
                    push!(vals, "'$(val)'")
                else
                    # Convert anything else to string
                    push!(vals, "'$(replace(string(val), "'" => "''"))'")
                end
            end
            
            values_str = join(vals, ", ")
            sql = "INSERT INTO \"$table\" ($cols) VALUES ($values_str)"
            
            DBInterface.execute(con, sql)
            successful_inserts += 1
            
            # Show progress periodically
            if i % 1000 == 0
                @info "Inserted $i/$total_rows rows ($(round(i/total_rows*100, digits=1))%)"
            end
        catch e
            # Log error but continue with next row
            @warn "Failed to insert row $i" exception=e
        end
    end
    
    @info "Direct insertion complete: $successful_inserts/$total_rows rows inserted successfully"
    return successful_inserts
end

write_duckdb_table!(df, db, table) = (con = DuckDB.DB(db);
create_and_load_table_directly!(df, con, table);
DBInterface.close!(con))

function write_large_duckdb_table!(df, db, table)
    if isempty(df)
        @warn "Skipping write to DuckDB - dataframe is empty"
        return
    end
    
    row_count = nrow(df)
    col_count = ncol(df)
    @info "Writing dataframe to DuckDB table '$table' ($row_count rows, $col_count columns)"
    
    # Try multiple methods with fallbacks if one fails
    methods = [
        (:csv, "CSV method", (df, con, table) -> create_and_load_table_throughCSV!(df, con, table)),
        (:chunked, "Chunked CSV method", (df, con, table) -> create_and_load_table_chunked!(df, con, table)),
        (:direct, "Direct SQL insertion", (df, con, table) -> create_and_load_table_direct!(df, con, table))
    ]
    
    for (method_id, method_name, method_func) in methods
        @info "Trying to write data using $method_name"
        con = DuckDB.DB(db)
        
        try
            method_func(df, con, table)
            @info "Successfully wrote data to table '$table' using $method_name"
            DBInterface.close!(con)
            return true  # Success!
        catch e
            @warn "Failed to write using $method_name" exception=e
            DBInterface.close!(con)
            
            # Try dropping and recreating the table for the next attempt
            if method_id != methods[end][1]  # Not the last method
                @info "Will try next method after failure"
                # Prepare for next attempt by ensuring table doesn't exist
                con = DuckDB.DB(db)
                try 
                    DBInterface.execute(con, "DROP TABLE IF EXISTS \"$table\"")
                catch drop_err
                    @warn "Failed to drop table for retry" exception=drop_err
                finally
                    DBInterface.close!(con)
                end
            end
        end
    end
    
    # If we get here, all methods failed
    @error "All methods failed to write data to table '$table'"
    
    # Emergency fallback: Save to SQLite or CSV file
    backup_dir = abspath(joinpath(dirname(db), "backup"))
    mkpath(backup_dir)
    backup_file = joinpath(backup_dir, "$(table)_$(round(Int, time())).csv")
    
    @warn "Saving data to backup CSV file: $backup_file"
    try
        CSV.write(backup_file, df)
        @info "Data saved to backup file: $backup_file"
    catch backup_err
        @error "Failed to save backup file" exception=backup_err
    end
    
    return false  # Indicate failure
end

function executePRQL(dbpath, prqlpath)
    con = DuckDB.DB(dbpath)
    try
        DuckDB.execute(con, "LOAD 'prql'")
        DataFrame(DuckDB.query(con, read(prqlpath, String)))
    finally
        DBInterface.close!(con)
    end
end

end # module
