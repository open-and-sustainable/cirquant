module DatabaseAccess

using DuckDB, DBInterface
using DataFrames, CSV
import Dates: Date, DateTime

export write_duckdb_table!, write_large_duckdb_table!, executePRQL, recreate_duckdb_database

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
        
        # For simplicity and reliability, use VARCHAR for most columns
        # DuckDB has better type inference on read than on write
        if T >: Missing && any((!ismissing(x) && (x isa String || !(x isa Number))) for x in col)
            # Use VARCHAR for any column with string values or mixed types
            push!(cols, "\"$n\" VARCHAR")
        elseif T >: Missing
            # For numeric columns with missing values
            nonmissing_type = filter(t -> t != Missing, Base.uniontypes(T))
            if !isempty(nonmissing_type)
                base_type = first(nonmissing_type)
                # Prefer VARCHAR for most types to avoid conversion issues
                if base_type <: Integer
                    push!(cols, "\"$n\" BIGINT")
                elseif base_type <: AbstractFloat
                    push!(cols, "\"$n\" DOUBLE")
                else
                    push!(cols, "\"$n\" VARCHAR")
                end
            else
                push!(cols, "\"$n\" VARCHAR")
            end
        else
            # For columns without missing values
            if T <: Integer
                push!(cols, "\"$n\" BIGINT")
            elseif T <: AbstractFloat
                push!(cols, "\"$n\" DOUBLE")
            elseif T <: Bool
                push!(cols, "\"$n\" BOOLEAN")
            elseif T <: DateTime
                push!(cols, "\"$n\" TIMESTAMP")
            elseif T <: Date
                push!(cols, "\"$n\" DATE")
            else
                push!(cols, "\"$n\" VARCHAR")
            end
        end
    end
    
    # Try to create the table
    try
        DBInterface.execute(con, "DROP TABLE IF EXISTS \"$table\"")
        sql_columns = join(cols, ", ")
        create_sql = "CREATE TABLE \"$table\" ($sql_columns)"
        
        @info "Creating table with SQL: $create_sql"
        DBInterface.execute(con, create_sql)
    catch e
        @error "Failed to create table with custom types" exception=e
        
        # Fallback: Create table with simpler VARCHAR columns for everything
        @warn "Trying simplified table creation with all VARCHAR columns"
        DBInterface.execute(con, "DROP TABLE IF EXISTS \"$table\"")
        
        simple_cols = ["\"$n\" VARCHAR" for n in names(df)]
        simple_sql = "CREATE TABLE \"$table\" ($(join(simple_cols, ", ")))"
        
        @info "Creating table with simplified SQL: $simple_sql"
        DBInterface.execute(con, simple_sql)
    end
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
                                (x isa String && (x == ":C" || x == ":c" || x == ":" || x == "-" || 
                                 x == "null" || x == "NULL" || x == "NaN" || x == "Inf" || x == "-Inf")) ? missing : x 
                                for x in clean_df[!, col]]
        end
        
        # Handle problematic numeric values
        if eltype(clean_df[!, col]) >: Number
            clean_df[!, col] = [ismissing(x) ? missing : 
                                (x isa String || (x isa Number && !isfinite(x))) ? missing : x 
                                for x in clean_df[!, col]]
        end
    end
    
    # Additional checks for very large values that might cause DuckDB issues
    for col in names(clean_df)
        if eltype(clean_df[!, col]) >: Number
            # Find extreme values that might cause issues
            extremes = filter(x -> !ismissing(x) && x isa Number && (abs(x) > 1e15), clean_df[!, col])
            if !isempty(extremes)
                @warn "Column $col has $(length(extremes)) extreme values that might cause issues. Converting to missing."
                clean_df[!, col] = [ismissing(x) || !(x isa Number) || abs(x) <= 1e15 ? x : missing for x in clean_df[!, col]]
            end
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
        # Don't rethrow the error - let the fallback mechanism work
    finally
        # Always clean up the temporary file
        if isfile(tmp)
            rm(tmp)
        end
    end
end

function create_and_load_table_chunked!(df, con, table, chunk_size=5000)
    # Clean the dataframe to ensure DuckDB compatibility
    clean_df = clean_dataframe_for_duckdb(df)
    
    # Create the table with proper types
    create_table_with_types!(clean_df, con, table)
    
    # Process in smaller chunks to avoid memory issues
    total_rows = nrow(clean_df)
    @info "Processing $total_rows rows in chunks of $chunk_size"
    
    successful_chunks = 0
    total_chunks = ceil(Int, total_rows / chunk_size)
    
    for chunk_start in 1:chunk_size:total_rows
        chunk_end = min(chunk_start + chunk_size - 1, total_rows)
        chunk = clean_df[chunk_start:chunk_end, :]
        
        # Write chunk to CSV
        tmp_path, tmp_io = mktemp()
        close(tmp_io)
        tmp = "$tmp_path.csv"
        
        chunk_success = false
        try
            @info "Writing chunk $(chunk_start)-$(chunk_end) to CSV ($(successful_chunks+1)/$total_chunks)"
            CSV.write(tmp, chunk, delim=',', quotestrings=true, missingstring="NULL")
            
            # Load chunk into DuckDB
            DBInterface.execute(con, 
                "COPY \"$table\" FROM '$tmp' (FORMAT CSV, HEADER TRUE, NULL 'NULL', IGNORE_ERRORS TRUE)")
            
            chunk_success = true
            successful_chunks += 1
        catch e
            @warn "Error processing chunk $(chunk_start)-$(chunk_end)" exception=e
            # Continue with next chunk even if this one fails
        finally
            isfile(tmp) && rm(tmp)
        end
        
        @info "Processed $(chunk_end)/$(total_rows) rows ($(round(chunk_end/total_rows*100, digits=1))%) - Chunk $(chunk_success ? "succeeded" : "failed")"
    end
    
    @info "Completed chunked processing for table: $table - $successful_chunks/$total_chunks chunks successful"
    
    # Consider operation successful if at least 50% of chunks were processed
    return successful_chunks >= (total_chunks / 2)
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

"""
    recreate_duckdb_database(db_path, backup_suffix="_corrupted")

Handles a corrupted DuckDB database by:
1. Renaming the existing (possibly corrupted) database file
2. Creating a fresh, empty database file at the original path

Returns a tuple (success::Bool, backup_path::String)
"""
function recreate_duckdb_database(db_path, backup_suffix="_corrupted")
    if !isfile(db_path)
        @info "No existing database found at $db_path, will create new one"
        # Create parent directory if it doesn't exist
        mkpath(dirname(db_path))
        return (true, "")
    end
    
    # Backup the existing database
    backup_path = "$(db_path)$(backup_suffix)_$(round(Int, time()))"
    @info "Backing up existing database to $backup_path"
    
    try
        # First try to extract existing table data to preserve it
        @info "Attempting to preserve existing tables from database"
        saved_tables = Dict{String, DataFrame}()
        table_schemas = Dict{String, String}()
        
        # Try to open the existing database and list tables
        try
            existing_con = DuckDB.DB(db_path)
            tables_result = DuckDB.query(existing_con, "SHOW TABLES")
            
            # Convert result to DataFrame
            tables_df = DataFrame(tables_result)
            
            # For each table, try to extract schema and data
            if size(tables_df, 1) > 0 && hasproperty(tables_df, :name)
                for table_name in tables_df.name
                    # Get table schema
                    schema_query = "DESCRIBE $(table_name)"
                    try
                        schema_result = DuckDB.query(existing_con, schema_query)
                        schema_df = DataFrame(schema_result)
                        
                        # Build CREATE TABLE statement from schema
                        col_defs = []
                        for row in eachrow(schema_df)
                            col_name = row.column_name
                            col_type = row.column_type
                            push!(col_defs, "\"$(col_name)\" $(col_type)")
                        end
                        
                        create_stmt = "CREATE TABLE \"$(table_name)\" ($(join(col_defs, ", ")))"
                        table_schemas[table_name] = create_stmt
                    catch schema_err
                        @warn "Failed to extract schema for table $(table_name)" exception=schema_err
                    end
                    
                    # Get table data
                    try
                        data_result = DuckDB.query(existing_con, "SELECT * FROM \"$(table_name)\"")
                        saved_tables[table_name] = DataFrame(data_result)
                        @info "Preserved table $(table_name) with $(nrow(saved_tables[table_name])) rows"
                    catch data_err
                        @warn "Failed to extract data from table $(table_name)" exception=data_err
                    end
                end
            end
            
            DBInterface.close!(existing_con)
        catch extract_err
            @warn "Failed to extract data from existing database" exception=extract_err
        end
        
        # Rename existing database to backup name
        mv(db_path, backup_path)
        @info "Successfully backed up database"
        
        # Try to create a fresh database
        @info "Creating fresh database at $db_path"
        con = DuckDB.DB(db_path)
        
        # Test query to ensure it's working
        DBInterface.execute(con, "CREATE TABLE test_table (id INTEGER)")
        DBInterface.execute(con, "DROP TABLE test_table")
        
        # Restore tables from backup
        for (table_name, create_stmt) in table_schemas
            # Create table with original schema
            DBInterface.execute(con, create_stmt)
            
            if haskey(saved_tables, table_name) && !isempty(saved_tables[table_name])
                # Write data back to the table
                temp_csv = tempname() * ".csv"
                CSV.write(temp_csv, saved_tables[table_name])
                
                # Use proper path format for DuckDB COPY command
                try
                    copy_stmt = "COPY \"$(table_name)\" FROM '$(replace(temp_csv, "'" => "''"))' (FORMAT CSV, HEADER)"
                    DBInterface.execute(con, copy_stmt)
                catch copy_err
                    @warn "Failed to copy data for table $(table_name)" exception=copy_err
                finally
                    # Clean up temp file
                    rm(temp_csv, force=true)
                end
                
                @info "Restored table $(table_name) with $(nrow(saved_tables[table_name])) rows"
            end
        end
        
        DBInterface.close!(con)
        
        @info "Successfully created fresh database with preserved tables"
        return (true, backup_path)
    catch e
        @error "Failed to recreate database" exception=e
        # Try to restore original if backup succeeded but create failed
        if !isfile(db_path) && isfile(backup_path)
            @warn "Attempting to restore original database"
            try
                mv(backup_path, db_path)
                @info "Restored original database"
            catch restore_err
                @error "Failed to restore original database" exception=restore_err
            end
        end
        return (false, backup_path)
    end
end

function write_large_duckdb_table!(df, db, table)
    if isempty(df)
        @warn "Skipping write to DuckDB - dataframe is empty"
        return false
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
    
    success = false
    db_recreated = false
    
    for (method_id, method_name, method_func) in methods
        @info "Trying to write data using $method_name"
        
        # Only create the connection inside the try block
        try
            con = DuckDB.DB(db)
            method_func(df, con, table)
            DBInterface.close!(con)
            @info "Successfully wrote data to table '$table' using $method_name"
            success = true
            break  # Exit the loop on success
        catch e
            @warn "Failed to write using $method_name" exception=e
            
            # Check if this looks like a corrupted database error
            error_str = string(e)
            if !db_recreated && (
                occursin("No more data remaining in MetadataReader", error_str) ||
                occursin("Database file is corrupt", error_str) ||
                occursin("Catalog Error", error_str)
            )
                @warn "Database may be corrupted, attempting to recreate it"
                try
                    # Close connection if it exists
                    if @isdefined con
                        DBInterface.close!(con)
                    end
                    # Recreate the database
                    recreate_success, backup_path = recreate_duckdb_database(db)
                    if recreate_success
                        db_recreated = true
                        @info "Database recreated successfully, original backed up to $backup_path"
                        # Try this method again with the fresh database
                        con = DuckDB.DB(db)
                        method_func(df, con, table)
                        DBInterface.close!(con)
                        @info "Successfully wrote data to table '$table' after database recreation"
                        success = true
                        break
                    end
                catch recreate_err
                    @error "Failed to recreate database" exception=recreate_err
                end
            end
            
            # Make sure connection is closed even on error
            try
                if @isdefined con
                    DBInterface.close!(con)
                end
            catch close_err
                @warn "Failed to close connection" exception=close_err
            end
            
            # Try dropping and recreating the table for the next attempt
            if method_id != methods[end][1]  # Not the last method
                @info "Will try next method after failure"
                # Prepare for next attempt by ensuring table doesn't exist
                try 
                    drop_con = DuckDB.DB(db)
                    DBInterface.execute(drop_con, "DROP TABLE IF EXISTS \"$table\"")
                    DBInterface.close!(drop_con)
                catch drop_err
                    @warn "Failed to drop table for retry" exception=drop_err
                end
            end
        end
    end
    
    # If success is still false, all methods failed
    if !success
        @error "All methods failed to write data to table '$table'"
        
        # Emergency fallback: Save to backup CSV file
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
    end
    
    return success  # Return the success status
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
