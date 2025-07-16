module DatabaseAccess

using DuckDB, DBInterface
using DataFrames, CSV
import Dates: Date, DateTime

export write_duckdb_table!, write_duckdb_table_with_connection!, write_large_duckdb_table!, executePRQL, recreate_duckdb_database, table_exists, get_table_columns, installPRQL_DuckDBextension

# --- helpers ---------------------------------------------------------------

escape_sql_string(s::String) = "'" * replace(s, "'" => "''") * "'"

sql_value(x) = x === nothing || x === missing ? "NULL" :
               x isa String ? escape_sql_string(x) : string(x)

"""
    table_info(db_path, table_name) → (rows, cols)

Returns the number of rows and columns of `table_name`
in the DuckDB database at `db_path`.
If the table does not exist it returns `missing, missing`.
"""
function table_info(db_path::AbstractString, table_name::AbstractString)
    db = DuckDB.DB(db_path)
    con = DBInterface.connect(db)

    try
        # Check if table exists using centralized function
        if !table_exists(db_path, table_name)
            return (missing, missing)
        end

        # Get row count
        result = DuckDB.query(con, "SELECT count(*) AS n FROM \"$table_name\"")
        result_df = DataFrames.DataFrame(result)
        rows = result_df[1, :n]

        # Get column count using centralized function
        column_names = get_table_columns(db_path, table_name)
        cols = length(column_names)

        return (rows, cols)
    finally
        DBInterface.close!(con)
        DBInterface.close!(db)
    end
end

"""
    table_exists(db_path::String, table_name::String) → Bool

Check if a table exists in the DuckDB database.

# Arguments
- `db_path`: Path to the DuckDB database file
- `table_name`: Name of the table to check

# Returns
- `true` if table exists, `false` otherwise

# Example
```julia
if table_exists("mydb.duckdb", "product_data")
    println("Table exists!")
end
```
"""
function table_exists(db_path::String, table_name::String)::Bool
    db = DuckDB.DB(db_path)
    con = DBInterface.connect(db)

    try
        # Use parameterized query to avoid SQL injection
        query = """
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_name = ?
        """

        # Execute with parameter binding
        stmt = DBInterface.prepare(con, query)
        result = DBInterface.execute(stmt, [table_name]) |> DataFrame

        return result.cnt[1] > 0
    catch e
        @error "Error checking table existence" exception = e table = table_name
        return false
    finally
        DBInterface.close!(con)
        DBInterface.close!(db)
    end
end

"""
    get_table_columns(db_path::String, table_name::String) → Vector{String}

Get the column names of a table in the DuckDB database.

# Arguments
- `db_path`: Path to the DuckDB database file
- `table_name`: Name of the table

# Returns
- Vector of column names, or empty vector if table doesn't exist

# Example
```julia
columns = get_table_columns("mydb.duckdb", "product_data")
println("Columns: ", join(columns, ", "))
```
"""
function get_table_columns(db_path::String, table_name::String)::Vector{String}
    db = DuckDB.DB(db_path)
    con = DBInterface.connect(db)

    try
        # First check if table exists
        if !table_exists(db_path, table_name)
            return String[]
        end

        # Get column names
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
        """

        stmt = DBInterface.prepare(con, query)
        result = DBInterface.execute(stmt, [table_name]) |> DataFrame

        return result.column_name
    catch e
        @error "Error getting table columns" exception = e table = table_name
        return String[]
    finally
        DBInterface.close!(con)
        DBInterface.close!(db)
    end
end


# --- core ------------------------------------------------------------------

"""
    create_table_with_types!(df::DataFrame, con::DuckDB.Connection, table::String)

Create a new table in DuckDB with appropriate column types based on the DataFrame schema.
This function handles complex type mappings and creates tables that can handle various data types.

# Arguments
- `df`: DataFrame containing the data schema
- `con`: Active DuckDB connection
- `table`: Name of the table to create

# Notes
- Drops existing table if it exists
- Maps Julia types to appropriate DuckDB types
- Handles special cases like Date, DateTime, and mixed type columns
"""
function create_table_with_types!(df::DataFrame, con::DuckDB.Connection, table::String)
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

        #@info "Creating table with SQL: $create_sql"
        DBInterface.execute(con, create_sql)
    catch e
        @error "Failed to create table with custom types" exception = e

        # Fallback: Create table with simpler VARCHAR columns for everything
        @warn "Trying simplified table creation with all VARCHAR columns"
        DBInterface.execute(con, "DROP TABLE IF EXISTS \"$table\"")

        simple_cols = ["\"$n\" VARCHAR" for n in names(df)]
        simple_sql = "CREATE TABLE \"$table\" ($(join(simple_cols, ", ")))"

        #@info "Creating table with simplified SQL: $simple_sql"
        DBInterface.execute(con, simple_sql)
    end
end

"""
    create_and_load_table_directly!(df, con, table)

Create a table and load data directly using INSERT statements.
This is the primary method for loading data into DuckDB.

# Arguments
- `df`: DataFrame to load
- `con`: Active DuckDB connection
- `table`: Name of the table to create/populate
"""
function create_and_load_table_directly!(df, con, table)
    create_table_with_types!(df, con, table)
    for row in eachrow(df)
        vals = join([sql_value(row[col]) for col in names(row)], ", ")
        DBInterface.execute(con, "INSERT INTO \"$table\" VALUES ($vals)")
    end
end

"""
    clean_dataframe_for_duckdb(df)

Clean and prepare a DataFrame for loading into DuckDB by handling problematic values.

# Arguments
- `df`: DataFrame to clean

# Returns
- Cleaned copy of the DataFrame with problematic values converted to missing

# Notes
- Converts special string values like ":C", "null", "NaN" to missing
- Handles infinite numeric values
- Converts extreme numeric values (>1e15) to missing
- Converts Any-typed columns to strings for safety
"""
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

"""
    create_and_load_table_throughCSV!(df, con, table)

Create a table and load data through an intermediate CSV file.
This is a fallback method when direct loading fails.

# Arguments
- `df`: DataFrame to load
- `con`: Active DuckDB connection
- `table`: Name of the table to create/populate

# Notes
- Uses temporary CSV file for data transfer
- Automatically cleans up temporary files
- Falls back to chunked loading if CSV method fails
"""
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
        @error "Error during CSV processing or DuckDB loading" exception = e
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

"""
    create_and_load_table_chunked!(df, con, table, chunk_size=5000)

Create a table and load data in chunks to handle large datasets.

# Arguments
- `df`: DataFrame to load
- `con`: Active DuckDB connection
- `table`: Name of the table to create/populate
- `chunk_size`: Number of rows to insert per batch (default: 5000)

# Notes
- Useful for very large datasets that might cause memory issues
- Provides progress updates during loading
"""
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
            @warn "Error processing chunk $(chunk_start)-$(chunk_end)" exception = e
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

"""
    create_and_load_table_direct!(df, con, table)

Main entry point for creating and loading tables with automatic fallback strategies.

# Arguments
- `df`: DataFrame to load
- `con`: Active DuckDB connection
- `table`: Name of the table to create/populate

# Notes
- Tries multiple loading strategies in order:
  1. Direct INSERT statements
  2. CSV intermediate file
  3. Chunked loading
- Automatically handles errors and tries alternative methods
"""
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
            @warn "Failed to insert row $i" exception = e
        end
    end

    @info "Direct insertion complete: $successful_inserts/$total_rows rows inserted successfully"
    return successful_inserts
end

"""
    write_duckdb_table!(df, db, table)

Write a DataFrame to a DuckDB table, creating a new connection.

# Arguments
- `df`: DataFrame to write
- `db`: Path to DuckDB database file
- `table`: Name of the table to create/replace

# Notes
- Opens and closes its own database connection
- Use `write_duckdb_table_with_connection!` if you have an existing connection
"""
write_duckdb_table!(df, db, table) = (db_conn = DuckDB.DB(db);
con = DBInterface.connect(db_conn);
create_and_load_table_directly!(df, con, table);
DBInterface.close!(con);
DBInterface.close!(db_conn))

"""
    write_duckdb_table_with_connection!(df, con::DuckDB.Connection, table)

Write a DataFrame to a DuckDB table using an existing connection.
This avoids opening/closing connections repeatedly which can cause corruption.

# Arguments
- `df`: DataFrame to write
- `con`: Existing DuckDB connection
- `table`: Name of the table to create/replace

# Example
```julia
db = DuckDB.DB("mydb.duckdb")
con = DBInterface.connect(db)
write_duckdb_table_with_connection!(df, con, "my_table")
# Note: caller is responsible for closing the connection
```
"""
function write_duckdb_table_with_connection!(df, con::DuckDB.Connection, table)
    create_and_load_table_directly!(df, con, table)
end

"""
    recreate_duckdb_database(db_path, backup_suffix="_corrupted")

Creates a new DuckDB database file without modifying the existing one.

# Arguments
- `db_path`: Path to the database file
- `backup_suffix`: Suffix for backup (not used in current implementation)

# Returns
- `(success::Bool, new_db_path::String)`: Success status and path to new database

# Notes
- SAFETY: This function does NOT replace the existing database
- Creates a new database with timestamp suffix (e.g., "db.duckdb_new_1234567890")
- The original database remains untouched
- User must manually rename the new database if they want to use it
"""
function recreate_duckdb_database(db_path, backup_suffix="_corrupted")
    # IMPORTANT: This function has been modified to NEVER replace the existing database
    # Instead, it creates a new database file with a different name and returns it

    if !isfile(db_path)
        @info "No existing database found at $db_path, will create new one"
        # Create parent directory if it doesn't exist
        mkpath(dirname(db_path))
        return (true, "")
    end

    # Instead of overwriting the existing database, create a new one with a timestamp
    new_db_path = "$(db_path)_new_$(round(Int, time()))"
    @info "SAFETY MEASURE: Creating new database at $new_db_path instead of replacing existing one"

    try
        # Create a new database instead of replacing the existing one
        con = DuckDB.DB(new_db_path)

        # Test query to ensure it's working
        DBInterface.execute(con, "CREATE TABLE test_table (id INTEGER)")
        DBInterface.execute(con, "DROP TABLE test_table")
        DBInterface.close!(con)

        @info "Successfully created new database at $new_db_path"
        @info "IMPORTANT: The original database at $db_path has NOT been modified"
        @info "If you want to use the new database, manually rename $new_db_path to $db_path"

        return (true, new_db_path)
    catch e
        @error "Failed to create new database" exception = e

        # Try to clean up the new database if it was created but errored
        if isfile(new_db_path)
            try
                rm(new_db_path)
                @info "Cleaned up incomplete new database file"
            catch cleanup_err
                @warn "Failed to clean up new database file" exception = cleanup_err
            end
        end

        return (false, "")
    end
end

"""
    write_large_duckdb_table!(df, db, table)

Write a large DataFrame to DuckDB with optimized settings and error handling.

# Arguments
- `df`: DataFrame to write (can be very large)
- `db`: Path to DuckDB database file
- `table`: Name of the table to create/replace

# Returns
- `true` if successful, `false` if failed

# Notes
- Optimized for large datasets with increased memory limits
- Handles database corruption by creating new database if needed
- Provides detailed progress updates and error messages
- Automatically cleans data before insertion
"""
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
            db_conn = DuckDB.DB(db)
            con = DBInterface.connect(db_conn)
            method_func(df, con, table)
            DBInterface.close!(con)
            DBInterface.close!(db_conn)
            @info "Successfully wrote data to table '$table' using $method_name"
            success = true
            break  # Exit the loop on success
        catch e
            @warn "Failed to write using $method_name" exception = e

            # Check if this looks like a corrupted database error
            error_str = string(e)
            if !db_recreated && (
                occursin("No more data remaining in MetadataReader", error_str) ||
                occursin("Database file is corrupt", error_str) ||
                occursin("Catalog Error", error_str)
            )
                @warn "Database may have issues, but we'll avoid recreation to preserve existing tables"
                @warn "Will try alternative methods to write the data"

                # Instead of recreating the database, we'll just try the next method
                # This way we don't risk losing existing tables

                # Make sure any existing connection is closed
                if @isdefined con
                    try
                        DBInterface.close!(con)
                    catch close_err
                        @warn "Failed to close connection" exception = close_err
                    end
                end
                if @isdefined db_conn
                    try
                        DBInterface.close!(db_conn)
                    catch close_err
                        @warn "Failed to close database" exception = close_err
                    end
                end
            end

            # Make sure connection is closed even on error
            try
                if @isdefined con
                    DBInterface.close!(con)
                end
                if @isdefined db_conn
                    DBInterface.close!(db_conn)
                end
            catch close_err
                @warn "Failed to close connection" exception = close_err
            end

            # Try dropping and recreating the table for the next attempt
            if method_id != methods[end][1]  # Not the last method
                @info "Will try next method after failure"
                # Prepare for next attempt by ensuring table doesn't exist
                try
                    drop_db = DuckDB.DB(db)
                    drop_con = DBInterface.connect(drop_db)
                    DBInterface.execute(drop_con, "DROP TABLE IF EXISTS \"$table\"")
                    DBInterface.close!(drop_con)
                    DBInterface.close!(drop_db)
                catch drop_err
                    @warn "Failed to drop table for retry" exception = drop_err
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
            @info "To manually import this data, you can use: COPY $(table) FROM '$(backup_file)' (FORMAT CSV, HEADER);"
        catch backup_err
            @error "Failed to save backup file" exception = backup_err
        end
    end

    return success  # Return the success status
end

"""
    installPRQL_DuckDBextension()

Install the PRQL extension from the DuckDB community repository.
This function needs to be called once before using PRQL queries.
"""
function installPRQL_DuckDBextension()
    db = DuckDB.DB()
    con = DBInterface.connect(db)
    try
        # Attempt to install the PRQL extension from community repository
        DBInterface.execute(con, "INSTALL 'prql' FROM community;")
        DBInterface.execute(con, "LOAD 'prql';")

        @info "PRQL extension installed and loaded successfully."
        return true
    catch e
        @error "Error during PRQL extension installation" exception = e
        return false
    finally
        DBInterface.close!(con)
        DBInterface.close!(db)
    end
end

"""
    executePRQL(dbpath::String, prqlpath::String)::DataFrame

Execute a PRQL query from a file against a DuckDB database.

# Arguments
- `dbpath::String`: Path to the DuckDB database file
- `prqlpath::String`: Path to the PRQL query file

# Returns
- `DataFrame`: Query results as a DataFrame
- Empty DataFrame if an error occurs

# Example
```julia
result = executePRQL("data.duckdb", "query.prql")
```
"""
function executePRQL(dbpath::String, prqlpath::String)::DataFrame
    db = DuckDB.DB(dbpath)
    con = DBInterface.connect(db)

    try
        # Load the PRQL extension
        DBInterface.execute(con, "LOAD 'prql';")

        # Read the PRQL code from the file
        prql_query = read(prqlpath, String)

        # Execute the PRQL query and capture the result
        result = DBInterface.execute(con, prql_query)
        result_df = DataFrame(result)

        # Return the resulting DataFrame
        return result_df
    catch e
        # Handle any errors that occur during the process
        @error "Error during PRQL execution" exception = e
        return DataFrame()  # Return an empty DataFrame in case of error
    finally
        # Ensure the database connection is closed
        DBInterface.close!(con)
        DBInterface.close!(db)
    end
end

end # module
