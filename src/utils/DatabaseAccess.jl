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

function create_and_load_table_throughCSV!(df, con, table)
    # Clean the dataframe to ensure DuckDB compatibility
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
                                (x == ":C" || x == ":c" || x == ":" || x == "-") ? missing : x 
                                for x in clean_df[!, col]]
        end
    end
    
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
            "COPY \"$table\" FROM '$tmp' (FORMAT CSV, HEADER TRUE, NULL 'NULL', IGNORE_ERRORS FALSE)")
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

write_duckdb_table!(df, db, table) = (con = DuckDB.DB(db);
create_and_load_table_directly!(df, con, table);
DBInterface.close!(con))

function write_large_duckdb_table!(df, db, table)
    if isempty(df)
        @warn "Skipping write to DuckDB - dataframe is empty"
        return
    end
    
    @info "Writing dataframe to DuckDB table '$table' ($(nrow(df)) rows, $(ncol(df)) columns)"
    
    con = DuckDB.DB(db)
    try
        create_and_load_table_throughCSV!(df, con, table)
        @info "Successfully wrote data to table '$table'"
    catch e
        @error "Failed to write dataframe to DuckDB" exception=e table=table rows=nrow(df) cols=ncol(df)
        rethrow(e)
    finally
        # Always close the connection
        DBInterface.close!(con)
    end
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
