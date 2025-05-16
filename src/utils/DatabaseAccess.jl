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
        Float64 => "DOUBLE",
        String => "STRING",
        Bool => "BOOLEAN",
        Date => "DATE",
        DateTime => "TIMESTAMP"
    )
    cols = ["\"$n\" $(get(type_map, T, "STRING"))"
            for (n, T) in zip(names(df), eltype.(eachcol(df)))]
    DBInterface.execute(con, "DROP TABLE IF EXISTS $table")
    sql_columns = join(cols, ", ")
    create_sql = "CREATE TABLE \"$table\" ($sql_columns)"
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
    create_table_with_types!(df, con, table)
    tmp_path, tmp_io = mktemp()
    close(tmp_io)
    tmp = "$tmp_path.csv"
    CSV.write(tmp, df)
    DBInterface.execute(con,
        "COPY $table FROM '$tmp' (FORMAT CSV, HEADER TRUE)")
    rm(tmp)
end

write_duckdb_table!(df, db, table) = (con = DuckDB.DB(db);
create_and_load_table_directly!(df, con, table);
DBInterface.close!(con))

write_large_duckdb_table!(df, db, table) =
    (con = DuckDB.DB(db);
    create_and_load_table_throughCSV!(df, con, table);
    DBInterface.close!(con))

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
