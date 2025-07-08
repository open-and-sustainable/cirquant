module ProdcomUnitConverter

using DataFrames
using DuckDB, DBInterface

export process_prodcom_to_tonnes

# Include the unit converter
include(joinpath(@__DIR__, "UnitConversion/UnitConverter.jl"))
import .UnitConverter: convert_to_tonnes, is_convertible_to_tonnes

"""
    process_prodcom_to_tonnes(db_path::String, year::Int)

Process PRODCOM data: get numeric quantities from PRODQNT, find their units from QNTUNIT, convert to tonnes.

# Arguments
- `db_path`: Path to the raw DuckDB database
- `year`: Year to process

# Returns
- DataFrame with columns: product_code, geo, production_tonnes
"""
function process_prodcom_to_tonnes(db_path::String, year::Int)
    println("Processing PRODCOM data for year $year...")

    # Connect to database
    db_conn = DuckDB.DB(db_path)
    con = DBInterface.connect(db_conn)

    try
        table_name = "prodcom_ds_056121_$year"

        # Step 1: Get all records with numeric PRODQNT values
        quantities_query = """
        SELECT
            prccode as product_code,
            decl as geo,
            TRY_CAST(value AS DOUBLE) as quantity
        FROM $table_name
        WHERE indicators = 'PRODQNT'
            AND TRY_CAST(value AS DOUBLE) IS NOT NULL
            AND TRY_CAST(value AS DOUBLE) > 0
        """

        quantities_df = DBInterface.execute(con, quantities_query) |> DataFrame
        println("Found $(nrow(quantities_df)) records with numeric quantities")

        if nrow(quantities_df) == 0
            return DataFrame(product_code=String[], geo=String[], production_tonnes=Float64[])
        end

        # Step 2: For each product-geo combination, get the unit
        results = DataFrame()

        for row in eachrow(quantities_df)
            # Query for the unit
            unit_query = """
            SELECT value as unit
            FROM $table_name
            WHERE prccode = '$(row.product_code)'
                AND decl = '$(row.geo)'
                AND indicators = 'QNTUNIT'
                AND value IS NOT NULL
                AND value != ''
            LIMIT 1
            """

            unit_df = DBInterface.execute(con, unit_query) |> DataFrame

            if nrow(unit_df) > 0
                unit = unit_df.unit[1]

                # The unit is already a numeric code
                unit_code = unit

                # Convert if possible
                if is_convertible_to_tonnes(String(unit_code))
                    tonnes_value = convert_to_tonnes(row.quantity, String(unit_code))
                    if !isnan(tonnes_value) && tonnes_value > 0
                        push!(results, (
                            product_code = row.product_code,
                            geo = row.geo,
                            production_tonnes = tonnes_value
                        ))
                    end
                end
            end
        end

        println("Converted $(nrow(results)) records to tonnes")
        return results

    finally
        DBInterface.close!(con)
        DBInterface.close!(db_conn)
    end
end

end # module
