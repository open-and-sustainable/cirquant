module MaterialRecyclingRatesFetch

using DataFrames, Dates
using EurostatAPI
using ..DatabaseAccess: write_large_duckdb_table!

export fetch_material_recycling_rates_data

"""
    fetch_material_recycling_rates_data(years_range="2002-2023"; db_path::String)

Fetch env_wastrt (waste treatment) per year using EurostatAPI and write to DuckDB tables `env_wastrt_YYYY`.
"""
function fetch_material_recycling_rates_data(years_range="2002-2023"; db_path::String)
    years = split(years_range, "-")
    start_year = length(years) == 2 ? parse(Int, years[1]) : parse(Int, years[1])
    end_year = length(years) == 2 ? parse(Int, years[2]) : start_year

    for year in start_year:end_year
        @info "Fetching env_wastrt for $year via EurostatAPI"
        df = try
            EurostatAPI.fetch_dataset("env_wastrt", year)
        catch e
            @warn "EurostatAPI fetch failed for env_wastrt $year" exception=e
            DataFrame()
        end

        if nrow(df) == 0
            @warn "No data returned for env_wastrt $year"
            continue
        end

        df[!, :fetch_date] .= Dates.format(Dates.now(), dateformat"yyyy-mm-ddTHH:MM:SS")
        table_name = "env_wastrt_$(year)"
        write_large_duckdb_table!(df, db_path, table_name)
    end

    return true
end

end # module MaterialRecyclingRatesFetch
