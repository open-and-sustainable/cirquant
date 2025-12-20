module ProductCollectionRatesFetch

using DataFrames, Dates
using EurostatAPI
using ..DatabaseAccess: write_large_duckdb_table!
using ..AnalysisConfigLoader: load_product_mappings

export fetch_product_collection_rates_data

"""
    fetch_product_collection_rates_data(years_range="2002-2023"; db_path::String, product_keys_filter=nothing)

Fetch WEEE and battery collection datasets via EurostatAPI and write raw tables:
- env_waselee_YYYY (legacy WEEE scope)
- env_waseleeos_YYYY (open scope WEEE, 2018+)
- env_waspb_YYYY (portable batteries)

If `product_keys_filter` is provided, WEEE datasets are filtered to the `weee_waste_codes`
configured for those products. Battery dataset is left unfiltered (no WEEE codes).
"""
function fetch_product_collection_rates_data(years_range="2002-2023"; db_path::String, product_keys_filter=nothing)
    years = split(years_range, "-")
    start_year = length(years) == 2 ? parse(Int, years[1]) : parse(Int, years[1])
    end_year = length(years) == 2 ? parse(Int, years[2]) : start_year

    filter_keys = isnothing(product_keys_filter) ? nothing : Set(string.(product_keys_filter))
    mapping_df = load_product_mappings()
    selected = isnothing(filter_keys) ? mapping_df : filter(:product => (p -> string(p) in filter_keys), mapping_df)
    weee_codes = Set{String}()
    for row in eachrow(selected)
        for code in get(row, :weee_waste_codes, String[])
            push!(weee_codes, String(code))
        end
    end

    datasets = ["env_waselee", "env_waseleeos", "env_waspb"]

    for year in start_year:end_year
        for ds in datasets
            @info "Fetching $ds for $year via EurostatAPI"
            df = try
                EurostatAPI.fetch_dataset(ds, year)
            catch e
                @warn "EurostatAPI fetch failed" dataset=ds year=year exception=e
                DataFrame()
            end

            if nrow(df) == 0
                @warn "No data returned" dataset=ds year=year
                continue
            end

            if ds != "env_waspb" && !isempty(weee_codes) && :waste in names(df)
                df = filter(:waste => (w -> w in weee_codes), df)
            end

            df[!, :fetch_date] .= Dates.format(Dates.now(), dateformat"yyyy-mm-ddTHH:MM:SS")
            table_name = "$(ds)_$(year)"
            write_large_duckdb_table!(df, db_path, table_name)
        end
    end

    return true
end

end # module ProductCollectionRatesFetch
