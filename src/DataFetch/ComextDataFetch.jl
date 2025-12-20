module ComextDataFetch

using DataFrames, Dates, DuckDB, CSV, ComextAPI, TOML
import ..AnalysisConfigLoader
using ..DatabaseAccess: write_large_duckdb_table!
using ..AnalysisConfigLoader: load_product_mappings

export fetch_comext_data

"""
    fetch_comext_data(years_range="2002-2023", custom_datasets=nothing; db_path::String)

Fetches COMEXT data from Eurostat API for dataset DS-059341 using HS codes from AnalysisConfigLoader.
Fetches VALUE_EUR and QUANTITY_KG indicators for both imports and exports,
for intra-EU and extra-EU trade.
Data is saved to DuckDB tables in the raw database.

# Arguments
- `years_range::String`: Year range to fetch (default: "2002-2023")
- `custom_datasets`: If empty only DS-059341 is used
- `db_path::String`: Path to the raw DuckDB database (required keyword argument)
"""
function fetch_comext_data(years_range="2002-2023", custom_datasets=nothing; db_path::String, product_keys_filter=nothing, prefer_cn8::Bool=false)
    # Parse years
    years = split(years_range, "-")
    if length(years) == 1
        start_year = parse(Int, years[1])
        end_year = start_year
    elseif length(years) == 2
        start_year = parse(Int, years[1])
        end_year = parse(Int, years[2])
    else
        error("Invalid years format. Use either 'YYYY' for a single year or 'YYYY-YYYY' for a range.")
    end

    # Fixed parameters
    default_datasets = prefer_cn8 ? ["ds-059322"] : ["ds-059341"]
    datasets = isnothing(custom_datasets) ? default_datasets : custom_datasets
    freq = "A"  # Annual frequency

    # Define indicators (include supplementary quantity when available for unit counts)
    indicators = ["VALUE_EUR", "QUANTITY_KG", "SUP_QUANTITY"]

    # Define partners (intra-EU and extra-EU)
    partners = Dict(
        "INTRA_EU" => "INT_EU27_2020",
        "EXTRA_EU" => "EXT_EU27_2020"
    )

    # Define flows
    flows = Dict(
        "IMPORT" => 1,
        "EXPORT" => 2
    )

    @info "Using database path: $db_path"

    # Ensure directories exist
    db_dir = dirname(db_path)
    if !isdir(db_dir)
        mkpath(db_dir)
    end

    # Get product codes from configuration
    product_mapping = load_product_mappings()
    cfg = TOML.parsefile(AnalysisConfigLoader.PRODUCTS_CONFIG_PATH)
    products_cfg = get(cfg, "products", Dict{String,Any}())
    filter_set = isnothing(product_keys_filter) ? nothing : Set(string.(product_keys_filter))

    function collect_codes(key::String; default_from_mapping=false)
        codes = Set{String}()
        if default_from_mapping
            for hs_code_entry in product_mapping.hs_codes
                for code in split(hs_code_entry, ",")
                    clean_code = replace(strip(code), "." => "")
                    isempty(clean_code) && continue
                    push!(codes, clean_code)
                end
            end
            return collect(codes)
        end
        for (k, pdata) in products_cfg
            if filter_set !== nothing && !(k in filter_set)
                continue
            end
            for code in get(pdata, key, String[])
                clean_code = replace(strip(string(code)), "." => "")
                isempty(clean_code) && continue
                push!(codes, clean_code)
            end
        end
        return collect(codes)
    end

    cn8_codes = collect_codes("cn8_codes")
    hs_codes = collect_codes("hs_codes"; default_from_mapping=filter_set === nothing)

    code_list = if prefer_cn8 && !isempty(cn8_codes)
        @info "Using CN8 codes for COMEXT fetch (prefer_cn8=true)"
        cn8_codes
    else
        if prefer_cn8 && isempty(cn8_codes)
            @warn "prefer_cn8=true but no cn8_codes found for selected products; falling back to HS codes"
        end
        hs_codes
    end

    @info "Found $(length(code_list)) unique $(prefer_cn8 ? \"CN8\" : \"HS\") codes to fetch"

    # Track statistics
    stats = Dict(
        :total_queries => 0,
        :successful => 0,
        :failed => 0,
        :rows_processed => 0
    )

    # Process each dataset and year
    for dataset in datasets
        @info "Processing dataset: $dataset"
        # Process each year
        for year in start_year:end_year
            @info "Processing year: $year"

            # Collect all data for this year
            year_data = DataFrame()

            # Process each product code (HS or CN8 depending on selection)
            for hs_code in code_list
                # Process each indicator
                for indicator in indicators
                    # Process each partner type
                    for (partner_type, partner_code) in partners
                        # Process each flow
                        for (flow_type, flow_code) in flows
                            @info "Fetching: HS=$hs_code, Indicator=$indicator, Partner=$partner_type, Flow=$flow_type"
                            stats[:total_queries] += 1

                            # Log the exact API call parameters
                            #@info "API call parameters:" dataset=dataset year=year indicator=indicator product=hs_code freq=freq partner=partner_code flow=flow_code
                            #@info "Expected API call: fetch_comext_data(\"$dataset\", $year, \"$indicator\", \"$hs_code\", \"$freq\", \"$partner_code\", $flow_code)"

                            try
                                # Fetch data using ComextAPI
                                df = ComextAPI.fetch_comext_data(
                                    dataset,
                                    year,
                                    indicator,
                                    hs_code,
                                    freq,
                                    partner_code,
                                    flow_code
                                )

                                # Add delay to avoid rate limiting
                                sleep(5)  # 5 seconds delay between API calls

                                if !isnothing(df) && nrow(df) > 0
                                    # Add metadata columns for clarity
                                    df[!, :hs_code_query] .= hs_code
                                    df[!, :indicator_query] .= indicator
                                    df[!, :partner_type] .= partner_type
                                    df[!, :partner_code] .= partner_code
                                    df[!, :flow_type] .= flow_type
                                    df[!, :flow_code] .= flow_code
                                    df[!, :fetch_date] .= now()

                                    # Convert value columns to strings to handle mixed types
                                    if hasproperty(df, :value)
                                        df[!, :value] = string.(df.value)
                                    end

                                    # Append to year data
                                    if nrow(year_data) == 0
                                        year_data = df
                                    else
                                        year_data = vcat(year_data, df, cols=:union)
                                    end

                                    @info "Retrieved $(nrow(df)) rows"
                                else
                                    @debug "No data for combination"
                                end

                            catch e
                                @warn "Failed to fetch data" hs_code indicator partner_type flow_type exception=e
                                stats[:failed] += 1

                                # Add delay even on failure to avoid rate limiting
                                sleep(10)  # 10 seconds delay after failed requests
                            end
                        end
                    end
                end
            end

            # Save the collected data for this year
            if nrow(year_data) > 0
                table_name = "comext_$(replace(dataset, "-" => "_"))_$year"

                try
                    # Write to database
                    @info "Writing $(nrow(year_data)) rows to table $table_name"
                    write_large_duckdb_table!(year_data, db_path, table_name)

                    stats[:successful] += 1
                    stats[:rows_processed] += nrow(year_data)
                    @info "✓ Successfully saved data to table $table_name"

                catch e
                    @error "Failed to write data to database" table=table_name exception=e
                    stats[:failed] += 1

                    # Save as backup CSV
                    backup_dir = joinpath(db_dir, "..", "logs", "backups")
                    mkpath(backup_dir)
                    backup_file = joinpath(backup_dir, "backup_$(table_name)_$(round(Int, time())).csv")

                    try
                        CSV.write(backup_file, year_data)
                        @info "Saved backup to $backup_file"
                    catch csv_err
                        @error "Failed to save backup CSV" exception=csv_err
                    end
                end
            else
                @warn "No data collected for year $year"
                stats[:failed] += 1
            end
        end
    end

    # Report final statistics
    @info "COMEXT data fetching completed:"
    @info "  Total queries made: $(stats[:total_queries])"
    @info "  Years successfully processed: $(stats[:successful])"
    @info "  Failed queries: $(stats[:failed])"
    @info "  Total rows processed: $(stats[:rows_processed])"

    return stats
end

end # module
