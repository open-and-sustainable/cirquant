module ProductWeightsFetch

using DataFrames, Dates, DuckDB, CSV, DBInterface
using TOML
using ..DatabaseAccess: write_large_duckdb_table!, table_exists
using ..AnalysisConfigLoader: prodcom_codes_for_year, load_product_mappings
using ..CountryCodeMapper: harmonize_country_code

export fetch_product_weights_data, compute_average_weights_from_df

const MASS_UNIT_FACTORS = Dict(
    "t" => 1.0,
    "tonne" => 1.0,
    "tonnes" => 1.0,
    "tne" => 1.0,
    "1000 kg" => 1.0,
    "kg" => 0.001,
    "kilogram" => 0.001,
    "kilograms" => 0.001,
    "kgr" => 0.001,
    "g" => 1e-6,
    "gram" => 1e-6,
    "grams" => 1e-6
)

const COUNT_UNITS = Set([
    "p/st",
    "p/st.",
    "pst",
    "pieces",
    "piece",
    "units",
    "unit",
    "no",
    "number",
    "u"
])

normalize_unit(unit::AbstractString) = lowercase(strip(unit))

function parse_numeric_value(val)
    if val === missing || val === nothing
        return nothing
    elseif val isa Number
        value = Float64(val)
        return isfinite(value) ? value : nothing
    elseif val isa AbstractString
        cleaned = replace(strip(val), ',' => "")
        try
            return parse(Float64, cleaned)
        catch
            return nothing
        end
    else
        return nothing
    end
end

function load_prodcom_quantity_data(db_path::String, year::Int, prodcom_codes::Vector{String})
    table_name = "prodcom_ds_059358_$(year)"
    if !table_exists(db_path, table_name)
        @warn "PRODCOM table $(table_name) not found in raw database, skipping year $(year)"
        return DataFrame()
    end

    code_list = unique(prodcom_codes)
    filter_clause = ""
    if !isempty(code_list)
        cleaned = [replace(code, "." => "") for code in code_list]
        quoted = join(["'$(code)'" for code in cleaned], ",")
        filter_clause = " AND prccode IN ($quoted)"
    end

    query = """
        SELECT time, prccode, reporter, indicators, value
        FROM \"$table_name\"
        WHERE indicators IN ('PRODQNT', 'QNTUNIT')$filter_clause
    """

    db = DuckDB.DB(db_path)
    con = DBInterface.connect(db)
    try
        df = DataFrame(DuckDB.query(con, query))
        return df
    finally
        DBInterface.close!(con)
        DBInterface.close!(db)
    end
end

function load_comext_quantity_data(db_path::String, year::Int, hs_codes::Vector{String})
    table_name = "comext_ds_059341_$(year)"
    if !table_exists(db_path, table_name)
        @warn "COMEXT table $(table_name) not found in raw database, skipping year $(year)"
        return DataFrame()
    end

    code_list = unique(hs_codes)
    filter_clause = ""
    if !isempty(code_list)
        cleaned = [replace(code, "." => "") for code in code_list]
        quoted = join(["'$(code)'" for code in cleaned], ",")
        filter_clause = " AND product IN ($quoted)"
    end

    query = """
        SELECT time, product, reporter, indicators, value
        FROM \"$table_name\"
        WHERE indicators IN ('QUANTITY_KG', 'SUP_QUANTITY')$filter_clause
    """

    db = DuckDB.DB(db_path)
    con = DBInterface.connect(db)
    try
        df = DataFrame(DuckDB.query(con, query))
        return df
    finally
        DBInterface.close!(con)
        DBInterface.close!(db)
    end
end

"""
    compute_average_weights_from_df(prodcom_df; year=nothing)

Helper function used during development to validate data-driven product weight calculations.
It expects a DataFrame with PRODCOM indicators (including `PRODQNT` and `QNTUNIT`) and
returns average weights per product/year pair by dividing total mass (tonnes) by total unit counts.
"""
function compute_average_weights_from_df(prodcom_df::DataFrame; year::Union{Nothing,Int}=nothing)
    required_columns = [:time, :prccode, :reporter, :indicators, :value]
    missing_cols = setdiff(required_columns, Symbol.(names(prodcom_df)))
    if !isempty(missing_cols)
        error("DataFrame missing required PRODCOM columns: $(missing_cols)")
    end

    if year !== nothing
        target_year = string(year)
        prodcom_df = filter(:time => (t -> t == target_year), prodcom_df)
    end

    qntunit_rows = filter(:indicators => (x -> uppercase(x) == "QNTUNIT"), prodcom_df)
    unit_lookup = Dict{Tuple{String,String,String},String}()
    for row in eachrow(qntunit_rows)
        key = (String(row.time), String(row.prccode), String(row.reporter))
        unit_lookup[key] = normalize_unit(String(row.value))
    end

    prodqnt_rows = filter(:indicators => (x -> uppercase(x) == "PRODQNT"), prodcom_df)
    mass_totals_geo = Dict{Tuple{String,String,String},Float64}()
    unit_totals_geo = Dict{Tuple{String,String,String},Float64}()
    mass_totals_eu = Dict{Tuple{String,String},Float64}()
    unit_totals_eu = Dict{Tuple{String,String},Float64}()

    for row in eachrow(prodqnt_rows)
        prod_time = String(row.time)
        prod_code = String(row.prccode)
        reporter_code = String(row.reporter)
        unit = get(unit_lookup, (prod_time, prod_code, reporter_code), nothing)
        parsed_value = parse_numeric_value(row.value)
        if unit === nothing || parsed_value === nothing || parsed_value <= 0
            continue
        end

        normalized = normalize_unit(unit)
        iso_geo = harmonize_country_code(reporter_code, :prodcom)
        geo_key = (prod_time, prod_code, iso_geo)
        eu_key = (prod_time, prod_code)

        if haskey(MASS_UNIT_FACTORS, normalized)
            factor = MASS_UNIT_FACTORS[normalized]
            mass_totals_geo[geo_key] = get(mass_totals_geo, geo_key, 0.0) + parsed_value * factor
            mass_totals_eu[eu_key] = get(mass_totals_eu, eu_key, 0.0) + parsed_value * factor
        elseif normalized in COUNT_UNITS
            unit_totals_geo[geo_key] = get(unit_totals_geo, geo_key, 0.0) + parsed_value
            unit_totals_eu[eu_key] = get(unit_totals_eu, eu_key, 0.0) + parsed_value
        end
    end

    results = DataFrame(
        time = String[],
        prodcom_code = String[],
        geo = String[],
        average_weight_kg = Float64[],
        tonnes_observed = Float64[],
        units_observed = Float64[]
    )

    for (key, tonnes) in mass_totals_geo
        units = get(unit_totals_geo, key, 0.0)
        if units <= 0
            continue
        end
        avg_tonnes_per_unit = tonnes / units
        push!(results, (
            time = key[1],
            prodcom_code = key[2],
            geo = key[3],
            average_weight_kg = avg_tonnes_per_unit * 1000,
            tonnes_observed = tonnes,
            units_observed = units
        ))
    end

    for (key, tonnes) in mass_totals_eu
        units = get(unit_totals_eu, key, 0.0)
        if units <= 0
            continue
        end
        avg_tonnes_per_unit = tonnes / units
        push!(results, (
            time = key[1],
            prodcom_code = key[2],
            geo = "EU27_2020",
            average_weight_kg = avg_tonnes_per_unit * 1000,
            tonnes_observed = tonnes,
            units_observed = units
        ))
    end

    return results
end

"""
    compute_comext_weights_from_df(comext_df, hs_to_prodcom_map)

Compute kg-per-unit from COMEXT data where both net mass (kg) and supplementary counts are present.
Returns a DataFrame keyed by prodcom_code, geo, year with derived average_weight_kg.
"""
function compute_comext_weights_from_df(comext_df::DataFrame, hs_to_prodcom_map::Dict{String,String})
    required_columns = [:time, :product, :reporter, :indicators, :value]
    missing_cols = setdiff(required_columns, Symbol.(names(comext_df)))
    if !isempty(missing_cols)
        error("DataFrame missing required COMEXT columns: $(missing_cols)")
    end

    mass_totals = Dict{Tuple{String,String,String},Float64}()   # (time, product, reporter) => kg
    count_totals = Dict{Tuple{String,String,String},Float64}()  # (time, product, reporter) => count

    for row in eachrow(comext_df)
        key = (String(row.time), String(row.product), String(row.reporter))
        parsed_value = parse_numeric_value(row.value)
        parsed_value === nothing && continue

        ind = uppercase(String(row.indicators))
        if ind == "QUANTITY_KG"
            mass_totals[key] = get(mass_totals, key, 0.0) + parsed_value
        elseif ind == "SUP_QUANTITY"
            count_totals[key] = get(count_totals, key, 0.0) + parsed_value
        end
    end

    results = DataFrame(
        time = String[],
        prodcom_code = String[],
        geo = String[],
        average_weight_kg = Float64[],
        tonnes_observed = Float64[],
        units_observed = Float64[],
        source = String[]
    )

    for (key, mass_kg) in mass_totals
        count = get(count_totals, key, 0.0)
        if count <= 0 || mass_kg <= 0
            continue
        end
        hs_code = key[2]
        prodcom_code = get(hs_to_prodcom_map, hs_code, nothing)
        prodcom_code === nothing && continue

        push!(results, (
            time = key[1],
            prodcom_code = prodcom_code,
            geo = harmonize_country_code(key[3], :comext),
            average_weight_kg = mass_kg / count,
            tonnes_observed = mass_kg / 1000, # keep parallel metric for parity with prodcom result
            units_observed = count,
            source = "comext"
        ))
    end

    return results
end

function _default_weight_map(config_path::String=joinpath(@__DIR__, "..", "..", "config", "products.toml"))
    cfg = TOML.parsefile(config_path)
    products = get(cfg, "products", Dict{String,Any}())
    defaults = Dict{String,Float64}()
    for (_, pdata) in products
        weight = get(get(pdata, "parameters", Dict{String,Any}()), "weight_kg", nothing)
        codes = get(get(pdata, "prodcom_codes", Dict{String,Any}()), "nace_rev2", nothing)
        if weight === nothing || codes === nothing
            continue
        end
        for code in codes
            defaults[replace(code, "." => "")] = Float64(weight)
        end
    end
    return defaults
end

"""
    fetch_product_weights_data(years_range="2002-2023"; db_path::String)

Calculates average product weights from PRODCOM quantity/value data.
This replaces hardcoded weights in the configuration with data-driven values.

# Arguments
- `years_range::String`: Year range to fetch (default: "2002-2023")
- `db_path::String`: Path to the raw DuckDB database (required keyword argument)

# Notes
- Derived from existing PRODCOM tables (not a new dataset)
- Calculates: total_tonnes / total_units for each product
- Data structure: Rows by product × geo
"""
function fetch_product_weights_data(years_range="2002-2023"; db_path::String, processed_db_path::String=db_path)
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

    results_written = false
    default_weights = _default_weight_map()
    for year in start_year:end_year
        @info "Calculating product average weights for year $year"
        code_info = prodcom_codes_for_year(year)

        prodcom_df = load_prodcom_quantity_data(db_path, year, code_info.codes_original)
        mapping_df = load_product_mappings()
        hs_list = String[]
        for hs_entry in unique(mapping_df.hs_codes)
            for code in split(String(hs_entry), ",")
                clean_code = replace(strip(code), "." => "")
                isempty(clean_code) && continue
                push!(hs_list, clean_code)
            end
        end
        comext_df = load_comext_quantity_data(db_path, year, hs_list)

        weights_sources = DataFrame()

        if nrow(comext_df) > 0
            # Build HS -> prodcom map (use year-appropriate prodcom codes)
            hs_to_prodcom = Dict{String,String}()
            for row in eachrow(mapping_df)
                if row.epoch_start_year <= year <= row.epoch_end_year
                    for hs in split(String(row.hs_codes), ",")
                        clean_hs = replace(strip(hs), "." => "")
                        hs_to_prodcom[clean_hs] = String(row.prodcom_code_clean)
                    end
                end
            end
            comext_weights = compute_comext_weights_from_df(comext_df, hs_to_prodcom)
            if nrow(comext_weights) > 0
                weights_sources = nrow(weights_sources) == 0 ? comext_weights : vcat(weights_sources, comext_weights, cols=:union)
            end
        end

        if nrow(prodcom_df) > 0
            prodcom_weights = compute_average_weights_from_df(prodcom_df; year=year)
            if nrow(prodcom_weights) > 0
                prodcom_weights[!, :source] .= "prodcom"
                weights_sources = nrow(weights_sources) == 0 ? prodcom_weights : vcat(weights_sources, prodcom_weights, cols=:union)
            end
        end

        # Add defaults for missing keys
        combined = DataFrame(
            product_code = String[],
            geo = String[],
            year = Int[],
            average_weight_kg = Float64[],
            tonnes_observed = Float64[],
            units_observed = Float64[],
            calculation_date = String[],
            source = String[]
        )

        # prefer comext, then prodcom
        key_seen = Set{Tuple{String,String}}()
        if nrow(weights_sources) > 0
            for row in eachrow(weights_sources)
                key = (String(row.prodcom_code), String(row.geo))
                push!(key_seen, key)
                push!(combined, (
                    product_code = String(row.prodcom_code),
                    geo = String(row.geo),
                    year = parse(Int, row.time),
                    average_weight_kg = row.average_weight_kg,
                    tonnes_observed = get(row, :tonnes_observed, 0.0),
                    units_observed = get(row, :units_observed, 0.0),
                    calculation_date = Dates.format(Dates.now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
                    source = get(row, :source, "unknown")
                ))
            end
        end

        # default fallback per product with EU-level geo
        for code in code_info.codes_clean
            key = (code, "EU27_2020")
            if !(key in key_seen) && haskey(default_weights, code)
                push!(combined, (
                    product_code = code,
                    geo = "EU27_2020",
                    year = year,
                    average_weight_kg = default_weights[code],
                    tonnes_observed = 0.0,
                    units_observed = 0.0,
                    calculation_date = Dates.format(Dates.now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
                    source = "config_fallback"
                ))
            end
        end

        if nrow(combined) == 0
            @warn "Unable to derive product weights for year $year (missing tonnes or units data)"
            continue
        end

        table_name = "product_average_weights_$(year)"
        write_large_duckdb_table!(combined, processed_db_path, table_name)
        results_written = true
    end

    return results_written
end

end # module ProductWeightsFetch
