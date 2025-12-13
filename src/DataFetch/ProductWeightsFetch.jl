module ProductWeightsFetch

using DataFrames, Dates, DuckDB, CSV, DBInterface
using ..DatabaseAccess: write_large_duckdb_table!, table_exists
using ..AnalysisConfigLoader: load_product_mappings
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
    table_name = "prodcom_ds_056120_$(year)"
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
        SELECT time, prccode, decl, indicators, value
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

"""
    compute_average_weights_from_df(prodcom_df; year=nothing)

Helper function used during development to validate data-driven product weight calculations.
It expects a DataFrame with PRODCOM indicators (including `PRODQNT` and `QNTUNIT`) and
returns average weights per product/year pair by dividing total mass (tonnes) by total unit counts.
"""
function compute_average_weights_from_df(prodcom_df::DataFrame; year::Union{Nothing,Int}=nothing)
    required_columns = [:time, :prccode, :decl, :indicators, :value]
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
        key = (String(row.time), String(row.prccode), String(row.decl))
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
        decl_code = String(row.decl)
        unit = get(unit_lookup, (prod_time, prod_code, decl_code), nothing)
        parsed_value = parse_numeric_value(row.value)
        if unit === nothing || parsed_value === nothing || parsed_value <= 0
            continue
        end

        normalized = normalize_unit(unit)
        iso_geo = harmonize_country_code(decl_code, :prodcom)
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

    product_mapping = load_product_mappings()
    prodcom_codes = collect(product_mapping.prodcom_code)

    results_written = false
    for year in start_year:end_year
        @info "Calculating product average weights for year $year"
        prodcom_df = load_prodcom_quantity_data(db_path, year, prodcom_codes)
        if nrow(prodcom_df) == 0
            @warn "No PRODCOM quantity data found for year $year - skipping weight calculation"
            continue
        end

        weights_df = compute_average_weights_from_df(prodcom_df; year=year)
        if nrow(weights_df) == 0
            @warn "Unable to derive product weights for year $year (missing tonnes or units data)"
            continue
        end

        calculation_ts = Dates.format(Dates.now(), dateformat"yyyy-mm-ddTHH:MM:SS")
        output_df = DataFrame(
            product_code = weights_df.prodcom_code,
            geo = weights_df.geo,
            year = parse.(Int, weights_df.time),
            average_weight_kg = weights_df.average_weight_kg,
            tonnes_observed = weights_df.tonnes_observed,
            units_observed = weights_df.units_observed,
            calculation_date = fill(calculation_ts, nrow(weights_df))
        )

        table_name = "product_average_weights_$(year)"
        write_large_duckdb_table!(output_df, processed_db_path, table_name)
        results_written = true
    end

    return results_written
end

end # module ProductWeightsFetch
