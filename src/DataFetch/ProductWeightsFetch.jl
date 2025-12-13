module ProductWeightsFetch

using DataFrames, Dates, DuckDB, CSV
using ..DatabaseAccess: write_large_duckdb_table!
using ..AnalysisConfigLoader: load_product_mappings

export fetch_product_weights_data

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
        unit_lookup[(String(row.time), String(row.prccode), String(row.decl))] = normalize_unit(String(row.value))
    end

    prodqnt_rows = filter(:indicators => (x -> uppercase(x) == "PRODQNT"), prodcom_df)
    mass_totals = Dict{Tuple{String,String},Float64}()
    unit_totals = Dict{Tuple{String,String},Float64}()

    for row in eachrow(prodqnt_rows)
        key_full = (String(row.time), String(row.prccode), String(row.decl))
        unit = get(unit_lookup, key_full, nothing)
        parsed_value = parse_numeric_value(row.value)
        if unit === nothing || parsed_value === nothing || parsed_value <= 0
            continue
        end

        normalized = normalize_unit(unit)
        product_key = (key_full[1], key_full[2])  # aggregate across geographies for now

        if haskey(MASS_UNIT_FACTORS, normalized)
            mass_totals[product_key] = get(mass_totals, product_key, 0.0) + parsed_value * MASS_UNIT_FACTORS[normalized]
        elseif normalized in COUNT_UNITS
            unit_totals[product_key] = get(unit_totals, product_key, 0.0) + parsed_value
        end
    end

    results = DataFrame(
        time = String[],
        prodcom_code = String[],
        average_weight_kg = Float64[],
        tonnes_observed = Float64[],
        units_observed = Float64[]
    )

    for (key, tonnes) in mass_totals
        units = get(unit_totals, key, 0.0)
        if units <= 0
            continue
        end
        avg_tonnes_per_unit = tonnes / units
        push!(results, (
            time = key[1],
            prodcom_code = key[2],
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
function fetch_product_weights_data(years_range="2002-2023"; db_path::String)
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

    @info "Product weights calculation not yet implemented"
    @info "Will derive from existing PRODCOM quantity/value ratios"

    # Get product mappings to know which products to calculate
    product_mapping = load_product_mappings()
    unique_prodcom_codes = unique(product_mapping.prodcom_code)
    @info "Would calculate average weights for $(length(unique_prodcom_codes)) products"

    @warn "Product weights calculation is a stub - implementation pending"

    for year in start_year:end_year
        @info "Year $year: Would calculate average weights from PRODCOM data"
    end

    return nothing
end

end # module ProductWeightsFetch
