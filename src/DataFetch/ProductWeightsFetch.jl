module ProductWeightsFetch

using DataFrames, Dates, DuckDB, CSV, DBInterface
using TOML
using ..DatabaseAccess: write_large_duckdb_table!, write_duckdb_table_with_connection!, table_exists
using ..AnalysisConfigLoader: prodcom_codes_for_year, load_product_mappings
using ..CountryCodeMapper: harmonize_country_code

export fetch_product_weights_data, compute_average_weights_from_df, build_product_weights_table, build_product_weights_table_with_conn

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
    tables = ["comext_ds_059341_$(year)"]
    code_list = unique(hs_codes)
    dfs = DataFrame[]

    for table_name in tables
        if !table_exists(db_path, table_name)
            continue
        end

        filter_clause = ""
        if !isempty(code_list)
            cleaned = [replace(code, "." => "") for code in code_list]
            quoted = join(["'$(code)'" for code in cleaned], ",")
            filter_clause = " AND product IN ($quoted)"
        end

        query = """
            SELECT time, product, reporter, indicators, value
            FROM \"$table_name\"
            WHERE indicators IN ('QUANTITY_KG')$filter_clause
        """

        db = DuckDB.DB(db_path)
        con = DBInterface.connect(db)
        try
            df = DataFrame(DuckDB.query(con, query))
            push!(dfs, df)
        finally
            DBInterface.close!(con)
            DBInterface.close!(db)
        end
    end

    return isempty(dfs) ? DataFrame() : reduce((a,b)->vcat(a,b, cols=:union), dfs)
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
    build_product_weights_table(years_range="2002-2023"; db_path_raw::String, db_path_processed::String, config_path=joinpath(@__DIR__, "..", "..", "config", "products.toml"))

Create a processed table `product_weights_YYYY` containing config weights and derived mass/counts per product/geo/year.
Derivation rules:
- If PRODCOM provides counts (QNTUNIT pieces) and config weight is available, derive total_mass_tonnes = counts * weight / 1000.
- If COMEXT provides mass (QUANTITY_KG) and config weight is available, derive unit_counts = mass_kg / weight.
- If both exist, keep both and mark source as "combined".
- If neither exists, store config weight with null mass/counts and source "config".
"""
function build_product_weights_table(years_range="2002-2023"; db_path_raw::String, db_path_processed::String, config_path=joinpath(@__DIR__, "..", "..", "config", "products.toml"))
    # Parse years
    years = split(years_range, "-")
    if length(years) == 1
        start_year = parse(Int, years[1])
        end_year = start_year
    elseif length(years) == 2
        start_year = parse(Int, years[1])
        end_year = parse(Int, years[2])
    else
        error("Invalid years format. Use 'YYYY' or 'YYYY-YYYY'.")
    end

    # Config weights
    cfg = TOML.parsefile(config_path)
    products_cfg = get(cfg, "products", Dict{String,Any}())
    weight_map = Dict{String,Float64}()
    for (_, pdata) in products_cfg
        weight = get(get(pdata, "parameters", Dict{String,Any}()), "weight_kg", nothing)
        prodcom_sets = get(get(pdata, "prodcom_codes", Dict{String,Any}()), "nace_rev2", String[])
        if weight === nothing
            continue
        end
        for code in prodcom_sets
            weight_map[replace(code, "." => "")] = Float64(weight)
        end
    end

    # HS -> prodcom mapping
    mapping_df = load_product_mappings()
    hs_to_prodcom = Dict{String,String}()
    for row in eachrow(mapping_df)
        for hs in split(String(row.hs_codes), ",")
            clean_hs = replace(strip(hs), "." => "")
            isempty(clean_hs) && continue
            hs_to_prodcom[clean_hs] = String(row.prodcom_code_clean)
        end
    end

    # Helper to parse numbers safely
    function _parse_float(x)
        try
            return parse(Float64, replace(string(x), "," => ""))
        catch
            return nothing
        end
    end

    # Allowed piece units
    piece_units = COUNT_UNITS

    for year in start_year:end_year
        # PRODCOM counts
        prodcom_counts = Dict{Tuple{String,String},Float64}() # (geo, prodcom) => counts
        prod_table = "prodcom_ds_059358_$(year)"
        if table_exists(db_path_raw, prod_table)
            db = DuckDB.DB(db_path_raw)
            con = DBInterface.connect(db)
            q_units = DataFrame(DuckDB.query(con, """
                SELECT reporter, prccode, value FROM "$prod_table" WHERE indicators = 'QNTUNIT'
            """))
            unit_lookup = Dict{Tuple{String,String},String}()
            for row in eachrow(q_units)
                unit_lookup[(String(row.reporter), String(row.prccode))] = lowercase(strip(String(row.value)))
            end
            q_counts = DataFrame(DuckDB.query(con, """
                SELECT reporter, prccode, value FROM "$prod_table" WHERE indicators = 'PRODQNT'
            """))
            for row in eachrow(q_counts)
                key = (String(row.reporter), String(row.prccode))
                unit = get(unit_lookup, key, nothing)
                unit === nothing && continue
                lowercase(unit) in piece_units || continue
                val = _parse_float(row.value)
                val === nothing && continue
                prodcom_counts[key] = get(prodcom_counts, key, 0.0) + val
            end
            DBInterface.close!(con); DBInterface.close!(db)
        end

        # COMEXT mass
        comext_mass = Dict{Tuple{String,String},Float64}() # (geo, prodcom) => kg
        com_table = "comext_ds_059341_$(year)"
        if table_exists(db_path_raw, com_table)
            db = DuckDB.DB(db_path_raw)
            con = DBInterface.connect(db)
            q_mass = DataFrame(DuckDB.query(con, """
                SELECT reporter, product, value FROM "$com_table" WHERE indicators = 'QUANTITY_KG'
            """))
            for row in eachrow(q_mass)
                hs = replace(strip(String(row.product)), "." => "")
                prod = get(hs_to_prodcom, hs, nothing)
                prod === nothing && continue
                val = _parse_float(row.value)
                val === nothing && continue
                key = (String(row.reporter), prod)
                comext_mass[key] = get(comext_mass, key, 0.0) + val
            end
            DBInterface.close!(con); DBInterface.close!(db)
        end

        # Build rows
        result = DataFrame(
            product_code = String[],
            geo = String[],
            year = Int[],
            weight_kg_config = Float64[],
            total_mass_tonnes = Union{Float64,Missing}[],
            unit_counts = Union{Float64,Missing}[],
            source = String[]
        )

        keys_set = union(collect(keys(prodcom_counts)), collect(keys(comext_mass)))
        for key in keys_set
            geo, prod = key
            weight = get(weight_map, prod, nothing)
            counts = get(prodcom_counts, key, 0.0)
            mass_kg = get(comext_mass, key, 0.0)
            total_mass_tonnes = missing
            unit_counts = missing
            src = "config"

            if counts > 0 && weight !== nothing
                total_mass_tonnes = counts * weight / 1000
                unit_counts = counts
                src = "prodcom_counts_config_mass"
            end

            if mass_kg > 0
                mass_t = mass_kg / 1000
                if total_mass_tonnes === missing
                    total_mass_tonnes = mass_t
                end
                if weight !== nothing && (unit_counts === missing || unit_counts == 0)
                    unit_counts = mass_kg / weight
                    src = "comext_mass_config_counts"
                elseif unit_counts !== missing
                    src = "combined"
                end
            end

            push!(result, (
                product_code = prod,
                geo = geo,
                year = year,
                weight_kg_config = weight === nothing ? 0.0 : weight,
                total_mass_tonnes = total_mass_tonnes,
                unit_counts = unit_counts,
                source = src
            ))
        end

        table_name = "product_weights_$(year)"
        write_large_duckdb_table!(result, db_path_processed, table_name)
    end
end

function build_product_weights_table_with_conn(years_range="2002-2023"; db_path_raw::String, conn_processed, config_path=joinpath(@__DIR__, "..", "..", "config", "products.toml"))
    # Reuse logic but write via provided connection
    years = split(years_range, "-")
    start_year = length(years) == 2 ? parse(Int, years[1]) : parse(Int, years[1])
    end_year = length(years) == 2 ? parse(Int, years[2]) : start_year

    cfg = TOML.parsefile(config_path)
    products_cfg = get(cfg, "products", Dict{String,Any}())
    weight_map = Dict{String,Float64}()
    for (_, pdata) in products_cfg
        weight = get(get(pdata, "parameters", Dict{String,Any}()), "weight_kg", nothing)
        prodcom_sets = get(get(pdata, "prodcom_codes", Dict{String,Any}()), "nace_rev2", String[])
        weight === nothing && continue
        for code in prodcom_sets
            weight_map[replace(code, "." => "")] = Float64(weight)
        end
    end

    mapping_df = load_product_mappings()
    hs_to_prodcom = Dict{String,String}()
    for row in eachrow(mapping_df)
        for hs in split(String(row.hs_codes), ",")
            clean_hs = replace(strip(hs), "." => "")
            isempty(clean_hs) && continue
            hs_to_prodcom[clean_hs] = String(row.prodcom_code_clean)
        end
    end

    function _parse_float(x)
        try
            return parse(Float64, replace(string(x), "," => ""))
        catch
            return nothing
        end
    end

    piece_units = COUNT_UNITS

    for year in start_year:end_year
        prodcom_counts = Dict{Tuple{String,String},Float64}()
        prod_table = "prodcom_ds_059358_$(year)"
        if table_exists(db_path_raw, prod_table)
            db = DuckDB.DB(db_path_raw)
            con = DBInterface.connect(db)
            q_units = DataFrame(DuckDB.query(con, """
                SELECT reporter, prccode, value FROM "$prod_table" WHERE indicators = 'QNTUNIT'
            """))
            unit_lookup = Dict{Tuple{String,String},String}()
            for row in eachrow(q_units)
                unit_lookup[(String(row.reporter), String(row.prccode))] = lowercase(strip(String(row.value)))
            end
            q_counts = DataFrame(DuckDB.query(con, """
                SELECT reporter, prccode, value FROM "$prod_table" WHERE indicators = 'PRODQNT'
            """))
            for row in eachrow(q_counts)
                key = (String(row.reporter), String(row.prccode))
                unit = get(unit_lookup, key, nothing)
                unit === nothing && continue
                lowercase(unit) in piece_units || continue
                val = _parse_float(row.value)
                val === nothing && continue
                prodcom_counts[key] = get(prodcom_counts, key, 0.0) + val
            end
            DBInterface.close!(con); DBInterface.close!(db)
        end

        comext_mass = Dict{Tuple{String,String},Float64}()
        com_table = "comext_ds_059341_$(year)"
        if table_exists(db_path_raw, com_table)
            db = DuckDB.DB(db_path_raw)
            con = DBInterface.connect(db)
            q_mass = DataFrame(DuckDB.query(con, """
                SELECT reporter, product, value FROM "$com_table" WHERE indicators = 'QUANTITY_KG'
            """))
            for row in eachrow(q_mass)
                hs = replace(strip(String(row.product)), "." => "")
                prod = get(hs_to_prodcom, hs, nothing)
                prod === nothing && continue
                val = _parse_float(row.value)
                val === nothing && continue
                key = (String(row.reporter), prod)
                comext_mass[key] = get(comext_mass, key, 0.0) + val
            end
            DBInterface.close!(con); DBInterface.close!(db)
        end

        result = DataFrame(
            product_code = String[],
            geo = String[],
            year = Int[],
            weight_kg_config = Float64[],
            total_mass_tonnes = Union{Float64,Missing}[],
            unit_counts = Union{Float64,Missing}[],
            source = String[]
        )

        keys_set = union(collect(keys(prodcom_counts)), collect(keys(comext_mass)))
        for key in keys_set
            geo, prod = key
            weight = get(weight_map, prod, nothing)
            counts = get(prodcom_counts, key, 0.0)
            mass_kg = get(comext_mass, key, 0.0)
            total_mass_tonnes = missing
            unit_counts = missing
            src = "config"

            if counts > 0 && weight !== nothing
                total_mass_tonnes = counts * weight / 1000
                unit_counts = counts
                src = "prodcom_counts_config_mass"
            end

            if mass_kg > 0
                mass_t = mass_kg / 1000
                if total_mass_tonnes === missing
                    total_mass_tonnes = mass_t
                end
                if weight !== nothing && (unit_counts === missing || unit_counts == 0)
                    unit_counts = mass_kg / weight
                    src = "comext_mass_config_counts"
                elseif unit_counts !== missing
                    src = "combined"
                end
            end

            push!(result, (
                product_code = prod,
                geo = geo,
                year = year,
                weight_kg_config = weight === nothing ? 0.0 : weight,
                total_mass_tonnes = total_mass_tonnes,
                unit_counts = unit_counts,
                source = src
            ))
        end

        table_name = "product_weights_$(year)"
        write_duckdb_table_with_connection!(result, conn_processed, table_name)
    end
end

"""
    fetch_product_weights_data(years_range="2002-2023"; db_path::String, processed_db_path::String=db_path)

Wrapper to populate `product_weights_YYYY` tables by combining config weights,
PRODCOM counts (pieces), and COMEXT mass (kg). Delegates to
`build_product_weights_table`.

# Arguments
- `years_range::String`: Year range to fetch (default: "2002-2023")
- `db_path::String`: Path to the raw DuckDB database (required keyword argument)
- `processed_db_path::String`: Target processed DuckDB (defaults to `db_path`)

# Returns
- `true` once the tables are written.
"""
function fetch_product_weights_data(years_range="2002-2023"; db_path::String, processed_db_path::String=db_path)
    build_product_weights_table(years_range; db_path_raw=db_path, db_path_processed=processed_db_path)
    return true
end

end # module ProductWeightsFetch
