module UmpDataFetch

using CSV
using DataFrames
using Dates
using HTTP
using TOML
using ..DatabaseAccess: write_large_duckdb_table!
using ..AnalysisConfigLoader: load_product_mappings, PRODUCTS_CONFIG_PATH

export fetch_ump_weee_data, fetch_ump_battery_data

const DEFAULT_UMP_DOWNLOAD_URL = "https://www.urbanmineplatform.eu/download"
const UMP_CHARTS_CSV_NAME = "weee_forcharts.csv"
const UMP_SANKEY_CSV_NAME = "weee_forsankey.csv"
const UMP_WEEE_CODE_MAP = Dict(
    "EE_TEE" => "WEEE_Cat1",
    "EE_SME" => "WEEE_Cat5",
    "EE_SITTE" => "WEEE_Cat6",
    "EE_LE_PVP" => "WEEE_Cat4b",
    "EE_LE" => "WEEE_Cat4a",
)

"""
    fetch_ump_weee_data(; db_path::String, download_url::String=DEFAULT_UMP_DOWNLOAD_URL,
                           dataset_path::Union{Nothing,String}=nothing,
                           years_range::Union{Nothing,String}=nothing,
                           product_keys_filter=nothing)

Download the Urban Mine Platform (UMP) WEEE dataset, extract historical country-by-year
observations from the charts CSV into `ump_weee_history`, and store sankey flow data
from the sankey CSV into `ump_weee_sankey` (historical scenarios only).

If `dataset_path` is not provided, the fetcher looks for a local download in `temp/` at
the project root before attempting a remote download. When no dataset is found, the
function logs a warning and returns `false` without erroring.

When `years_range` is provided, only rows within that range are retained. When
`product_keys_filter` is provided, rows are filtered to the UMP WEEE categories mapped
from the selected product keys. When it is not provided, the filter defaults to all
products in `config/products.toml` that define WEEE waste codes (batteries are not
covered by UMP yet).

Only historical records are kept (rows whose scenario values include "hist"). Each record
includes the geography, year, product/category label, an inferred metric name, unit when
present, and the associated CirQuant product key when a WEEE code match is detected. Source
metadata (`source_file`, `source_sheet`, `fetch_date`) are attached to every row.
"""
function fetch_ump_weee_data(; db_path::String, download_url::String=DEFAULT_UMP_DOWNLOAD_URL,
                                dataset_path::Union{Nothing,String}=nothing,
                                years_range::Union{Nothing,String}=nothing,
                                product_keys_filter=nothing)
    path = if isnothing(dataset_path)
        local_path = _find_local_ump_file()
        if local_path !== nothing
            @info "Using local UMP dataset" path=local_path
            local_path
        else
            _download_ump_file(download_url)
        end
    else
        if isfile(dataset_path)
            dataset_path
        else
            @warn "UMP dataset path not found; skipping" dataset_path
            return false
        end
    end

    if path === nothing
        @warn "UMP dataset not available; place the file in temp/ or pass dataset_path"
        return false
    end
    product_lookup = _build_weee_lookup()
    allowed_categories = _ump_categories_for_products(product_keys_filter)
    allowed_years = _parse_years_range(years_range)
    if product_keys_filter === nothing
        if allowed_categories === nothing
            @warn "No WEEE categories configured in products.toml; UMP fetch will load all categories"
        else
            @info "Filtering UMP WEEE to configured products with WEEE codes; batteries not covered yet" category_count=length(allowed_categories)
        end
    end
    csvs = _locate_ump_csvs(path)
    history_loaded = false
    sankey_loaded = false

    if csvs.charts === nothing
        @warn "UMP charts CSV not found; cannot build history table" path
    else
        history = _extract_ump_history(csvs.charts, product_lookup, allowed_categories, allowed_years)
        if isempty(history)
            @warn "UMP charts CSV produced no historical rows" path=csvs.charts
        else
            write_large_duckdb_table!(history, db_path, "ump_weee_history")
            history_loaded = true
        end
    end

    if csvs.sankey !== nothing
        sankey = _extract_ump_sankey(csvs.sankey, allowed_categories, allowed_years)
        if isempty(sankey)
            @warn "UMP sankey CSV produced no historical rows" path=csvs.sankey
        else
            write_large_duckdb_table!(sankey, db_path, "ump_weee_sankey")
            sankey_loaded = true
        end
    else
        @info "UMP sankey CSV not found; skipping sankey table" path
    end

    return history_loaded || sankey_loaded
end

"""
    fetch_ump_battery_data(; db_path::String)

Placeholder for future Urban Mine Platform battery imports. Logs a message and
returns `nothing` until a dataset is available for integration.
"""
function fetch_ump_battery_data(; db_path::String)
    @info "No UMP battery dataset available yet; skipping" db_path
    return nothing
end

function _download_ump_file(download_url::String)
    tmp_path = tempname() * ".bin"
    @info "Downloading UMP data" url=download_url dest=tmp_path

    response = try
        HTTP.request("GET", download_url; status_exception=false)
    catch e
        @warn "Failed to download UMP dataset" exception=e
        return nothing
    end
    if response.status != 200
        @warn "Failed to download UMP dataset" status=response.status url=download_url
        return nothing
    end

    open(tmp_path, "w") do io
        write(io, response.body)
    end

    return tmp_path
end

function _find_local_ump_file()
    local_dir = normpath(joinpath(@__DIR__, "..", "..", "temp"))
    isdir(local_dir) || return nothing
    candidates = String[]
    for entry in readdir(local_dir; join=true)
        lower_entry = lowercase(entry)
        if endswith(lower_entry, ".zip") || endswith(lower_entry, ".xlsx") || endswith(lower_entry, ".xls")
            push!(candidates, entry)
        elseif endswith(lower_entry, ".csv")
            push!(candidates, entry)
        end
    end
    isempty(candidates) && return nothing

    best = candidates[1]
    best_rank = _ump_candidate_rank(best)
    for candidate in candidates[2:end]
        rank = _ump_candidate_rank(candidate)
        if rank < best_rank
            best = candidate
            best_rank = rank
        end
    end
    return best
end

function _ump_candidate_rank(path::String)
    name = lowercase(basename(path))
    return occursin("weee", name) ? 0 : 1
end

function _locate_ump_csvs(path::String)
    lower_path = lowercase(path)
    if endswith(lower_path, ".zip")
        return _extract_zip_csvs(path)
    elseif endswith(lower_path, ".csv")
        name = lowercase(basename(path))
        if name == UMP_CHARTS_CSV_NAME
            return (charts=path, sankey=nothing)
        elseif name == UMP_SANKEY_CSV_NAME
            return (charts=nothing, sankey=path)
        else
            @warn "Unrecognized UMP CSV filename; expected WEEE_ForCharts.csv or WEEE_ForSankey.csv" path
            return (charts=nothing, sankey=nothing)
        end
    end

    magic = _file_magic(path)
    if _looks_like_zip_archive(magic)
        return _extract_zip_csvs(path)
    end

    @warn "Unrecognized file type for UMP download; skipping" path
    return (charts=nothing, sankey=nothing)
end

function _extract_zip_csvs(zip_path::String)
    extract_dir = mktempdir()
    try
        run(pipeline(`unzip -o $zip_path -d $extract_dir`, stdout=devnull, stderr=devnull))
    catch e
        @error "Failed to unzip UMP archive" exception=e zip_path
        return String[]
    end

    workbooks = String[]
    charts = nothing
    sankey = nothing
    for (root, _, files) in walkdir(extract_dir)
        for file in files
            lower_file = lowercase(file)
            endswith(lower_file, ".csv") || continue
            full_path = joinpath(root, file)
            if lower_file == UMP_CHARTS_CSV_NAME
                charts = full_path
            elseif lower_file == UMP_SANKEY_CSV_NAME
                sankey = full_path
            end
        end
    end

    if charts === nothing && sankey === nothing
        @warn "No UMP CSV files found in UMP archive" zip_path
    elseif charts === nothing
        @warn "UMP charts CSV not found in UMP archive" zip_path
    end
    return (charts=charts, sankey=sankey)
end

function _file_magic(path::String, n::Int=4)
    try
        open(path, "r") do io
            return read(io, min(n, filesize(path)))
        end
    catch e
        @warn "Failed to inspect UMP download" path exception=e
        return UInt8[]
    end
end

_looks_like_zip_archive(bytes) = length(bytes) >= 4 && bytes[1:4] == UInt8[0x50, 0x4B, 0x03, 0x04]

function _read_ump_csv(csv_path::String)
    df = try
        CSV.read(csv_path, DataFrame)
    catch e
        @warn "Failed to read UMP CSV" csv_path exception=e
        return DataFrame()
    end
    return df
end

function _normalize_ump_csv_columns(df::DataFrame)
    normalized = _normalize_columns(df)
    renames = Pair{Symbol,Symbol}[]
    for name in names(normalized)
        name_str = String(name)
        old_name = name isa Symbol ? name : Symbol(name)
        if name_str == "stock/flow_id"
            push!(renames, old_name => :stock_flow_id)
        elseif name_str == "additionalspecification"
            push!(renames, old_name => :additional_specification)
        end
    end
    isempty(renames) || rename!(normalized, renames)
    return normalized
end

function _extract_ump_history(
    csv_path::String,
    product_lookup::Dict{String,String},
    allowed_categories::Union{Nothing,Set{String}},
    allowed_years::Union{Nothing,Set{Int}}
)
    df = _read_ump_csv(csv_path)
    isempty(df) && return df

    normalized = _normalize_ump_csv_columns(df)
    geo_col = _find_column(normalized, r"location|geo|country|member|nation")
    year_col = _find_column(normalized, r"^year$")
    scenario_col = _find_column(normalized, r"scenario")
    unit_col = _find_column(normalized, r"unit")
    value_col = _find_column(normalized, r"^value$")
    stock_flow_col = _find_column(normalized, r"stock_flow_id")
    product_col = _find_column(normalized, r"layer_1|layer1")
    product_col === nothing && (product_col = _find_column(normalized, r"waste_stream|product|category|equipment|weee"))

    if geo_col === nothing || year_col === nothing || value_col === nothing
        @warn "UMP charts CSV missing required columns" csv_path geo_col year_col value_col
        return DataFrame()
    end

    fetch_date = Dates.format(Dates.now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    rows = DataFrame(geo=String[], year=Int[], product_label=String[], metric=String[], value=Float64[], unit=Union{String,Missing}[], product_key=Union{String,Missing}[], source_sheet=String[], source_file=String[], fetch_date=String[])
    scenario_values = scenario_col === nothing ? fill("", nrow(normalized)) : normalized[:, scenario_col]
    unit_values = unit_col === nothing ? fill(missing, nrow(normalized)) : normalized[:, unit_col]
    metric_values = stock_flow_col === nothing ? fill("value", nrow(normalized)) : normalized[:, stock_flow_col]
    product_values = product_col === nothing ? fill("WEEE", nrow(normalized)) : normalized[:, product_col]
    source_file = basename(csv_path)

    for r in 1:nrow(normalized)
        _is_historical(scenario_values[r]) || continue
        year = _parse_number(normalized[r, year_col])
        year === missing && continue
        if allowed_years !== nothing && !(Int(year) in allowed_years)
            continue
        end
        product_label = string(product_values[r])
        if allowed_categories !== nothing && !(lowercase(product_label) in allowed_categories)
            continue
        end
        value = _parse_number(normalized[r, value_col])
        value === missing && continue
        metric = string(metric_values[r])
        unit = unit_values[r] === missing ? missing : string(unit_values[r])
        push!(rows, (
            string(normalized[r, geo_col]),
            Int(year),
            product_label,
            metric,
            value,
            unit,
            _match_product(product_label, product_lookup),
            source_file,
            source_file,
            fetch_date,
        ))
    end

    return rows
end

function _extract_ump_sankey(
    csv_path::String,
    allowed_categories::Union{Nothing,Set{String}},
    allowed_years::Union{Nothing,Set{Int}}
)
    df = _read_ump_csv(csv_path)
    isempty(df) && return df

    normalized = _normalize_ump_csv_columns(df)
    scenario_col = _find_column(normalized, r"scenario")
    if scenario_col !== nothing
        keep = [_is_historical(value) for value in normalized[:, scenario_col]]
        normalized = normalized[keep, :]
    end

    geo_col = _find_column(normalized, r"location|geo|country|member|nation")
    year_col = _find_column(normalized, r"^year$")
    value_col = _find_column(normalized, r"^value$")
    unit_col = _find_column(normalized, r"unit")

    if geo_col === nothing || year_col === nothing || value_col === nothing
        @warn "UMP sankey CSV missing required columns" csv_path geo_col year_col value_col
        return DataFrame()
    end

    fetch_date = Dates.format(Dates.now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    source_file = basename(csv_path)
    rows = DataFrame(
        waste_stream=String[],
        location=String[],
        year=Int[],
        scenario=Union{String,Missing}[],
        additional_specification=Union{String,Missing}[],
        stock_flow_id=Union{String,Missing}[],
        layer_1=Union{String,Missing}[],
        layer_2=Union{String,Missing}[],
        layer_3=Union{String,Missing}[],
        layer_4=Union{String,Missing}[],
        value=Float64[],
        unit=Union{String,Missing}[],
        source_file=String[],
        fetch_date=String[],
    )

    waste_col = _find_column(normalized, r"waste_stream")
    additional_col = _find_column(normalized, r"additional_specification")
    stock_flow_col = _find_column(normalized, r"stock_flow_id")
    layer1_col = _find_column(normalized, r"layer_1|layer1")
    layer2_col = _find_column(normalized, r"layer_2|layer2")
    layer3_col = _find_column(normalized, r"layer_3|layer3")
    layer4_col = _find_column(normalized, r"layer_4|layer4")

    scenario_values = scenario_col === nothing ? fill(missing, nrow(normalized)) : normalized[:, scenario_col]
    unit_values = unit_col === nothing ? fill(missing, nrow(normalized)) : normalized[:, unit_col]

    for r in 1:nrow(normalized)
        year = _parse_number(normalized[r, year_col])
        year === missing && continue
        if allowed_years !== nothing && !(Int(year) in allowed_years)
            continue
        end
        if allowed_categories !== nothing && layer1_col !== nothing
            layer1_value = string(normalized[r, layer1_col])
            if !(lowercase(layer1_value) in allowed_categories)
                continue
            end
        end
        value = _parse_number(normalized[r, value_col])
        value === missing && continue

        push!(rows, (
            waste_col === nothing ? "WEEE" : string(normalized[r, waste_col]),
            string(normalized[r, geo_col]),
            Int(year),
            scenario_values[r] === missing ? missing : string(scenario_values[r]),
            additional_col === nothing ? missing : string(normalized[r, additional_col]),
            stock_flow_col === nothing ? missing : string(normalized[r, stock_flow_col]),
            layer1_col === nothing ? missing : string(normalized[r, layer1_col]),
            layer2_col === nothing ? missing : string(normalized[r, layer2_col]),
            layer3_col === nothing ? missing : string(normalized[r, layer3_col]),
            layer4_col === nothing ? missing : string(normalized[r, layer4_col]),
            value,
            unit_values[r] === missing ? missing : string(unit_values[r]),
            source_file,
            fetch_date,
        ))
    end

    return rows
end

function _normalize_columns(df::DataFrame)
    renamed = copy(df)
    new_names = Symbol[]
    for name in names(df)
        if name isa Symbol
            raw = String(name)
        else
            raw = string(name)
        end
        cleaned = lowercase(strip(raw))
        cleaned = replace(cleaned, r"\s+" => "_")
        push!(new_names, Symbol(cleaned))
    end
    rename!(renamed, Pair.(names(renamed), new_names))
    return renamed
end

function _parse_years_range(years_range::Union{Nothing,String})
    years_range === nothing && return nothing
    range_str = strip(years_range)
    isempty(range_str) && return nothing
    if occursin("-", range_str)
        parts = split(range_str, "-")
        length(parts) == 2 || return nothing
        start_year = parse(Int, strip(parts[1]))
        end_year = parse(Int, strip(parts[2]))
        return Set(start_year:end_year)
    end
    return Set([parse(Int, range_str)])
end

function _ump_categories_for_products(product_keys_filter)
    cfg = TOML.parsefile(PRODUCTS_CONFIG_PATH)
    products = get(cfg, "products", Dict{String,Any}())
    target_keys = product_keys_filter === nothing ? Set(keys(products)) : Set(string.(product_keys_filter))
    categories = Set{String}()
    for (key, pdata) in products
        if !(key in target_keys)
            continue
        end
        for code in get(pdata, "weee_waste_codes", String[])
            mapped = get(UMP_WEEE_CODE_MAP, code, nothing)
            mapped === nothing && continue
            push!(categories, lowercase(mapped))
        end
    end
    return isempty(categories) ? nothing : categories
end

function _find_column(df::DataFrame, pattern::Regex)
    for name in names(df)
        occursin(pattern, String(name)) && return name
    end
    return nothing
end

function _is_historical(value)
    val = lowercase(strip(string(value)))
    return occursin("hist", val) || occursin("baseline", val) || occursin("obs", val) || isempty(val)
end

function _parse_number(value)
    value isa Missing && return missing
    value isa Number && return float(value)
    parsed = tryparse(Float64, string(value))
    return parsed === nothing ? missing : parsed
end

function _build_weee_lookup()
    mappings = load_product_mappings()
    lookup = Dict{String,String}()
    for row in eachrow(mappings)
        codes_str = row.weee_waste_codes
        if ismissing(codes_str) || isempty(String(codes_str))
            continue
        end
        codes = split(String(codes_str), ",")
        for code in codes
            code_clean = strip(code)
            isempty(code_clean) && continue
            mapped = get(UMP_WEEE_CODE_MAP, code_clean, nothing)
            if mapped !== nothing
                lookup[lowercase(mapped)] = row.product
            end
            lookup[lowercase(code_clean)] = row.product
        end
    end
    return lookup
end

function _match_product(label, lookup::Dict{String,String})
    text = lowercase(strip(string(label)))
    for (code, product_key) in lookup
        occursin(lowercase(code), text) && return product_key
    end
    return missing
end

end # module UmpDataFetch
