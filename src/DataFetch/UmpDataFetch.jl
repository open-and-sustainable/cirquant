module UmpDataFetch

using DataFrames
using Dates
using HTTP
using XLSX
using ..DatabaseAccess: write_large_duckdb_table!
using ..AnalysisConfigLoader: load_product_mappings

export fetch_ump_weee_data, fetch_ump_battery_data

const DEFAULT_UMP_DOWNLOAD_URL = "https://www.urbanmineplatform.eu/download"

"""
    fetch_ump_weee_data(; db_path::String, download_url::String=DEFAULT_UMP_DOWNLOAD_URL,
                           dataset_path::Union{Nothing,String}=nothing,
                           max_tables::Int=typemax(Int))

Download the Urban Mine Platform (UMP) WEEE dataset, extract historical country-by-year
observations, and store them in a normalized `ump_weee_history` table in the raw database.

Only historical records are kept (rows whose scenario values include "hist"). Each record
includes the geography, year, product/category label, an inferred metric name, unit when
present, and the associated CirQuant product key when a WEEE code match is detected. Source
metadata (`source_file`, `source_sheet`, `fetch_date`) are attached to every row.
"""
function fetch_ump_weee_data(; db_path::String, download_url::String=DEFAULT_UMP_DOWNLOAD_URL,
                                dataset_path::Union{Nothing,String}=nothing,
                                max_tables::Int=typemax(Int))
    path = isnothing(dataset_path) ? _download_ump_file(download_url) : dataset_path
    workbooks = _collect_workbooks(path)
    isempty(workbooks) && error("No Excel workbooks found in the UMP download")

    product_lookup = _build_weee_lookup()
    results = DataFrame()
    written = 0
    for workbook_path in workbooks
        rows = _extract_workbook(workbook_path, product_lookup; max_tables=max_tables - written)
        isempty(rows) && continue
        results = isempty(results) ? rows : vcat(results, rows)
        written += length(unique(rows.source_sheet))
        written >= max_tables && break
    end

    if isempty(results)
        @warn "UMP download produced no historical rows"
        return false
    end

    write_large_duckdb_table!(results, db_path, "ump_weee_history")
    return true
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

    response = HTTP.request("GET", download_url; status_exception=false)
    response.status == 200 || error("Failed to download UMP dataset (status $(response.status))")

    open(tmp_path, "w") do io
        write(io, response.body)
    end

    return tmp_path
end

function _collect_workbooks(path::String)
    lower_path = lowercase(path)
    if endswith(lower_path, ".xlsx") || endswith(lower_path, ".xls")
        return [path]
    elseif endswith(lower_path, ".zip")
        return _extract_zip_workbooks(path)
    else
        @warn "Unrecognized file type for UMP download; assuming Excel workbook" path
        return [path]
    end
end

function _extract_zip_workbooks(zip_path::String)
    extract_dir = mktempdir()
    try
        run(pipeline(`unzip -o $zip_path -d $extract_dir`, stdout=devnull, stderr=devnull))
    catch e
        @error "Failed to unzip UMP archive" exception=e zip_path
        return String[]
    end

    workbooks = String[]
    for (root, _, files) in walkdir(extract_dir)
        for file in files
            lower_file = lowercase(file)
            if endswith(lower_file, ".xlsx") || endswith(lower_file, ".xls")
                push!(workbooks, joinpath(root, file))
            end
        end
    end

    isempty(workbooks) && @warn "No Excel workbooks found in UMP archive" zip_path
    return workbooks
end

function _extract_workbook(workbook_path::String, product_lookup::Dict{String,String}; max_tables::Int)
    all_rows = DataFrame()
    timestamp = Dates.format(Dates.now(), dateformat"yyyy-mm-ddTHH:MM:SS")

    XLSX.openxlsx(workbook_path) do xf
        sheet_names = collect(keys(xf))
        taken = 0
        for sheet_name in sheet_names
            taken >= max_tables && break
            rows = _extract_sheet(xf, sheet_name, basename(workbook_path), product_lookup, timestamp)
            isempty(rows) && continue
            all_rows = isempty(all_rows) ? rows : vcat(all_rows, rows)
            taken += 1
        end
    end

    return all_rows
end

function _extract_sheet(xf, sheet_name, source_file::String, product_lookup::Dict{String,String}, fetch_date::String)
    table = try
        XLSX.gettable(xf[sheet_name]; infer_eltypes=false)
    catch e
        @warn "Failed to read UMP sheet" source_file sheet_name exception=e
        return DataFrame()
    end

    df = DataFrame(table)
    isempty(df) && return df

    normalized = _normalize_columns(df)
    geo_col = _find_column(normalized, r"geo|country|member|nation")
    year_col = _find_column(normalized, r"^year$")
    scenario_col = _find_column(normalized, r"scenario")
    unit_col = _find_column(normalized, r"unit")
    product_col = _find_column(normalized, r"product|category|equipment|weee")

    geo_col === nothing && return DataFrame()

    if isnothing(year_col)
        year_cols = [name for name in names(normalized) if _is_year_column(name)]
        if isempty(year_cols)
            @warn "Skipping UMP sheet: no year columns" source_file sheet_name
            return DataFrame()
        end
        return _unpivot_wide_years(normalized, geo_col, product_col, year_cols, scenario_col, unit_col, sheet_name, source_file, product_lookup, fetch_date)
    else
        return _select_long_rows(normalized, geo_col, product_col, year_col, scenario_col, unit_col, sheet_name, source_file, product_lookup, fetch_date)
    end
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

function _find_column(df::DataFrame, pattern::Regex)
    for name in names(df)
        occursin(pattern, String(name)) && return name
    end
    return nothing
end

_is_year_column(name) = tryparse(Int, String(name)) !== nothing

function _is_historical(value)
    val = lowercase(strip(string(value)))
    return occursin("hist", val) || occursin("baseline", val) || isempty(val)
end

function _parse_number(value)
    value isa Missing && return missing
    value isa Number && return float(value)
    parsed = tryparse(Float64, string(value))
    return parsed === nothing ? missing : parsed
end

function _unpivot_wide_years(df::DataFrame, geo_col, product_col, year_cols, scenario_col, unit_col, sheet_name, source_file, product_lookup, fetch_date)
    rows = DataFrame(geo=String[], year=Int[], product_label=String[], metric=String[], value=Float64[], unit=Union{String,Missing}[], product_key=Union{String,Missing}[], source_sheet=String[], source_file=String[], fetch_date=String[])
    unit_values = unit_col === nothing ? nothing : df[:, unit_col]
    product_values = product_col === nothing ? fill(sheet_name, nrow(df)) : df[:, product_col]
    scenario_values = scenario_col === nothing ? fill("", nrow(df)) : df[:, scenario_col]

    for r in 1:nrow(df)
        _is_historical(scenario_values[r]) || continue
        for year_name in year_cols
            year_int = parse(Int, String(year_name))
            raw_value = df[r, year_name]
            parsed = _parse_number(raw_value)
            parsed === missing && continue
            push!(rows, (
                string(df[r, geo_col]),
                year_int,
                string(product_values[r]),
                sheet_name,
                parsed,
                unit_values === nothing ? missing : string(unit_values[r]),
                _match_product(product_values[r], product_lookup),
                sheet_name,
                source_file,
                fetch_date,
            ))
        end
    end

    return rows
end

function _select_long_rows(df::DataFrame, geo_col, product_col, year_col, scenario_col, unit_col, sheet_name, source_file, product_lookup, fetch_date)
    candidates = [name for name in names(df) if !(name in (geo_col, product_col, year_col, scenario_col, unit_col))]
    value_col = findfirst(col -> begin
        parsed = _parse_number.(df[!, col])
        return eltype(df[!, col]) <: Number || any(!ismissing, parsed)
    end, candidates)
    value_col === nothing && return DataFrame()

    rows = DataFrame(geo=String[], year=Int[], product_label=String[], metric=String[], value=Float64[], unit=Union{String,Missing}[], product_key=Union{String,Missing}[], source_sheet=String[], source_file=String[], fetch_date=String[])
    scenario_values = scenario_col === nothing ? fill("", nrow(df)) : df[:, scenario_col]
    unit_values = unit_col === nothing ? fill(missing, nrow(df)) : string.(df[:, unit_col])
    product_values = product_col === nothing ? fill(sheet_name, nrow(df)) : df[:, product_col]

    for r in 1:nrow(df)
        _is_historical(scenario_values[r]) || continue
        year = _parse_number(df[r, year_col])
        year === missing && continue
        value = _parse_number(df[r, value_col])
        value === missing && continue
        push!(rows, (
            string(df[r, geo_col]),
            Int(year),
            string(product_values[r]),
            String(value_col),
            value,
            unit_values[r],
            _match_product(product_values[r], product_lookup),
            sheet_name,
            source_file,
            fetch_date,
        ))
    end

    return rows
end

function _build_weee_lookup()
    mappings = load_product_mappings()
    lookup = Dict{String,String}()
    for row in eachrow(mappings)
        codes = row.weee_waste_codes
        for code in codes
            lookup[lowercase(code)] = row.product
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
