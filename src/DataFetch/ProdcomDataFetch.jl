module ProdcomDataFetch

using DataFrames, Dates, DuckDB, CSV, ComextAPI, ProdcomAPI
using ..DatabaseAccess: write_large_duckdb_table!, recreate_duckdb_database
using ..AnalysisConfigLoader: prodcom_codes_for_year
using ..FetchUtils: RateLimiter, throttle!, run_bounded_tasks

export fetch_prodcom_data

"""
    fetch_prodcom_data(years_range="1995-2023", custom_datasets=nothing; db_path::String)

Fetches PRODCOM data from Eurostat API for specified datasets and year range.
Data is saved to DuckDB tables in the raw database.

# Arguments
- `years_range::String`: Year range to fetch (default: "1995-2023")
- `custom_datasets`: Optional custom datasets to fetch (default: ["ds-056120"])
- `db_path::String`: Path to the raw DuckDB database (required keyword argument)
- `parallel_years`: Run per-year fetches with bounded concurrency (default: false)
- `max_parallel_years`: Maximum concurrent year workers when `parallel_years` is true (default: 2)
- `rate_limit_seconds` / `rate_limit_jitter`: Shared throttle between workers to avoid API bursts
- `product_keys_filter`: Optional vector of product keys (as in `products.<key>`) to limit fetch scope
"""
function fetch_prodcom_data(
    years_range="1995-2023",
    custom_datasets=nothing;
    db_path::String,
    parallel_years::Bool=false,
    max_parallel_years::Int=2,
    rate_limit_seconds::Float64=0.6,
    rate_limit_jitter::Float64=0.2,
    product_keys_filter=nothing
)
    # Parse years
    years = split(years_range, "-")
    if length(years) == 1
        # Single year provided
        start_year = parse(Int, years[1])
        end_year = start_year
    elseif length(years) == 2
        # Year range provided
        start_year = parse(Int, years[1])
        end_year = parse(Int, years[2])
    else
        error("Invalid years format. Use either 'YYYY' for a single year or 'YYYY-YYYY' for a range.")
    end

    years_to_fetch = collect(start_year:end_year)

    # Datasets to fetch
    datasets = isnothing(custom_datasets) ? ["ds-059358"] : custom_datasets

    # Define indicators to fetch per dataset
    dataset_indicators = Dict(
        "ds-059358" => ["PRODVAL", "PRODQNT", "EXPVAL", "EXPQNT", "IMPVAL", "IMPQNT", "QNTUNIT"],
        "ds-059359" => ["PRODQNT", "QNTUNIT"]
    )
    dataset_sources = Dict(
        "ds-059358" => [:api],
        "ds-059359" => [:api]
    )
    default_indicators = ["PRODVAL", "PRODQNT", "EXPVAL", "EXPQNT", "IMPVAL", "IMPQNT"]
    default_sources = [:bulk, :api]

    @info "Using database path: $db_path"

    # Track success/failure statistics
    stats = Dict(
        :total_queries => 0,
        :successful => 0,
        :failed => 0,
        :rows_processed => 0
    )

    # Ensure the database directory exists
    db_dir = dirname(db_path)
    if !isdir(db_dir)
        @info "Creating database directory: $db_dir"
        mkpath(db_dir)
    end

    # Create a log directory for error dumps
    log_dir = abspath(joinpath(dirname(dirname(dirname(@__FILE__))), "CirQuant-database/logs"))
    if !isdir(log_dir)
        @info "Creating log directory: $log_dir"
        mkpath(log_dir)
    end

    # Check if database exists and try to validate it
    if isfile(db_path)
        @info "Checking database integrity: $db_path"
        db_valid = try
            con = DuckDB.DB(db_path)
            # Test query to check if database is accessible
            DuckDB.query(con, "SELECT 1")
            DBInterface.close!(con)
            true
        catch e
            @warn "Database appears to be corrupted or inaccessible" exception = e
            false
        end

        # If database seems corrupted, log a warning but continue without recreation
        # to avoid losing existing data
        if !db_valid
            @warn "Database appears to have issues, but will continue without recreation to preserve existing tables"
            @info "New data will be added to backup CSV files if database writes fail"
            # Note: We intentionally avoid recreating the database to prevent data loss
        end
    end

    # Shared rate limiter across all tasks/datasets
    rate_limiter = rate_limit_seconds > 0 ? RateLimiter(rate_limit_seconds; jitter=rate_limit_jitter) : nothing

    for dataset in datasets
        @info "Processing dataset: $dataset"
        indicators = get(dataset_indicators, dataset, default_indicators)
        sources = get(dataset_sources, dataset, default_sources)

        process_year = function (year)
            year_stats = (successful=0, failed=1, rows_processed=0)
            try
                code_info = prodcom_codes_for_year(year; products_filter=product_keys_filter)
                prodcom_code_set = Set(code_info.codes_clean)
                prodcom_code_map = code_info.clean_to_original

                if isempty(prodcom_code_set)
                    @warn "No PRODCOM codes defined for year $year; skipping"
                    return year_stats
                end

                epoch_range = "$(code_info.epoch_info.start_year)-$(code_info.epoch_info.end_year)"
                @info "Using PRODCOM nomenclature epoch '$(code_info.epoch_info.label)' ($epoch_range)"
                @info "Focusing on $(length(prodcom_code_set)) PRODCOM codes for year $year"

                combined_df = DataFrame()
                @info "Fetching $dataset for year $year with indicators: $indicators (sources: $sources, products=$(length(prodcom_code_set)))"

                unsupported_dataset = false

                for indicator in indicators
                    indicator_combined = DataFrame()
                    for code in prodcom_code_set
                        code_df = nothing
                        source_used = nothing

                        for source in sources
                            if rate_limiter !== nothing
                                throttle!(rate_limiter)
                            end

                            try
                                code_df = ProdcomAPI.fetch_prodcom_data(
                                    dataset, year, indicator;
                                    verbose=false, source=source, prccode=code
                                )
                                source_used = source
                                break
                            catch e
                                @warn "Failed to fetch indicator $indicator for $dataset, year $year, code $code with source $source" exception = e
                            end
                        end

                        if isnothing(code_df) || nrow(code_df) == 0
                            @info "No data returned for $dataset, year $year, indicator $indicator, code $code after trying sources $sources"
                            continue
                        end
                        code_column = if :prccode in propertynames(code_df)
                            :prccode
                        elseif :product in propertynames(code_df)
                            :product
                        else
                            nothing
                        end

                        if code_column === nothing
                            @warn "No product/prccode column in response for $dataset, year $year, indicator $indicator, code $code; skipping"
                            if dataset in ["ds-059358", "ds-059359"]
                                unsupported_dataset = true
                                break
                            end
                            continue
                        end
                        try
                            code_df[!, :prccode] = string.(code_df[!, code_column])
                            mask = code_df.prccode .== code
                            filtered_df = code_df[mask, :]

                            if nrow(filtered_df) == 0
                                @info "No matching rows after filtering for code $code on indicator $indicator"
                                continue
                            end

                            filtered_df[!, :prodcom_code_original] = [prodcom_code_map[c] for c in filtered_df.prccode]

                            if :value in propertynames(filtered_df)
                                filtered_df[!, :value] = string.(filtered_df.value)
                            end

                            indicator_combined = nrow(indicator_combined) == 0 ? filtered_df : vcat(indicator_combined, filtered_df, cols=:union)
                        catch e
                            @warn "Failed to process indicator $indicator for $dataset, year $year, code $code (source used: $source_used)" exception = e
                        end

                        if unsupported_dataset
                            break
                        end
                    end

                    if unsupported_dataset
                        break
                    end

                    if nrow(indicator_combined) == 0
                        @warn "No data received for $dataset, year $year, indicator $indicator across requested codes"
                        continue
                    end

                    combined_df = nrow(combined_df) == 0 ? indicator_combined : vcat(combined_df, indicator_combined, cols=:union)
                end

                if unsupported_dataset
                    @error "$dataset appears unsupported by current ProdcomAPI (missing data/prccode); skipping year $year"
                    return year_stats
                end

                if nrow(combined_df) == 0
                    @warn "No data received for $dataset, year $year"
                    return year_stats
                end

                @info "Received $(nrow(combined_df)) rows of data"

                table_name = "prodcom_$(replace(dataset, "-" => "_"))_$year"
                @info "Writing $(nrow(combined_df)) rows to database"

                success = false
                try
                    _db_valid = try
                        con = DuckDB.DB(db_path)
                        DuckDB.query(con, "SELECT 1")
                        DBInterface.close!(con)
                        true
                    catch db_check_error
                        @warn "Database seems inaccessible, but will continue without recreation" exception = db_check_error
                        @info "Will attempt to write data with fallback to CSV backup if needed"
                        false
                    end

                    success = try
                        write_large_duckdb_table!(combined_df, db_path, table_name)
                    catch db_error
                        @error "Failed to write to DuckDB through main function" exception = db_error table = table_name
                        false
                    end

                    if success === true
                        @info "✓ Successfully saved data to table $table_name"
                        year_stats = (successful=1, failed=0, rows_processed=nrow(combined_df))
                    else
                        @warn "Automatic methods failed, saving to backup CSV"
                        backup_dir = joinpath(log_dir, "backups")
                        mkpath(backup_dir)
                        backup_file = joinpath(backup_dir, "backup_$(dataset)_$(year)_$(round(Int, time())).csv")

                        try
                            @info "Saving emergency backup to $backup_file"
                            CSV.write(backup_file, combined_df)
                            @info "✓ Successfully saved backup data to $backup_file"
                            @info "Data can be manually imported later with: COPY \"$(table_name)\" FROM '$(backup_file)' (FORMAT CSV, HEADER);"
                        catch csv_err
                            @warn "Failed to save complete backup data" exception = csv_err

                            sample_file = joinpath(log_dir, "sample_$(dataset)_$(year)_$(round(Int, time())).csv")
                            try
                                CSV.write(sample_file, combined_df[1:min(1000, nrow(combined_df)), :])
                                @info "Saved sample of problematic data to $sample_file"
                            catch sample_err
                                @error "All backup methods failed" exception = sample_err
                            end
                        end
                    end
                catch outer_err
                    @error "Unexpected error in database writing process" exception = outer_err
                end

                return year_stats
            catch e
                @error "Error processing $dataset for year $year" exception = e
                @info "Continuing with next year/dataset to avoid losing progress"

                error_log = joinpath(log_dir, "error_$(dataset)_$(year)_$(round(Int, time())).txt")
                try
                    error_type = string(typeof(e))
                    error_msg = try
                        sprint(showerror, e)
                    catch
                        "Error message could not be extracted"
                    end

                    open(error_log, "w") do f
                        println(f, "Error processing $dataset for year $year")
                        println(f, "Dataset: $dataset")
                        println(f, "Year: $year")
                        println(f, "Exception Type: $error_type")
                        println(f, "Exception Message: $error_msg")
                        println(f, "Stacktrace: Available in Julia logs")
                    end
                    @info "Error details saved to $error_log"
                catch log_err
                    @warn "Failed to save error log" exception = log_err
                end

                return year_stats
            end
        end

        year_results = if parallel_years
            run_bounded_tasks(years_to_fetch; max_concurrency=max_parallel_years, task_fn=process_year)
        else
            [process_year(y) for y in years_to_fetch]
        end

        stats[:total_queries] += length(years_to_fetch)

        for result in year_results
            stats[:successful] += result.successful
            stats[:failed] += result.failed
            stats[:rows_processed] += result.rows_processed
        end
    end

    # Report final statistics
    success_rate = stats[:total_queries] > 0 ? round(stats[:successful]/stats[:total_queries]*100, digits=1) : 0.0
    @info "PRODCOM data fetching completed:"
    @info "  Total year attempts: $(stats[:total_queries])"
    @info "  Successfully processed: $(stats[:successful]) ($(success_rate)%)"
    @info "  Failed: $(stats[:failed])"
    @info "  Total rows processed: $(stats[:rows_processed])"

    return stats
end

end # module
