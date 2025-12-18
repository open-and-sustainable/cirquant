module ProdcomDataFetch

using DataFrames, Dates, DuckDB, CSV, ComextAPI, ProdcomAPI
using ..DatabaseAccess: write_large_duckdb_table!, recreate_duckdb_database
using ..AnalysisConfigLoader: prodcom_codes_for_year

export fetch_prodcom_data

"""
    fetch_prodcom_data(years_range="1995-2023", custom_datasets=nothing; db_path::String)

Fetches PRODCOM data from Eurostat API for specified datasets and year range.
Data is saved to DuckDB tables in the raw database.

# Arguments
- `years_range::String`: Year range to fetch (default: "1995-2023")
- `custom_datasets`: Optional custom datasets to fetch (default: ["ds-056120"])
- `db_path::String`: Path to the raw DuckDB database (required keyword argument)
"""
function fetch_prodcom_data(years_range="1995-2023", custom_datasets=nothing; db_path::String)
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

    # Datasets to fetch
    datasets = isnothing(custom_datasets) ? ["ds-059358"] : custom_datasets

    # Define indicators to fetch per dataset
    dataset_indicators = Dict(
        "ds-059358" => ["PRODVAL", "PRODQNT", "EXPVAL", "EXPQNT", "IMPVAL", "IMPQNT", "QNTUNIT"],
        "ds-059359" => ["PRODQNT", "QNTUNIT"]
    )
    default_indicators = ["PRODVAL", "PRODQNT", "EXPVAL", "EXPQNT", "IMPVAL", "IMPQNT"]

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

    # Process each dataset and year
    current_epoch_key = nothing

    for dataset in datasets
        @info "Processing dataset: $dataset"
        indicators = get(dataset_indicators, dataset, default_indicators)

        for year in start_year:end_year
            @info "Fetching data for year: $year"
            code_info = prodcom_codes_for_year(year)
            prodcom_code_set = Set(code_info.codes_clean)
            prodcom_code_map = code_info.clean_to_original

            if isempty(prodcom_code_set)
                @warn "No PRODCOM codes defined for year $year; skipping"
                continue
            end

            if current_epoch_key != code_info.epoch_key
                current_epoch_key = code_info.epoch_key
                epoch_range = "$(code_info.epoch_info.start_year)-$(code_info.epoch_info.end_year)"
                @info "Using PRODCOM nomenclature epoch '$(code_info.epoch_info.label)' ($epoch_range)"
            end

            @info "Focusing on $(length(prodcom_code_set)) PRODCOM codes for year $year"
            stats[:total_queries] += 1

            try
                # Use ProdcomAPI with indicator filters
                # Initialize combined_df outside to ensure proper scope
                combined_df = DataFrame()
                @info "Fetching $dataset for year $year with indicators: $indicators (bulk source)"

                for indicator in indicators
                    try
                        indicator_df = ProdcomAPI.fetch_prodcom_data(dataset, year, indicator;
                                                                    verbose=false, source=:bulk)

                        if isnothing(indicator_df) || nrow(indicator_df) == 0
                            @warn "No data returned for $dataset, year $year, indicator $indicator"
                            continue
                        end

                        indicator_df[!, :prccode] = string.(indicator_df.prccode)
                        mask = in.(indicator_df.prccode, Ref(prodcom_code_set))
                        filtered_df = indicator_df[mask, :]

                        if nrow(filtered_df) == 0
                            @info "No matching PRODCOM codes found for indicator $indicator"
                            continue
                        end

                        filtered_df[!, :prodcom_code_original] = [prodcom_code_map[code] for code in filtered_df.prccode]

                        if :value in propertynames(filtered_df)
                            filtered_df[!, :value] = string.(filtered_df.value)
                        end

                        if nrow(combined_df) == 0
                            combined_df = filtered_df
                        else
                            combined_df = vcat(combined_df, filtered_df, cols=:union)
                        end
                    catch e
                        @warn "Failed to fetch indicator $indicator for $dataset, year $year" exception = e
                    end
                end

                # Assign the combined data to df
                df = combined_df

                # Check if data was received
                if isnothing(df) || nrow(df) == 0
                    @warn "No data received for $dataset, year $year"
                    stats[:failed] += 1
                    continue
                end

                @info "Received $(nrow(df)) rows of data"

                # Save to DuckDB
                table_name = "prodcom_$(replace(dataset, "-" => "_"))_$year"
                if !isempty(df)
                    # Record row count for statistics
                    stats[:rows_processed] += nrow(df)

                    # Write to database
                    @info "Writing $(nrow(df)) rows to database"

                    # Try writing with error handling
                    success = false
                    try
                        # Check if the database appears to be valid
                        db_valid = try
                            con = DuckDB.DB(db_path)
                            DuckDB.query(con, "SELECT 1")
                            DBInterface.close!(con)
                            true
                        catch db_check_error
                            @warn "Database seems inaccessible, but will continue without recreation" exception = db_check_error
                            @info "Will attempt to write data with fallback to CSV backup if needed"
                            false
                        end

                        # We intentionally do NOT recreate the database here to preserve existing tables

                        # Handle both possible outcomes (success or error)
                        success = try
                            # Try to write to DuckDB but catch any error
                            write_large_duckdb_table!(df, db_path, table_name)
                        catch db_error
                            @error "Failed to write to DuckDB through main function" exception = db_error table = table_name
                            # Signal failure but don't propagate error
                            false
                        end

                        if success === true  # Explicit comparison to ensure boolean true
                            @info "✓ Successfully saved data to table $table_name"
                            stats[:successful] += 1
                        else
                            # Manual fallback if automatic ones didn't work
                            @warn "Automatic methods failed, saving to backup CSV"
                            stats[:failed] += 1

                            # Save problematic dataframe for inspection and backup
                            backup_dir = joinpath(log_dir, "backups")
                            mkpath(backup_dir)
                            backup_file = joinpath(backup_dir, "backup_$(dataset)_$(year)_$(round(Int, time())).csv")

                            try
                                # Save complete dataset as backup
                                @info "Saving emergency backup to $backup_file"
                                CSV.write(backup_file, df)
                                @info "✓ Successfully saved backup data to $backup_file"
                                @info "Data can be manually imported later with: COPY \"$(table_name)\" FROM '$(backup_file)' (FORMAT CSV, HEADER);"
                            catch csv_err
                                @warn "Failed to save complete backup data" exception = csv_err

                                # Last resort: try to save a small sample
                                sample_file = joinpath(log_dir, "sample_$(dataset)_$(year)_$(round(Int, time())).csv")
                                try
                                    CSV.write(sample_file, df[1:min(1000, nrow(df)), :])
                                    @info "Saved sample of problematic data to $sample_file"
                                catch sample_err
                                    @error "All backup methods failed" exception = sample_err
                                end
                            end
                        end
                    catch outer_err
                        # Catch any unexpected errors to prevent the entire process from failing
                        @error "Unexpected error in database writing process" exception = outer_err
                        stats[:failed] += 1
                    end
                else
                    @warn "No data to save for $dataset, year $year"
                    stats[:failed] += 1
                end
            catch e
                @error "Error processing $dataset for year $year" exception = e
                @info "Continuing with next year/dataset to avoid losing progress"
                stats[:failed] += 1

                # Create an error log with details - use safer approach to avoid closure serialization issues
                error_log = joinpath(log_dir, "error_$(dataset)_$(year)_$(round(Int, time())).txt")
                try
                    # Extract basic info before opening file to avoid closure issues
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
            end
        end
    end

    # Report final statistics
    success_rate = stats[:total_queries] > 0 ? round(stats[:successful]/stats[:total_queries]*100, digits=1) : 0.0
    @info "PRODCOM data fetching completed:"
    @info "  Total datasets: $(stats[:total_queries])"
    @info "  Successfully processed: $(stats[:successful]) ($(success_rate)%)"
    @info "  Failed: $(stats[:failed])"
    @info "  Total rows processed: $(stats[:rows_processed])"

    return stats
end

end # module
