module ProdcomDataFetch

using DataFrames, Dates, DuckDB, CSV, ComextAPI, ProdcomAPI
using ..DatabaseAccess: write_large_duckdb_table!, recreate_duckdb_database
using ..ProductConversionTables: get_product_mapping_data

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
    datasets = isnothing(custom_datasets) ? ["ds-056120"] : custom_datasets

    # Define indicators to fetch per dataset
    dataset_indicators = Dict(
        "ds-056120" => ["PRODVAL", "PRODQNT", "EXPVAL", "EXPQNT", "IMPVAL", "IMPQNT", "QNTUNIT"],
        "ds-056121" => ["PRODQNT", "QNTUNIT"]
    )

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
    for dataset in datasets
        @info "Processing dataset: $dataset"

        for year in start_year:end_year
            @info "Fetching data for year: $year"

            try
                # Use ProdcomAPI with indicator filters
                # Initialize combined_df outside to ensure proper scope
                combined_df = DataFrame()

                if haskey(dataset_indicators, dataset)
                    indicators = dataset_indicators[dataset]
                    @info "Fetching $dataset for year $year with indicators: $indicators"

                    # Get PRODCOM codes from ProductConversionTables
                    product_mapping = get_product_mapping_data()
                    prodcom_codes = unique(product_mapping.prodcom_code)

                    # Remove dots from PRODCOM codes for API submission
                    prodcom_codes_no_dots = [replace(code, "." => "") for code in prodcom_codes]

                    @info "Focusing on $(length(prodcom_codes_no_dots)) PRODCOM codes from sectors of interest"

                    # Fetch each indicator and PRODCOM code combination
                    for indicator in indicators
                        for (idx, prccode) in enumerate(prodcom_codes_no_dots)
                            @info "Fetching $dataset for year $year, indicator: $indicator, PRODCOM: $prccode ($(prodcom_codes[idx]))"
                            stats[:total_queries] += 1
                            indicator_df = ProdcomAPI.fetch_prodcom_data(dataset, year, indicator; prccode=prccode, verbose=false)

                            # Add delay to avoid rate limiting
                            sleep(5)  # 5 seconds delay between API calls

                            if !isnothing(indicator_df) && nrow(indicator_df) > 0
                                # Add original PRODCOM code with dots for reference
                                indicator_df[!, :prodcom_code_original] .= prodcom_codes[idx]

                                # Convert value column to string to accommodate both numeric and QNTUNIT string data
                                if hasproperty(indicator_df, :value)
                                    indicator_df[!, :value] = string.(indicator_df.value)
                                end

                                if nrow(combined_df) == 0
                                    combined_df = indicator_df
                                else
                                    # Combine dataframes, avoiding duplicates
                                    combined_df = vcat(combined_df, indicator_df, cols=:union)
                                end
                            end
                        end
                    end
                else
                    # Get PRODCOM codes from ProductConversionTables
                    product_mapping = get_product_mapping_data()
                    prodcom_codes = unique(product_mapping.prodcom_code)

                    # Remove dots from PRODCOM codes for API submission
                    prodcom_codes_no_dots = [replace(code, "." => "") for code in prodcom_codes]

                    @info "Focusing on $(length(prodcom_codes_no_dots)) PRODCOM codes from sectors of interest"

                    # Fetch data for each PRODCOM code
                    for (idx, prccode) in enumerate(prodcom_codes_no_dots)
                        @info "Fetching $dataset for year $year, PRODCOM: $prccode ($(prodcom_codes[idx]))"
                        # When no specific indicators are requested, we fetch all indicators for the PRODCOM code
                        # We'll use the standard indicators that are typically available
                        for indicator in ["PRODVAL", "PRODQNT", "EXPVAL", "EXPQNT", "IMPVAL", "IMPQNT"]
                            try
                                indicator_df = ProdcomAPI.fetch_prodcom_data(dataset, year, indicator; prccode=prccode, verbose=false)

                                # Add delay to avoid rate limiting
                                sleep(5)  # 5 seconds delay between API calls

                                if !isnothing(indicator_df) && nrow(indicator_df) > 0
                                    # Add original PRODCOM code with dots for reference
                                    indicator_df[!, :prodcom_code_original] .= prodcom_codes[idx]

                                    # Convert value column to string to accommodate both numeric and QNTUNIT string data
                                    if hasproperty(indicator_df, :value)
                                        indicator_df[!, :value] = string.(indicator_df.value)
                                    end

                                    if nrow(combined_df) == 0
                                        combined_df = indicator_df
                                    else
                                        # Combine dataframes, avoiding duplicates
                                        combined_df = vcat(combined_df, indicator_df, cols=:union)
                                    end
                                end
                            catch e
                                # Some indicators might not be available for all datasets
                                @debug "Indicator $indicator not available for $dataset: $e"
                                stats[:failed] += 1

                                # Add delay even on failure to avoid rate limiting
                                sleep(10)  # 10 seconds delay after failed requests
                            end
                        end
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

                # Add delay even on failure to avoid rate limiting
                sleep(10)  # 10 seconds delay after failed requests

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
    @info "PRODCOM data fetching completed:"
    @info "  Total datasets: $(stats[:total_queries])"
    @info "  Successfully processed: $(stats[:successful]) ($(round(stats[:successful]/stats[:total_queries]*100, digits=1))%)"
    @info "  Failed: $(stats[:failed])"
    @info "  Total rows processed: $(stats[:rows_processed])"

    return stats
end

end # module
