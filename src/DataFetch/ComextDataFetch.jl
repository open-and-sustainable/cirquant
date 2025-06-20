module ComextDataFetch

using HTTP, JSON3, DataFrames, Dates, DuckDB, CSV, ComextAPI
using ..DatabaseAccess: write_large_duckdb_table!, recreate_duckdb_database

export fetch_comext_data

"""
    fetch_comext_data(years_range="1995-2023")

Fetches COMEXT data from Eurostat API for specified datasets and year range.
Data is saved to DuckDB tables in the raw database.
"""
function fetch_comext_data(years_range="1995-2023", custom_datasets=nothing)
    # Parse years
    years = split(years_range, "-")
    start_year = parse(Int, years[1])
    end_year = parse(Int, years[2])

    # Get available datasets if none specified
    datasets = if isnothing(custom_datasets)
        try
            available_datasets = ComextAPI.get_available_datasets()
            if nrow(available_datasets) > 0
                # Filter out datasets that are known to have API issues
                valid_datasets = filter(row -> !in(row.dataset_id, ["DS-045409"]), available_datasets)
                if nrow(valid_datasets) > 0
                    @info "Found $(nrow(valid_datasets)) potentially working datasets"
                    valid_datasets.dataset_id
                else
                    @warn "All available datasets appear to have known issues"
                    @info "Trying alternative Comext datasets from bulk download patterns"
                    # Try some alternative dataset patterns that might work
                    ["DS-016890", "DS-018995", "DS-041688"]  # Alternative trade datasets
                end
            else
                @warn "No datasets found from ComextAPI, trying alternative approaches"
                # Try some common Comext dataset patterns
                ["DS-016890", "DS-018995", "DS-041688"]
            end
        catch e
            @warn "Failed to get available datasets from ComextAPI" exception = e
            @info "Trying fallback dataset patterns commonly used for trade data"
            # Common Eurostat trade dataset patterns
            ["DS-016890", "DS-018995", "DS-041688", "DS-056125"]
        end
    else
        custom_datasets
    end

    @info "Found $(length(datasets)) datasets to process"

    # Database path - use absolute path for reliability
    db_path = abspath(joinpath(dirname(dirname(dirname(@__FILE__))), "CirQuant-database/raw/CirQuant_1995-2023.duckdb"))
    @info "Using database path: $db_path"

    # Track success/failure statistics
    stats = Dict(
        :total_datasets => length(datasets) * (end_year - start_year + 1),
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
        end
    end

    # Process each dataset and year
    for dataset in datasets
        @info "Processing Comext dataset: $dataset"

        # Check what years are available for this dataset
        available_years = try
            years = ComextAPI.get_dataset_years(dataset)
            @info "Dataset $dataset has data for years: $(length(years)) years available"
            years
        catch e
            @warn "Failed to get available years for dataset $dataset, will try all requested years" exception = e
            nothing  # Will try all years
        end

        for year in start_year:end_year
            # Skip if year is not available for this dataset (only if we got a valid year list)
            if !isnothing(available_years) && isa(available_years, AbstractArray) && !(year in available_years)
                @info "Year $year not available for dataset $dataset (available: $(first(available_years, 3))...), skipping"
                stats[:failed] += 1
                continue
            end

            @info "Fetching Comext data for dataset $dataset, year: $year"

            try
                # Fetch data using ComextAPI with additional error context
                @info "Attempting to fetch dataset $dataset for year $year..."
                start_time = time()
                
                df = try
                    ComextAPI.fetch_comext_dataset(dataset, year)
                catch fetch_error
                    # Provide helpful information about the error
                    if isa(fetch_error, HTTP.Exceptions.StatusError)
                        status = fetch_error.status
                        if status == 404
                            @warn "Dataset $dataset not found for year $year (HTTP 404)"
                            @info "This might mean:"
                            @info "  - Dataset ID is incorrect"
                            @info "  - Year $year is not available for this dataset"
                            @info "  - Dataset has been renamed or discontinued"
                        elseif status == 500
                            @warn "Server error for dataset $dataset year $year (HTTP 500)"
                            @info "This is likely a temporary Eurostat API issue"
                        else
                            @warn "HTTP error $status for dataset $dataset year $year"
                        end
                    end
                    rethrow(fetch_error)
                end
                
                processing_time = round(time() - start_time, digits=2)
                @info "Successfully fetched $(nrow(df)) rows in $processing_time seconds"

                # Add metadata columns to match Prodcom structure
                df[!, :dataset] = dataset
                df[!, :year] = year
                df[!, :fetch_date] = now()
                df[!, :data_source] = "Eurostat COMEXT API"

                # Clean and process the dataframe for DuckDB compatibility
                df = clean_comext_dataframe(df, dataset, year)

                # Save to DuckDB
                table_name = "comext_$(replace(dataset, "-" => "_"))_$year"
                if !isempty(df)
                    # Record row count for statistics
                    stats[:rows_processed] += nrow(df)

                    # Write to database
                    @info "Writing $(nrow(df)) rows to database (processed in $processing_time seconds)"

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
                            @info "✓ Successfully saved Comext data to table $table_name"
                            stats[:successful] += 1
                        else
                            # Manual fallback if automatic ones didn't work
                            @warn "Automatic methods failed, saving to backup CSV"
                            stats[:failed] += 1

                            # Save problematic dataframe for inspection and backup
                            backup_dir = joinpath(log_dir, "backups")
                            mkpath(backup_dir)
                            backup_file = joinpath(backup_dir, "backup_comext_$(dataset)_$(year)_$(round(Int, time())).csv")

                            try
                                # Save complete dataset as backup
                                @info "Saving emergency backup to $backup_file"
                                CSV.write(backup_file, df)
                                @info "✓ Successfully saved backup data to $backup_file"
                                @info "Data can be manually imported later with: COPY \"$(table_name)\" FROM '$(backup_file)' (FORMAT CSV, HEADER);"
                            catch csv_err
                                @warn "Failed to save complete backup data" exception = csv_err

                                # Last resort: try to save a small sample
                                sample_file = joinpath(log_dir, "sample_comext_$(dataset)_$(year)_$(round(Int, time())).csv")
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
                    @warn "No data to save for Comext dataset $dataset, year $year"
                    stats[:failed] += 1
                end
            catch e
                @error "Error processing Comext dataset $dataset for year $year" exception = e
                @info "Continuing with next year/dataset to avoid losing progress"
                stats[:failed] += 1

                # Create an error log with details and suggestions
                error_log = joinpath(log_dir, "error_comext_$(dataset)_$(year)_$(round(Int, time())).txt")
                try
                    # Extract basic info before opening file to avoid closure issues
                    error_type = string(typeof(e))
                    error_msg = try
                        sprint(showerror, e)
                    catch
                        "Error message could not be extracted"
                    end
                    
                    open(error_log, "w") do f
                        println(f, "Error processing Comext dataset $dataset for year $year")
                        println(f, "Exception Type: $error_type")
                        println(f, "Exception Message: $error_msg")
                        println(f, "")
                        println(f, "Troubleshooting suggestions:")
                        println(f, "1. Check if dataset $dataset exists in Eurostat database")
                        println(f, "2. Verify if year $year is available for this dataset")
                        println(f, "3. Try using bulk download instead: https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=comext")
                        println(f, "4. Check Eurostat API status: https://ec.europa.eu/eurostat/web/json-and-unicode-web-services")
                        println(f, "5. Consider using alternative dataset IDs")
                        println(f, "")
                        println(f, "Common working Comext datasets to try:")
                        println(f, "- Monthly trade data: Use bulk download CSV files")
                        println(f, "- Annual aggregated data: Check DS-016890, DS-018995")
                        println(f, "")
                        println(f, "Stacktrace: Available in Julia logs")
                    end
                    @info "Error details and troubleshooting saved to $error_log"
                catch log_err
                    @warn "Failed to save error log" exception = log_err
                end
            end
        end
    end

    # Report final statistics with recommendations
    @info "COMEXT data fetching completed:"
    @info "  Total datasets: $(stats[:total_datasets])"
    @info "  Successfully processed: $(stats[:successful]) ($(round(stats[:successful]/stats[:total_datasets]*100, digits=1))%)"
    @info "  Failed: $(stats[:failed])"
    @info "  Total rows processed: $(stats[:rows_processed])"
    
    if stats[:successful] == 0
        @warn "No COMEXT data was successfully fetched!"
        @info "Recommendations:"
        @info "1. The ComextAPI package may have dataset compatibility issues"
        @info "2. Consider using Eurostat bulk download for Comext data:"
        @info "   https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=comext"
        @info "3. Check error logs in: $(log_dir)"
        @info "4. Try with different dataset IDs or years"
        @info "5. Verify internet connectivity and Eurostat API availability"
    end

    return stats
end

"""
    clean_comext_dataframe(df, dataset, year)

Clean and prepare the Comext dataframe for DuckDB storage.
Handles data type conversions and ensures compatibility.
"""
function clean_comext_dataframe(df, dataset, year)
    if isempty(df)
        @warn "Empty dataframe for Comext dataset $dataset, year $year"
        return df
    end

    @info "Cleaning Comext dataframe with $(nrow(df)) rows and $(ncol(df)) columns"

    # Clean up any problematic columns for DuckDB compatibility
    for col in names(df)
        # Handle columns with mixed types
        if eltype(df[!, col]) == Any
            @info "Converting Any-type column $col to String for DuckDB compatibility"
            df[!, col] = [ismissing(x) ? missing : string(x) for x in df[!, col]]
        end

        # Handle special string values that may cause DuckDB problems
        if eltype(df[!, col]) >: String
            # Replace problematic values with missing
            mask = [x isa String && (x == ":C" || x == ":c" || x == ":" || x == "-" ||
                                     x == "null" || x == "NULL" || x == "NaN" || x == "Inf" || x == "-Inf") for x in df[!, col]]
            if any(mask)
                @info "Replacing $(sum(mask)) special values in column $col with missing"
                df[mask, col] .= missing
            end
        end

        # Handle very large numeric values that might cause DuckDB issues
        if eltype(df[!, col]) >: Number
            extremes = filter(x -> !ismissing(x) && x isa Number && (abs(x) > 1e15), df[!, col])
            if !isempty(extremes)
                @warn "Column $col has $(length(extremes)) extreme values that might cause issues. Converting to missing."
                df[!, col] = [ismissing(x) || !(x isa Number) || abs(x) <= 1e15 ? x : missing for x in df[!, col]]
            end
        end
    end

    # Ensure consistent column names (replace spaces and special characters)
    for old_name in names(df)
        new_name = replace(string(old_name), r"[^a-zA-Z0-9_]" => "_")
        if old_name != new_name
            @info "Renaming column '$old_name' to '$new_name' for database compatibility"
            rename!(df, old_name => new_name)
        end
    end

    @info "Cleaned dataframe now has $(nrow(df)) rows and $(ncol(df)) columns"
    return df
end

end # module