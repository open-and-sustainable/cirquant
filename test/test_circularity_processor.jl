# Test file for CircularityProcessor module
using Test
using DataFrames
using Dates
using DuckDB

# Add the source directory to load path
push!(LOAD_PATH, joinpath(@__DIR__, "..", "src"))

using CirQuant
using CirQuant.CircularityProcessor

@testset "CircularityProcessor Tests" begin
    # Create temporary test databases
    test_raw_db = tempname() * ".duckdb"
    test_processed_db = tempname() * ".duckdb"

    # Test year
    test_year = 2009

    @testset "Table Name Generation" begin
        @test CircularityProcessor.get_circularity_table_name(2009) == "circularity_indicators_2009"
        @test CircularityProcessor.get_circularity_table_name(2023) == "circularity_indicators_2023"
    end

    @testset "Circularity Table Creation" begin
        # Test table creation
        @test CircularityProcessor.create_circularity_table(
            test_year,
            db_path=test_processed_db,
            replace=false
        ) == true

        # Test that recreating without replace=true fails
        @test CircularityProcessor.create_circularity_table(
            test_year,
            db_path=test_processed_db,
            replace=false
        ) == false

        # Test recreating with replace=true succeeds
        @test CircularityProcessor.create_circularity_table(
            test_year,
            db_path=test_processed_db,
            replace=true
        ) == true
    end

    @testset "Table Validation" begin
        validation = CircularityProcessor.validate_circularity_table(
            test_year,
            db_path=test_processed_db
        )

        @test validation[:exists] == true
        @test validation[:has_correct_columns] == true
        @test isempty(validation[:missing_columns])
        @test validation[:row_count] == 0  # Empty table initially

        # Test validation for non-existent year
        validation_missing = CircularityProcessor.validate_circularity_table(
            1990,
            db_path=test_processed_db
        )
        @test validation_missing[:exists] == false
    end

    @testset "Product Mapping" begin
        # First create the product conversion table
        success = CirQuant.write_product_conversion_table(test_processed_db)
        @test success == true

        # Test loading product mapping
        mapping_df = CircularityProcessor.load_product_mapping(test_processed_db)
        @test !isnothing(mapping_df)
        @test isa(mapping_df, DataFrame)
        @test nrow(mapping_df) > 0
        @test all(col -> col in names(mapping_df), ["product_id", "product", "prodcom_code", "hs_codes"])
    end

    @testset "Product Code Mapping" begin
        # Create test data
        test_data = DataFrame(
            product_code = ["28.21.13.30", "27.11.40.00", "UNKNOWN_CODE"],
            quantity = [100.0, 200.0, 300.0]
        )

        # Load actual mapping
        mapping_df = CirQuant.get_product_mapping_data()

        # Test PRODCOM code mapping
        mapped_data = CircularityProcessor.map_product_codes(
            test_data,
            mapping_df,
            source_code_col=:product_code,
            source_type=:prodcom_code
        )

        @test "product_name" in names(mapped_data)
        @test "product_id" in names(mapped_data)
        @test mapped_data[1, :product_name] == "Heat pumps"
        @test mapped_data[2, :product_name] == "PV panels"
        @test ismissing(mapped_data[3, :product_name])  # Unknown code

        # Test HS code mapping
        test_hs_data = DataFrame(
            product_code = ["8418.69", "8541.43", "9999.99"],
            value = [1000.0, 2000.0, 3000.0]
        )

        mapped_hs_data = CircularityProcessor.map_product_codes(
            test_hs_data,
            mapping_df,
            source_code_col=:product_code,
            source_type=:hs_codes
        )

        @test mapped_hs_data[1, :product_name] == "Heat pumps"
        @test mapped_hs_data[2, :product_name] == "PV panels"
        @test ismissing(mapped_hs_data[3, :product_name])  # Unknown code
    end

    @testset "Create Tables Range" begin
        # Test creating tables for a range of years
        results = CircularityProcessor.create_circularity_tables_range(
            2020, 2022,
            db_path=test_processed_db,
            replace=true
        )

        @test results[:successful] == 3  # 2020, 2021, 2022
        @test results[:failed] == 0
        @test results[:skipped] == 0

        # Verify tables exist
        for year in 2020:2022
            validation = CircularityProcessor.validate_circularity_table(
                year,
                db_path=test_processed_db
            )
            @test validation[:exists] == true
        end
    end

    @testset "PRQL File Execution" begin
        # Create a simple test PRQL file
        test_prql = tempname() * ".prql"
        open(test_prql, "w") do f
            write(f, """
            from test_table
            filter year == {{YEAR}}
            select {product_code, year, value}
            """)
        end

        # Create test database with sample data
        db_conn = DuckDB.DB(test_raw_db)
        con = DBInterface.connect(db_conn)

        # Create and populate test table
        DBInterface.execute(con, """
            CREATE TABLE test_table (
                product_code VARCHAR,
                year INTEGER,
                value DOUBLE
            )
        """)

        DBInterface.execute(con, """
            INSERT INTO test_table VALUES
            ('PROD001', 2009, 100.0),
            ('PROD002', 2009, 200.0),
            ('PROD001', 2010, 150.0)
        """)

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        # Test PRQL execution with year substitution
        result_df = CircularityProcessor.execute_prql_for_year(
            test_prql,
            test_raw_db,
            2009
        )

        @test !isnothing(result_df)
        @test isa(result_df, DataFrame)
        @test nrow(result_df) == 2  # Only 2009 data
        @test all(result_df.year .== 2009)

        # Clean up
        rm(test_prql, force=true)
    end

    @testset "Raw Table Inspection" begin
        # Create mock raw tables
        db_conn = DuckDB.DB(test_raw_db)
        con = DBInterface.connect(db_conn)

        # Create mock PRODCOM table
        DBInterface.execute(con, """
            CREATE TABLE prodcom_ds_056120_2009 (
                product_code VARCHAR,
                country_code VARCHAR,
                quantity DOUBLE,
                value DOUBLE,
                year INTEGER
            )
        """)

        # Create mock COMEXT table
        DBInterface.execute(con, """
            CREATE TABLE comext_DS_045409_2009 (
                product_code VARCHAR,
                reporter_code VARCHAR,
                flow_type VARCHAR,
                quantity DOUBLE,
                value DOUBLE,
                year INTEGER
            )
        """)

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        # Test inspection
        inspection_results = CircularityProcessor.inspect_raw_tables(
            test_raw_db,
            2009,
            show_sample=false
        )

        @test haskey(inspection_results, "prodcom_ds_056120_2009")
        @test haskey(inspection_results, "comext_DS_045409_2009")
        @test inspection_results["prodcom_ds_056120_2009"]["exists"] == true
        @test inspection_results["comext_DS_045409_2009"]["exists"] == true

        # Check for non-existent table
        @test haskey(inspection_results, "prodcom_ds_056121_2009")
        @test inspection_results["prodcom_ds_056121_2009"]["exists"] == false
    end

    @testset "Process Year Data Integration" begin
        # This is a more complex integration test
        # Create necessary PRQL files
        prod_prql = tempname() * "_production.prql"
        trade_prql = tempname() * "_trade.prql"

        open(prod_prql, "w") do f
            write(f, """
            from prodcom_ds_056120_{{YEAR}}
            select {
                product_code,
                year,
                geo = country_code,
                level = 'country',
                production_volume_tonnes = quantity,
                production_value_eur = value
            }
            """)
        end

        open(trade_prql, "w") do f
            write(f, """
            from comext_DS_045409_{{YEAR}}
            select {
                product_code,
                year,
                geo = reporter_code,
                level = 'country',
                import_volume_tonnes = 0.0,
                import_value_eur = 0.0,
                export_volume_tonnes = quantity,
                export_value_eur = value
            }
            """)
        end

        prql_files = Dict(
            "production" => prod_prql,
            "trade" => trade_prql
        )

        # Run the process (will fail due to missing tables, but structure should work)
        results = CircularityProcessor.process_year_data(
            2009,
            raw_db_path=test_raw_db,
            processed_db_path=test_processed_db,
            prql_files=prql_files,
            replace=true
        )

        @test isa(results, Dict)
        @test haskey(results, :success)
        @test haskey(results, :table_created)
        @test haskey(results, :product_mapping_loaded)
        @test haskey(results, :queries_executed)
        @test haskey(results, :errors)

        # Table should be created even if queries fail
        @test results[:table_created] == true
        @test results[:product_mapping_loaded] == true

        # Clean up
        rm(prod_prql, force=true)
        rm(trade_prql, force=true)
    end

    # Clean up test databases
    rm(test_raw_db, force=true)
    rm(test_processed_db, force=true)
end

println("\nAll CircularityProcessor tests completed!")
