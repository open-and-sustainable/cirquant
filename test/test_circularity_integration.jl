# Integration test for CircularityProcessor with CirQuant module
# This test uses the actual CirQuant module with all dependencies

using Test
using DataFrames
using Dates
using Pkg

# Activate the project
Pkg.activate(joinpath(@__DIR__, ".."))

# Load CirQuant module directly
push!(LOAD_PATH, joinpath(@__DIR__, "..", "src"))
include(joinpath(@__DIR__, "..", "src", "CirQuant.jl"))
using .CirQuant
using .CirQuant.CircularityProcessor

@testset "CircularityProcessor Integration Tests" begin
    # Create temporary test databases
    test_raw_db = tempname() * "_raw.duckdb"
    test_processed_db = tempname() * "_processed.duckdb"

    # Test year
    test_year = 2009

    @testset "Basic Functionality" begin
        # Test table name generation
        @test CircularityProcessor.get_circularity_table_name(2009) == "circularity_indicators_2009"
        @test CircularityProcessor.get_circularity_table_name(2023) == "circularity_indicators_2023"
    end

    @testset "Table Creation and Validation" begin
        # Create circularity table
        success = CircularityProcessor.create_circularity_table(
            test_year,
            db_path=test_processed_db,
            replace=false
        )
        @test success == true

        # Validate the created table
        validation = CircularityProcessor.validate_circularity_table(
            test_year,
            db_path=test_processed_db
        )
        @test validation[:exists] == true
        @test validation[:has_correct_columns] == true
        @test isempty(validation[:missing_columns])
        @test validation[:row_count] == 0

        # Test that recreating without replace fails
        success = CircularityProcessor.create_circularity_table(
            test_year,
            db_path=test_processed_db,
            replace=false
        )
        @test success == false

        # Test recreating with replace succeeds
        success = CircularityProcessor.create_circularity_table(
            test_year,
            db_path=test_processed_db,
            replace=true
        )
        @test success == true
    end

    @testset "Product Conversion Table" begin
        # Create product conversion table
        success = CirQuant.write_product_conversion_table(test_processed_db)
        @test success == true

        # Load product mapping
        mapping_df = CircularityProcessor.load_product_mapping(test_processed_db)
        @test !isnothing(mapping_df)
        @test isa(mapping_df, DataFrame)
        @test nrow(mapping_df) > 0

        # Check required columns
        required_cols = ["product_id", "product", "prodcom_code", "hs_codes"]
        for col in required_cols
            @test col in names(mapping_df)
        end
    end

    @testset "Product Code Mapping" begin
        # Get the standard product mapping data
        mapping_df = CirQuant.get_product_mapping_data()

        # Test data with known PRODCOM codes
        test_data = DataFrame(
            product_code = ["28.21.13.30", "27.11.40.00", "26.20.12.30", "UNKNOWN"],
            quantity = [100.0, 200.0, 300.0, 400.0]
        )

        # Map PRODCOM codes
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
        @test mapped_data[3, :product_name] == "Printers"
        @test ismissing(mapped_data[4, :product_name])  # Unknown code

        # Test with HS codes
        test_hs_data = DataFrame(
            product_code = ["8418.69", "8541.43", "8507.60", "9999.99"],
            value = [1000.0, 2000.0, 3000.0, 4000.0]
        )

        mapped_hs_data = CircularityProcessor.map_product_codes(
            test_hs_data,
            mapping_df,
            source_code_col=:product_code,
            source_type=:hs_codes
        )

        @test mapped_hs_data[1, :product_name] == "Heat pumps"
        @test mapped_hs_data[2, :product_name] == "PV panels"
        @test mapped_hs_data[3, :product_name] == "Batteries - Li-ion"
        @test ismissing(mapped_hs_data[4, :product_name])  # Unknown code
    end

    @testset "Create Tables for Multiple Years" begin
        results = CircularityProcessor.create_circularity_tables_range(
            2020, 2022,
            db_path=test_processed_db,
            replace=true
        )

        @test results[:successful] == 3  # 2020, 2021, 2022
        @test results[:failed] == 0
        @test results[:skipped] == 0

        # Verify each table exists and is valid
        for year in 2020:2022
            validation = CircularityProcessor.validate_circularity_table(
                year,
                db_path=test_processed_db
            )
            @test validation[:exists] == true
            @test validation[:has_correct_columns] == true
        end
    end

    @testset "PRQL Query Execution" begin
        # Create a test PRQL file
        test_prql = tempname() * ".prql"
        open(test_prql, "w") do f
            write(f, """
            from test_data_{{YEAR}}
            select {
                product_code,
                year = {{YEAR}},
                value
            }
            """)
        end

        # Create test database with year-specific table
        using DuckDB, DBInterface
        db_conn = DuckDB.DB(test_raw_db)
        con = DBInterface.connect(db_conn)

        DBInterface.execute(con, """
            CREATE TABLE test_data_2009 (
                product_code VARCHAR,
                value DOUBLE
            )
        """)

        DBInterface.execute(con, """
            INSERT INTO test_data_2009 VALUES
            ('PROD001', 100.0),
            ('PROD002', 200.0)
        """)

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        # Execute PRQL with year substitution
        result_df = CircularityProcessor.execute_prql_for_year(
            test_prql,
            test_raw_db,
            2009
        )

        @test !isnothing(result_df)
        @test isa(result_df, DataFrame)
        @test nrow(result_df) == 2
        @test all(result_df.year .== 2009)

        # Clean up
        rm(test_prql, force=true)
    end

    @testset "Raw Table Inspection" begin
        # Create mock raw database tables
        using DuckDB, DBInterface
        db_conn = DuckDB.DB(test_raw_db)
        con = DBInterface.connect(db_conn)

        # Create mock PRODCOM tables
        for dataset in ["056120", "056121"]
            DBInterface.execute(con, """
                CREATE TABLE prodcom_ds_$(dataset)_2009 (
                    product_code VARCHAR,
                    country_code VARCHAR,
                    quantity DOUBLE,
                    value DOUBLE,
                    year INTEGER DEFAULT 2009
                )
            """)
        end

        # Create mock COMEXT table
        DBInterface.execute(con, """
            CREATE TABLE comext_DS_045409_2009 (
                product_code VARCHAR,
                reporter_code VARCHAR,
                flow_type VARCHAR,
                quantity DOUBLE,
                value DOUBLE,
                year INTEGER DEFAULT 2009
            )
        """)

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        # Inspect tables
        inspection_results = CircularityProcessor.inspect_raw_tables(
            test_raw_db,
            2009,
            show_sample=false
        )

        @test haskey(inspection_results, "prodcom_ds_056120_2009")
        @test haskey(inspection_results, "prodcom_ds_056121_2009")
        @test haskey(inspection_results, "comext_DS_045409_2009")

        @test inspection_results["prodcom_ds_056120_2009"]["exists"] == true
        @test inspection_results["prodcom_ds_056121_2009"]["exists"] == true
        @test inspection_results["comext_DS_045409_2009"]["exists"] == true
    end

    @testset "Process Year Data Pipeline" begin
        # Create PRQL files for testing
        prod_prql = tempname() * "_production.prql"
        trade_prql = tempname() * "_trade.prql"

        open(prod_prql, "w") do f
            write(f, """
            from prodcom_ds_056120_{{YEAR}}
            select {
                product_code,
                year = {{YEAR}},
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
            derive {
                is_import = s"flow_type = 'IMPORT'",
                is_export = s"flow_type = 'EXPORT'"
            }
            select {
                product_code,
                year = {{YEAR}},
                geo = reporter_code,
                level = 'country',
                import_volume_tonnes = s"CASE WHEN is_import THEN quantity ELSE 0 END",
                import_value_eur = s"CASE WHEN is_import THEN value ELSE 0 END",
                export_volume_tonnes = s"CASE WHEN is_export THEN quantity ELSE 0 END",
                export_value_eur = s"CASE WHEN is_export THEN value ELSE 0 END"
            }
            """)
        end

        prql_files = Dict(
            "production" => prod_prql,
            "trade" => trade_prql
        )

        # Run the processing pipeline
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
        @test haskey(results, :rows_processed)
        @test haskey(results, :errors)

        # Table and mapping should be created successfully
        @test results[:table_created] == true
        @test results[:product_mapping_loaded] == true

        # Clean up PRQL files
        rm(prod_prql, force=true)
        rm(trade_prql, force=true)
    end

    # Clean up test databases
    rm(test_raw_db, force=true)
    rm(test_processed_db, force=true)
end

println("\nAll CircularityProcessor integration tests completed!")
