# Standalone test for CircularityProcessor module
# This test runs without requiring the full CirQuant module or external dependencies

using Test
using DataFrames
using DuckDB, DBInterface
using Dates

# Directly include only the necessary modules
include("../src/DataTransform/CircularityProcessor.jl")
using .CircularityProcessor

# Mock the database paths
const TEST_DB_PATH_RAW = tempname() * "_raw.duckdb"
const TEST_DB_PATH_PROCESSED = tempname() * "_processed.duckdb"

# Create minimal product mapping data for testing
function create_test_product_mapping()
    return DataFrame(
        product_id = [1, 2, 3],
        product = ["Test Product 1", "Test Product 2", "Test Product 3"],
        prodcom_code = ["10.10.10.00", "20.20.20.00", "30.30.30.00"],
        hs_codes = ["1234.56", "2345.67", "3456.78"]
    )
end

# Helper function to create test database with product mapping
function setup_test_database(db_path::String)
    db_conn = DuckDB.DB(db_path)
    con = DBInterface.connect(db_conn)

    # Create product mapping table
    DBInterface.execute(con, """
        CREATE TABLE product_mapping_codes (
            product_id INTEGER,
            product VARCHAR,
            prodcom_code VARCHAR,
            hs_codes VARCHAR
        )
    """)

    # Insert test data
    mapping_df = create_test_product_mapping()
    for row in eachrow(mapping_df)
        DBInterface.execute(con, """
            INSERT INTO product_mapping_codes VALUES (
                $(row.product_id),
                '$(row.product)',
                '$(row.prodcom_code)',
                '$(row.hs_codes)'
            )
        """)
    end

    DBInterface.close!(con)
    DBInterface.close!(db_conn)
end

@testset "CircularityProcessor Standalone Tests" begin
    # Setup test database
    setup_test_database(TEST_DB_PATH_PROCESSED)

    @testset "Table Name Generation" begin
        @test CircularityProcessor.get_circularity_table_name(2009) == "circularity_indicators_2009"
        @test CircularityProcessor.get_circularity_table_name(2023) == "circularity_indicators_2023"
        @test CircularityProcessor.get_circularity_table_name(1995) == "circularity_indicators_1995"
    end

    @testset "Circularity Table Creation" begin
        year = 2009

        # Test creating new table
        result = CircularityProcessor.create_circularity_table(
            year,
            db_path=TEST_DB_PATH_PROCESSED,
            replace=false
        )
        @test result == true

        # Test creating duplicate without replace fails
        result = CircularityProcessor.create_circularity_table(
            year,
            db_path=TEST_DB_PATH_PROCESSED,
            replace=false
        )
        @test result == false

        # Test creating with replace succeeds
        result = CircularityProcessor.create_circularity_table(
            year,
            db_path=TEST_DB_PATH_PROCESSED,
            replace=true
        )
        @test result == true
    end

    @testset "Table Validation" begin
        year = 2009

        # Validate existing table
        validation = CircularityProcessor.validate_circularity_table(
            year,
            db_path=TEST_DB_PATH_PROCESSED
        )

        @test validation[:exists] == true
        @test validation[:has_correct_columns] == true
        @test isempty(validation[:missing_columns])
        @test validation[:row_count] == 0

        # Validate non-existent table
        validation_missing = CircularityProcessor.validate_circularity_table(
            1990,
            db_path=TEST_DB_PATH_PROCESSED
        )
        @test validation_missing[:exists] == false
    end

    @testset "Product Mapping Loading" begin
        # Test loading product mapping
        mapping_df = CircularityProcessor.load_product_mapping(TEST_DB_PATH_PROCESSED)

        @test !isnothing(mapping_df)
        @test isa(mapping_df, DataFrame)
        @test nrow(mapping_df) == 3
        @test "product_id" in names(mapping_df)
        @test "product" in names(mapping_df)
        @test "prodcom_code" in names(mapping_df)
        @test "hs_codes" in names(mapping_df)
    end

    @testset "Product Code Mapping" begin
        # Load test mapping
        mapping_df = create_test_product_mapping()

        # Test data with PRODCOM codes
        test_data = DataFrame(
            product_code = ["10.10.10.00", "20.20.20.00", "99.99.99.99"],
            quantity = [100.0, 200.0, 300.0]
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
        @test mapped_data[1, :product_name] == "Test Product 1"
        @test mapped_data[2, :product_name] == "Test Product 2"
        @test ismissing(mapped_data[3, :product_name])
        @test mapped_data[1, :product_id] == 1
        @test mapped_data[2, :product_id] == 2
        @test ismissing(mapped_data[3, :product_id])

        # Test data with HS codes
        test_hs_data = DataFrame(
            product_code = ["1234.56", "2345.67", "9999.99"],
            value = [1000.0, 2000.0, 3000.0]
        )

        # Map HS codes
        mapped_hs_data = CircularityProcessor.map_product_codes(
            test_hs_data,
            mapping_df,
            source_code_col=:product_code,
            source_type=:hs_codes
        )

        @test mapped_hs_data[1, :product_name] == "Test Product 1"
        @test mapped_hs_data[2, :product_name] == "Test Product 2"
        @test ismissing(mapped_hs_data[3, :product_name])
    end

    @testset "Create Tables Range" begin
        # Test creating multiple tables
        results = CircularityProcessor.create_circularity_tables_range(
            2020, 2022,
            db_path=TEST_DB_PATH_PROCESSED,
            replace=true
        )

        @test results[:successful] == 3
        @test results[:failed] == 0
        @test results[:skipped] == 0

        # Verify each table exists
        for year in 2020:2022
            validation = CircularityProcessor.validate_circularity_table(
                year,
                db_path=TEST_DB_PATH_PROCESSED
            )
            @test validation[:exists] == true
            @test validation[:has_correct_columns] == true
        end
    end

    @testset "PRQL Execution with Year Substitution" begin
        # Create test PRQL file
        test_prql = tempname() * ".prql"
        open(test_prql, "w") do f
            write(f, """
            from test_table_{{YEAR}}
            select {product_code, value}
            """)
        end

        # Create test database with year-specific table
        db_conn = DuckDB.DB(TEST_DB_PATH_RAW)
        con = DBInterface.connect(db_conn)

        DBInterface.execute(con, """
            CREATE TABLE test_table_2009 (
                product_code VARCHAR,
                value DOUBLE
            )
        """)

        DBInterface.execute(con, """
            INSERT INTO test_table_2009 VALUES
            ('PROD001', 100.0),
            ('PROD002', 200.0)
        """)

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        # Test PRQL execution
        result = CircularityProcessor.execute_prql_for_year(
            test_prql,
            TEST_DB_PATH_RAW,
            2009
        )

        @test !isnothing(result)
        @test isa(result, DataFrame)
        @test nrow(result) == 2
        @test result[1, :product_code] == "PROD001"
        @test result[2, :product_code] == "PROD002"

        # Clean up
        rm(test_prql, force=true)
    end

    @testset "Raw Table Inspection" begin
        # Create mock raw tables
        db_conn = DuckDB.DB(TEST_DB_PATH_RAW)
        con = DBInterface.connect(db_conn)

        # Create mock PRODCOM table
        DBInterface.execute(con, """
            CREATE TABLE prodcom_ds_056120_2009 (
                product_code VARCHAR,
                country_code VARCHAR,
                quantity DOUBLE,
                value DOUBLE
            )
        """)

        # Create mock COMEXT table
        DBInterface.execute(con, """
            CREATE TABLE comext_DS_045409_2009 (
                product_code VARCHAR,
                reporter_code VARCHAR,
                flow_type VARCHAR,
                quantity DOUBLE,
                value DOUBLE
            )
        """)

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        # Test inspection
        results = CircularityProcessor.inspect_raw_tables(
            TEST_DB_PATH_RAW,
            2009,
            show_sample=false
        )

        @test haskey(results, "prodcom_ds_056120_2009")
        @test haskey(results, "comext_DS_045409_2009")
        @test results["prodcom_ds_056120_2009"]["exists"] == true
        @test results["comext_DS_045409_2009"]["exists"] == true

        # Check columns exist
        prodcom_cols = results["prodcom_ds_056120_2009"]["columns"]
        @test "product_code" in prodcom_cols.column_name
        @test "quantity" in prodcom_cols.column_name

        comext_cols = results["comext_DS_045409_2009"]["columns"]
        @test "product_code" in comext_cols.column_name
        @test "flow_type" in comext_cols.column_name
    end

    # Cleanup test databases
    rm(TEST_DB_PATH_RAW, force=true)
    rm(TEST_DB_PATH_PROCESSED, force=true)
end

println("\nStandalone CircularityProcessor tests completed successfully!")
