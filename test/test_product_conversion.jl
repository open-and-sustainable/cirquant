using Test
using CirQuant
using DataFrames
using DuckDB

@testset "ProductConversionTables Tests" begin

    # Create a temporary database for testing
    test_db_path = tempname() * ".duckdb"

    @testset "get_product_mapping_data" begin
        # Test that the function returns a DataFrame with expected structure
        mapping_df = CirQuant.get_product_mapping_data()

        @test isa(mapping_df, DataFrame)
        @test nrow(mapping_df) == 13
        @test ncol(mapping_df) == 4

        # Check column names
        expected_cols = [:product_id, :product, :prodcom_code, :hs_codes]
        @test all(col in names(mapping_df) for col in expected_cols)

        # Check that product IDs are unique
        @test length(unique(mapping_df.product_id)) == nrow(mapping_df)

        # Check specific entries
        heat_pumps = mapping_df[mapping_df.product_id.==1, :]
        @test nrow(heat_pumps) == 1
        @test heat_pumps.product[1] == "Heat pumps"
        @test heat_pumps.prodcom_code[1] == "28.21.13.30"
        @test heat_pumps.hs_codes[1] == "8418.69"

        # Check ICT products
        ict_products = filter(row -> occursin("ICT", row.product), mapping_df)
        @test nrow(ict_products) == 7
    end

    @testset "write_product_conversion_table" begin
        # Test writing to database
        success = CirQuant.write_product_conversion_table(test_db_path)
        @test success == true

        # Verify the file was created
        @test isfile(test_db_path)

        # Test that we can connect and the table exists
        db_conn = DuckDB.DB(test_db_path)
        con = DBInterface.connect(db_conn)

        # Check table exists
        tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'product_mapping_codes'
        """
        result = DBInterface.execute(con, tables_query) |> DataFrame
        @test nrow(result) == 1

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        # Test writing with custom table name
        custom_table = "custom_product_mapping"
        success = CirQuant.write_product_conversion_table(test_db_path, table_name=custom_table)
        @test success == true
    end

    @testset "read_product_conversion_table" begin
        # Test reading from database
        mapping_df = CirQuant.read_product_conversion_table(test_db_path)

        @test !isnothing(mapping_df)
        @test isa(mapping_df, DataFrame)
        @test nrow(mapping_df) == 13

        # Verify data integrity
        original_df = CirQuant.get_product_mapping_data()
        @test mapping_df.product_id == original_df.product_id
        @test mapping_df.product == original_df.product
        @test mapping_df.prodcom_code == original_df.prodcom_code
        @test mapping_df.hs_codes == original_df.hs_codes

        # Test reading non-existent table
        result = CirQuant.read_product_conversion_table(test_db_path, table_name="non_existent_table")
        @test isnothing(result)
    end

    @testset "get_product_by_code" begin
        # First ensure the table is written
        CirQuant.write_product_conversion_table(test_db_path)

        # Test PRODCOM code lookup
        result = CirQuant.get_product_by_code("26.20.12.30", :prodcom_code, db_path=test_db_path)
        @test !isnothing(result)
        @test nrow(result) == 1
        @test result.product[1] == "Printers"
        @test result.hs_codes[1] == "8443.31"

        # Test HS code lookup
        result = CirQuant.get_product_by_code("8507.60", :hs_codes, db_path=test_db_path)
        @test !isnothing(result)
        @test nrow(result) == 1
        @test result.product[1] == "Batteries - Li-ion"

        # Test HS code in comma-separated list
        result = CirQuant.get_product_by_code("8471.41", :hs_codes, db_path=test_db_path)
        @test !isnothing(result)
        @test nrow(result) == 1
        @test result.product[1] == "ICT - Other computers"

        # Test non-existent code
        result = CirQuant.get_product_by_code("99.99.99.99", :prodcom_code, db_path=test_db_path)
        @test isnothing(result) || nrow(result) == 0

        # Test invalid code type
        result = CirQuant.get_product_by_code("some_code", :invalid_type, db_path=test_db_path)
        @test isnothing(result)
    end

    @testset "ProductConversionTables.validate_product_mapping" begin
        # Test with valid data
        valid_df = CirQuant.get_product_mapping_data()
        @test CirQuant.ProductConversionTables.validate_product_mapping(valid_df) == true

        # Test with missing column
        invalid_df = select(valid_df, Not(:prodcom_code))
        @test CirQuant.ProductConversionTables.validate_product_mapping(invalid_df) == false

        # Test with duplicate product IDs
        dup_df = copy(valid_df)
        dup_df.product_id[2] = dup_df.product_id[1]
        @test CirQuant.ProductConversionTables.validate_product_mapping(dup_df) == false
    end

    @testset "Edge cases and error handling" begin
        # Test with non-existent database path
        non_existent_db = "/non/existent/path/db.duckdb"
        @test_logs (:error,) CirQuant.write_product_conversion_table(non_existent_db) == false

        # Test reading from non-existent database
        result = CirQuant.read_product_conversion_table(non_existent_db)
        @test isnothing(result)
    end

    # Cleanup
    if isfile(test_db_path)
        rm(test_db_path)
    end
end

# Integration test with main module constants
@testset "Integration with CirQuant module" begin
    # Create temporary test database
    test_db_path = tempname() * ".duckdb"

    try
        # Test that module functions work without specifying db_path
        # (Note: This would use the default DB_PATH_PROCESSED, which may not exist in test environment)

        # Test getting mapping data (doesn't require database)
        mapping_df = CirQuant.get_product_mapping_data()
        @test isa(mapping_df, DataFrame)
        @test nrow(mapping_df) > 0

        # Test product lookup data structure
        smartphones = filter(row -> row.product == "ICT - Smartphones", mapping_df)
        @test nrow(smartphones) == 1
        @test smartphones.prodcom_code[1] == "26.20.11.30"
        @test smartphones.hs_codes[1] == "8517.13"

    finally
        # Cleanup
        if isfile(test_db_path)
            rm(test_db_path)
        end
    end
end
