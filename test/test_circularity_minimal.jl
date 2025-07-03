# Minimal standalone test for CircularityProcessor functionality
# This test is self-contained and doesn't require the full CirQuant module

using Test
using DataFrames
using DuckDB, DBInterface
using Dates

# Minimal implementation of required functions for testing
module MinimalCircularityProcessor

using DataFrames
using DuckDB, DBInterface
using Dates

export get_circularity_table_name, create_circularity_table, validate_circularity_table

function get_circularity_table_name(year::Int)
    return "circularity_indicators_$(year)"
end

function create_circularity_table(year::Int; db_path::String, replace::Bool=false)
    table_name = get_circularity_table_name(year)

    try
        db_conn = DuckDB.DB(db_path)
        con = DBInterface.connect(db_conn)

        # Check if table exists
        result = DBInterface.execute(con,
            "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '$table_name'"
        ) |> DataFrame

        if result.cnt[1] > 0
            if replace
                DBInterface.execute(con, "DROP TABLE IF EXISTS $table_name")
            else
                DBInterface.close!(con)
                DBInterface.close!(db_conn)
                return false
            end
        end

        # Create the table
        create_sql = """
        CREATE TABLE $table_name (
            product_code VARCHAR NOT NULL,
            product_name VARCHAR NOT NULL,
            year INTEGER NOT NULL,
            geo VARCHAR NOT NULL,
            level VARCHAR NOT NULL,
            production_volume_tonnes DOUBLE,
            production_value_eur DOUBLE,
            import_volume_tonnes DOUBLE,
            import_value_eur DOUBLE,
            export_volume_tonnes DOUBLE,
            export_value_eur DOUBLE,
            apparent_consumption_tonnes DOUBLE,
            apparent_consumption_value_eur DOUBLE,
            current_circularity_rate_pct DOUBLE,
            potential_circularity_rate_pct DOUBLE,
            estimated_material_savings_tonnes DOUBLE,
            estimated_monetary_savings_eur DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (product_code, geo, year),
            CHECK (year = $year),
            CHECK (level IN ('country', 'EU'))
        )
        """

        DBInterface.execute(con, create_sql)

        DBInterface.close!(con)
        DBInterface.close!(db_conn)
        return true

    catch e
        println("Error creating table: $e")
        return false
    end
end

function validate_circularity_table(year::Int; db_path::String)
    table_name = get_circularity_table_name(year)
    validation_result = Dict(
        :exists => false,
        :has_correct_columns => false,
        :missing_columns => String[],
        :row_count => 0
    )

    try
        db_conn = DuckDB.DB(db_path)
        con = DBInterface.connect(db_conn)

        # Check if table exists
        result = DBInterface.execute(con,
            "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '$table_name'"
        ) |> DataFrame

        if result.cnt[1] == 0
            DBInterface.close!(con)
            DBInterface.close!(db_conn)
            return validation_result
        end

        validation_result[:exists] = true

        # Get columns
        columns_df = DBInterface.execute(con,
            "SELECT column_name FROM information_schema.columns WHERE table_name = '$table_name'"
        ) |> DataFrame

        existing_columns = Set(columns_df.column_name)

        required_columns = Set([
            "product_code", "product_name", "year", "geo", "level",
            "production_volume_tonnes", "production_value_eur",
            "import_volume_tonnes", "import_value_eur",
            "export_volume_tonnes", "export_value_eur",
            "apparent_consumption_tonnes", "apparent_consumption_value_eur",
            "current_circularity_rate_pct", "potential_circularity_rate_pct",
            "estimated_material_savings_tonnes", "estimated_monetary_savings_eur"
        ])

        missing = setdiff(required_columns, existing_columns)
        validation_result[:missing_columns] = collect(missing)
        validation_result[:has_correct_columns] = isempty(missing)

        # Get row count
        count_result = DBInterface.execute(con, "SELECT COUNT(*) as cnt FROM $table_name") |> DataFrame
        validation_result[:row_count] = count_result.cnt[1]

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        return validation_result

    catch e
        println("Error validating table: $e")
        return validation_result
    end
end

end # module MinimalCircularityProcessor

# Run tests
println("Running minimal CircularityProcessor tests...")
println("="^60)

using .MinimalCircularityProcessor

@testset "Minimal CircularityProcessor Tests" begin
    # Create temporary test database
    test_db = tempname() * ".duckdb"

    @testset "Table Name Generation" begin
        @test MinimalCircularityProcessor.get_circularity_table_name(2009) == "circularity_indicators_2009"
        @test MinimalCircularityProcessor.get_circularity_table_name(2023) == "circularity_indicators_2023"
    end

    @testset "Table Creation and Validation" begin
        year = 2009

        # Test creating new table
        @test MinimalCircularityProcessor.create_circularity_table(
            year, db_path=test_db, replace=false
        ) == true

        # Test validation of created table
        validation = MinimalCircularityProcessor.validate_circularity_table(year, db_path=test_db)
        @test validation[:exists] == true
        @test validation[:has_correct_columns] == true
        @test isempty(validation[:missing_columns])
        @test validation[:row_count] == 0

        # Test creating duplicate without replace fails
        @test MinimalCircularityProcessor.create_circularity_table(
            year, db_path=test_db, replace=false
        ) == false

        # Test creating with replace succeeds
        @test MinimalCircularityProcessor.create_circularity_table(
            year, db_path=test_db, replace=true
        ) == true
    end

    @testset "Multiple Years" begin
        # Create tables for multiple years
        for year in [2020, 2021, 2022]
            @test MinimalCircularityProcessor.create_circularity_table(
                year, db_path=test_db, replace=true
            ) == true
        end

        # Validate each table
        for year in [2020, 2021, 2022]
            validation = MinimalCircularityProcessor.validate_circularity_table(year, db_path=test_db)
            @test validation[:exists] == true
            @test validation[:has_correct_columns] == true
        end

        # Test non-existent year
        validation = MinimalCircularityProcessor.validate_circularity_table(1990, db_path=test_db)
        @test validation[:exists] == false
    end

    @testset "Table Structure Verification" begin
        # Create a table and insert test data
        year = 2023
        MinimalCircularityProcessor.create_circularity_table(year, db_path=test_db, replace=true)

        # Insert a test row
        db_conn = DuckDB.DB(test_db)
        con = DBInterface.connect(db_conn)

        table_name = MinimalCircularityProcessor.get_circularity_table_name(year)

        insert_sql = """
        INSERT INTO $table_name (
            product_code, product_name, year, geo, level,
            production_volume_tonnes, production_value_eur
        ) VALUES (
            'TEST001', 'Test Product', $year, 'DE', 'country',
            1000.0, 50000.0
        )
        """

        DBInterface.execute(con, insert_sql)

        # Verify the data
        result_df = DBInterface.execute(con, "SELECT * FROM $table_name") |> DataFrame
        @test nrow(result_df) == 1
        @test result_df[1, :product_code] == "TEST001"
        @test result_df[1, :geo] == "DE"
        @test result_df[1, :production_volume_tonnes] == 1000.0

        DBInterface.close!(con)
        DBInterface.close!(db_conn)

        # Validate shows 1 row
        validation = MinimalCircularityProcessor.validate_circularity_table(year, db_path=test_db)
        @test validation[:row_count] == 1
    end

    # Clean up
    rm(test_db, force=true)
end

println("\nAll minimal tests passed successfully!")
println("="^60)
