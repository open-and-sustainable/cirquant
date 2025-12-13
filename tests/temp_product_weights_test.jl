#!/usr/bin/env julia

# Temporary test harness for validating data-driven product weight calculations.
# Remove once the production pipeline fully replaces configuration weights.

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

push!(LOAD_PATH, joinpath(@__DIR__, "..", "src"))

using Test
using DataFrames
using CirQuant
using CirQuant.ProductWeightsFetch: compute_average_weights_from_df

function mock_prodcom_data()
    n = 12
    DataFrame(
        time = fill("2024", n),
        prccode = [
            "26201300", "26201300", "26201300", "26201300",
            "26201700", "26201700", "26201700", "26201700",
            "99999999", "99999999", "88888888", "88888888"
        ],
        decl = [
            "004", "004", "005", "005",
            "017", "017", "060", "060",
            "004", "004", "005", "005"
        ],
        indicators = [
            "PRODQNT", "QNTUNIT", "PRODQNT", "QNTUNIT",
            "PRODQNT", "QNTUNIT", "PRODQNT", "QNTUNIT",
            "PRODQNT", "QNTUNIT", "PRODQNT", "QNTUNIT"
        ],
        value = [
            "120", "t",
            "400", "p/st",
            "30", "t",
            "150", "p/st",
            "10", "t",     # mass only product (ignored)
            "60", "p/st"   # units only product (ignored)
        ]
    )
end

function expected_weights()
    Dict(
        "26201300" => 300.0,  # 120 tonnes / 400 units = 0.3 t = 300 kg
        "26201700" => 200.0   # (30 tonnes) / (150 units) = 0.2 t = 200 kg
    )
end

@testset "Average product weight calculation (temporary)" begin
    prodcom_df = mock_prodcom_data()
    result = compute_average_weights_from_df(prodcom_df; year=2024)

    @test nrow(result) == 2

    exp = expected_weights()
    for row in eachrow(result)
        @test row.prodcom_code in keys(exp)
        @test row.average_weight_kg ≈ exp[row.prodcom_code] atol=1e-6
        @test row.geo == "EU27_2020"
    end

    # Ensure rows lacking corresponding mass or units are skipped
    observed_codes = Set(result.prodcom_code)
    @test !("99999999" in observed_codes)
    @test !("88888888" in observed_codes)
end

println("Temporary product weight test completed.")
