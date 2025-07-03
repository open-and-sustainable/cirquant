# Main test runner for CirQuant package
using Test

# Add the source directory to load path
push!(LOAD_PATH, joinpath(@__DIR__, "..", "src"))

println("Running CirQuant tests...")
println("="^60)

# Run all test files
test_files = [
    "test_product_conversion.jl",
    "test_circularity_processor.jl"
]

# Track overall test results
total_passed = 0
total_failed = 0
failed_tests = String[]

for test_file in test_files
    println("\nRunning $test_file...")
    println("-"^40)

    try
        include(test_file)
        println("✓ $test_file completed successfully")
        global total_passed += 1
    catch e
        println("✗ $test_file failed: $e")
        push!(failed_tests, test_file)
        global total_failed += 1
    end
end

# Summary
println("\n" * "="^60)
println("Test Summary")
println("="^60)
println("Total test files: $(length(test_files))")
println("Passed: $total_passed")
println("Failed: $total_failed")

if total_failed > 0
    println("\nFailed tests:")
    for test in failed_tests
        println("  - $test")
    end
    exit(1)
else
    println("\n✅ All tests passed!")
end
