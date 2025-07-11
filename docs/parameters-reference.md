# Parameters Reference Guide

## Overview

This document describes the structure and metadata of parameters used in the `ANALYSIS_PARAMETERS` constant. These parameters control various aspects of the circularity analysis and are stored in the processed database for reproducibility.

## Parameter Categories

### 1. Circularity Rates

#### `current_circularity_rates`
- **Type**: `Dict{String, Float64}`
- **Purpose**: Defines the current material recirculation rate for each product
- **Structure**:
  - Keys: Product codes (PRODCOM format without dots) or "default"
  - Values: Percentage values (0.0 to 100.0)
- **Required Keys**:
  - Product codes (e.g., "28211330", "27114000"): Each product must have a specified rate
- **Structure**:
  - Product-specific codes (e.g., "28211330" for heat pumps)
- **Example**:
  ```julia
  "current_circularity_rates" => Dict{String, Float64}(
      "default" => 0.0,
      "28211330" => 5.5,  # Heat pumps
      "27114000" => 3.2   # PV panels
  )
  ```

#### `potential_circularity_rates`
- **Type**: `Dict{String, Float64}`
- **Purpose**: Defines achievable recirculation rates with best practices/innovations
- **Structure**: Same as `current_circularity_rates`
- **Required Keys**:
  - Product codes matching those in `current_circularity_rates`
- **Constraints**: Values should be ≥ corresponding `current_circularity_rates`
- **Example**:
  ```julia
  "potential_circularity_rates" => Dict{String, Float64}(
      "default" => 30.0,
      "28211330" => 45.0,  # Heat pumps potential
      "27114000" => 65.0   # PV panels potential
  )
  ```

### 2. Unit Conversion Parameters

#### `product_weights_tonnes`
- **Type**: `Dict{String, Float64}`
- **Purpose**: Convert piece counts to tonnes for products sold by unit
- **Structure**:
  - Keys: Product codes (full or prefix) or special identifiers
  - Values: Weight in tonnes per piece
- **Required Keys**:
  - Product codes: Each product must have a specified weight
- **Special Keys**:
  - Product code prefixes (e.g., "2720" for all battery codes starting with 2720)
  - Category identifiers (e.g., "battery_cell" for specific component types)
- **Matching Rules**: 
  1. Exact product code match
  2. Longest matching prefix
  3. Category identifier
  4. Default value
- **Example**:
  ```julia
  "product_weights_tonnes" => Dict{String, Float64}(
      "28211330" => 0.100,           # Heat pumps ~100kg
      "27114000" => 0.020,           # PV panels ~20kg
      "26201230" => 0.015,           # Printers ~15kg
      "2720" => 0.0008,              # Batteries (prefix) ~800g average
      "battery_cell" => 0.0003       # Battery cells ~300g
  )
  ```

### 3. Recovery Efficiency Parameters

#### `recovery_efficiency` (Optional)
- **Type**: `Dict{String, Dict{String, Any}}`
- **Purpose**: Material recovery rates by recycling method
- **Structure**:
  - Top-level keys: Recovery method names
  - Values: Dictionary with method details
- **Method Dictionary Keys**:
  - `"efficiency_rate"` (Float64): Recovery rate (0.0 to 1.0)
  - `"material_type"` (String, optional): Specific material category
  - `"applicable_products"` (Array{String}, optional): Product codes
  - `"notes"` (String, optional): Additional information
- **Example**:
  ```julia
  "recovery_efficiency" => Dict{String, Dict{String, Any}}(
      "mechanical_recycling" => Dict(
          "efficiency_rate" => 0.85,
          "material_type" => "metals",
          "applicable_products" => ["2720", "2825"]
      ),
      "chemical_recycling" => Dict(
          "efficiency_rate" => 0.65,
          "material_type" => "plastics"
      )
  )
  ```

## Adding New Parameters

### 1. Product-Specific Parameters

To add parameters for a new product:
1. Identify the PRODCOM code (remove dots)
2. Add entries in relevant parameter dictionaries
3. Ensure consistency across related parameters

### 2. New Parameter Categories

To add a new parameter category:
1. Add the new dictionary to `ANALYSIS_PARAMETERS`
2. Create corresponding database table structure in `DataProcessor`
3. Update PRQL queries to use new parameters
4. Document in this reference guide

### 3. Parameter Validation

When adding parameters, ensure:
- Data types match the specified format
- Values are within valid ranges
- Required keys are present
- Related parameters are consistent

## Database Storage

Parameters are stored in the processed database as separate tables:

1. **`parameters_circularity_rate`**
   - Stores product-specific rates with one row per product
   - Includes product_code, current_rate, potential_rate, and timestamp

2. **`parameters_recovery_efficiency`**
   - One row per recovery method
   - Includes efficiency rate and metadata

## PRQL Transformations with Product-Specific Rates

The product-specific parameters are used in PRQL queries through table joins:

### In `circularity_indicators.prql`

```prql
from p = production_temp_{{YEAR}}
join side:full t = trade_temp_{{YEAR}} (p.product_code == t.product_code && p.geo == t.geo)
# Join with circularity parameters based on product code
join side:left pcr = parameters_circularity_rate (s"COALESCE(p.product_code, t.product_code) = pcr.product_code")

derive {
    # Product-specific rates from parameters table
    current_circularity_rate_pct = pcr.current_circularity_rate,
    potential_circularity_rate_pct = pcr.potential_circularity_rate
}
```

### In `update_circularity_parameters.prql`

```prql
from ci = circularity_indicators_{{YEAR}}
# Join to get product-specific rates
join pcr = parameters_circularity_rate (ci.product_code == pcr.product_code)

derive {
    # Calculate savings using product-specific rates
    estimated_material_savings_tonnes = s"""
        CASE
            WHEN apparent_consumption_tonnes > 0
            THEN apparent_consumption_tonnes * (pcr.potential_circularity_rate - pcr.current_circularity_rate) / 100.0
            ELSE 0.0
        END
    """
}
```

### Key Points

- **Join on product_code**: Each query joins the parameters table with data tables using the product code
- **No defaults needed**: Since all products have specified rates, no fallback logic is required
- **Left joins**: Use left joins to handle cases where data might exist for products not in parameters
- **COALESCE**: Used in full outer joins to handle nulls from either side

## Best Practices

1. **Use Meaningful Keys**: Product codes should match PRODCOM standards
2. **Document Sources**: Add comments for parameter values from literature
3. **Version Control**: Parameters are part of code for tracking changes
4. **Validate Ranges**: Ensure percentages are 0-100, shares sum to 1
5. **Default Values**: Always provide sensible defaults
6. **Consistent Units**: Weights in tonnes, rates as percentages

## Future Extensions

Planned parameter categories:
- Product lifespan estimates
- Material composition percentages
- Regional adjustment factors
- Technology adoption curves
- Price elasticity factors

## Parameter Update Workflow

1. Update values in `CirQuant.jl` ANALYSIS_PARAMETERS constant
2. Run processing to store in database
3. Use PRQL queries to apply updated parameters
4. Document significant changes in git commits

## Related Documentation

- [Methodology](methodology.md) - How parameters are used in analysis
- [Database Schema - Processed](database-schema-processed.md) - Parameter table structures
- [Data Sources](data-sources.md) - Context for parameter selection