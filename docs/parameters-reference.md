# Parameters Reference Guide

## Purpose

This document serves as a metadata reference, providing detailed descriptions of all parameters used in CirQuant's analysis, their meanings, valid ranges, and data types. It explains WHAT each parameter represents and WHY it's important for the analysis.

For practical instructions on HOW to set up and configure an analysis, see the [Configuration Guide](configuration-guide.md).

## Overview

The parameters described in this reference are defined in the `config/products.toml` configuration file. At runtime, these parameters are loaded from the TOML file and populated into the `ANALYSIS_PARAMETERS` global variable in the CirQuant module. These parameters control various aspects of the circularity analysis and are stored in the processed database for reproducibility.

## Parameter Categories

### 1. Circularity Rates

#### `current_circularity_rates`
- **Type**: `Dict{String, Float64}`
- **Purpose**: Defines the current material recirculation rate for each product
- **Structure**:
  - Keys: Product codes (PRODCOM format without dots)
  - Values: Percentage values (0.0 to 100.0)
- **Required Keys**:
  - Each product must have its own explicitly defined rate - there are no defaults
- **Example**:
  ```julia
  "current_circularity_rates" => Dict{String, Float64}(
      "28211330" => 5.0,  # Heat pumps
      "27114000" => 3.0   # PV panels
  )
  ```

#### `potential_circularity_rates`
- **Type**: `Dict{String, Float64}`
- **Purpose**: Defines achievable recirculation rates with best practices/innovations
- **Structure**: Same as `current_circularity_rates` - product codes without dots
- **Required Keys**:
  - Each product must have its own explicitly defined rate matching those in `current_circularity_rates`
- **Constraints**: Values should be ≥ corresponding `current_circularity_rates`
- **Example**:
  ```julia
  "potential_circularity_rates" => Dict{String, Float64}(
      "28211330" => 45.0,  # Heat pumps potential
      "27114000" => 65.0   # PV panels potential
  )
  ```

### 2. Unit Conversion Parameters

#### `product_weights_tonnes`
- **Type**: `Dict{String, Float64}`
- **Purpose**: Convert piece counts to tonnes for products sold by unit
- **Structure**:
  - Keys: Product codes (PRODCOM format without dots)
  - Values: Weight in tonnes per piece
- **Required Keys**:
  - Each product must have its own explicitly defined weight - there are no defaults or prefix matching
- **Example**:
  ```julia
  "product_weights_tonnes" => Dict{String, Float64}(
      "28211330" => 0.100,           # Heat pumps ~100kg
      "27114000" => 0.020,           # PV panels ~20kg
      "26201230" => 0.015,           # Printers ~15kg
      "27202300" => 0.0005,          # Li-ion batteries ~500g
      "27202400" => 0.001            # Other batteries ~1kg
  )
  ```

### 3. Recovery Efficiency Parameters

#### `recovery_efficiency` (Optional - Not Currently Used)
- **Type**: `Dict{String, Dict{String, Any}}`
- **Purpose**: Material recovery rates by recycling method (for future use)
- **Note**: This parameter is not present in the current configuration and the `parameters_recovery_efficiency` table is only created if this parameter is provided
- **Structure**:
  - Top-level keys: Recovery method names
  - Values: Dictionary with method details
- **Method Dictionary Keys**:
  - `"efficiency_rate"` (Float64): Recovery rate (0.0 to 1.0)
  - `"material_type"` (String, optional): Specific material category
  - `"applicable_products"` (Array{String}, optional): Product codes
  - `"notes"` (String, optional): Additional information

## Parameter Sources and Basis

### 1. Product-Specific Parameters

Product-specific parameters in CirQuant are based on:
- Industry reports and technical specifications for product weights
- EU policy documents and circular economy action plans for circularity rates
- Scientific literature on material recovery and recycling technologies
- Regulatory frameworks (e.g., EU Battery Regulation, WEEE Directive)

Each parameter represents measurable physical or policy-driven characteristics of products that affect their circular economy potential.

### 2. Parameter Processing

When loaded into the system, parameters undergo specific transformations:
- PRODCOM codes: Dots are removed for internal consistency (e.g., "28.21.13.30" → "28211330")
- Weight units: Kilograms are converted to tonnes for calculations
- Validation: All values are checked against their defined ranges and constraints

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
   - Includes product_code, current_circularity_rate, potential_circularity_rate, and last_updated timestamp
   - Created automatically during data processing

2. **`parameters_recovery_efficiency`** (Optional)
   - Only created if recovery_efficiency parameters are provided in the configuration
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
- **Every product has explicit rates**: Each product in the configuration has its own defined rates - no defaults or fallback values
- **Left joins**: Use left joins to handle cases where data might exist for products not in parameters
- **COALESCE**: Used in full outer joins to handle nulls from either side

## Best Practices

1. **Use Meaningful Keys**: Product codes should match PRODCOM standards (with dots)
2. **Document Sources**: Consider adding source comments in the TOML file
3. **Version Control**: The `config/products.toml` file is tracked in git
4. **Validate Ranges**: Ensure percentages are 0-100, potential ≥ current
5. **Consistent Units**: Define weights in kg (converted to tonnes internally)
6. **Single Configuration**: All product parameters in one TOML file
7. **Validation First**: Always validate configuration before processing



## Parameter Versioning and Traceability

Parameters are versioned through the git repository, allowing:
- Historical tracking of parameter changes
- Reproducibility of past analyses
- Documentation of parameter evolution over time
- Comparison of results using different parameter sets

All parameter values are stored in the processed database alongside results, ensuring complete traceability of the analysis conditions used to generate any specific output.

## Related Documentation

- [Configuration Guide](configuration-guide.md) - Step-by-step instructions for setting up an analysis
- [Methodology](methodology.md) - How parameters are used in analysis
- [Database Schema - Processed](database-schema-processed.md) - Parameter table structures
- [Data Sources](data-sources.md) - Context for parameter selection
