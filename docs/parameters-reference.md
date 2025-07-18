# Parameters Reference Guide

## Purpose

This document serves as a metadata reference, providing detailed descriptions of all parameters used in CirQuant's analysis, their meanings, valid ranges, and data types. It explains WHAT each parameter represents and WHY it's important for the analysis.

For practical instructions on HOW to set up and configure an analysis, see the [Configuration Guide](configuration-guide.md).

## Overview

CirQuant uses two types of parameters:

1. **Research-based parameters** (in `config/products.toml`): Potential rates by circular strategy
2. **Data-driven parameters** (from databases): Current rates, material composition, weights

At runtime, research parameters are loaded from the TOML file into the `ANALYSIS_PARAMETERS` global variable. Data-driven parameters are fetched from statistical sources and stored in the database.

## Parameter Categories

### 1. Research-Based Parameters (Configuration File)

#### `potential_refurbishment_rates`
- **Type**: `Dict{String, Float64}`
- **Source**: `config/products.toml` - research-based assumptions
- **Purpose**: Defines achievable product refurbishment/reuse rates
- **Structure**:
  - Keys: Product codes (PRODCOM format without dots)
  - Values: Percentage values (0.0 to 100.0)
- **Material savings**: 100% (entire product reused)
- **Example**:
  ```julia
  "potential_refurbishment_rates" => Dict{String, Float64}(
      "28211330" => 20.0,  # Heat pumps refurbishment potential
      "27114000" => 10.0   # PV panels refurbishment potential
  )
  ```

#### `potential_recycling_rates`
- **Type**: `Dict{String, Float64}`
- **Source**: `config/products.toml` - research-based assumptions
- **Purpose**: Defines achievable recycling collection rates
- **Structure**: Same as potential_refurbishment_rates
- **Material savings**: Depends on material composition and recovery rates
- **Constraints**: Combined with refurbishment rate should not exceed 100%
- **Example**:
  ```julia
  "potential_recycling_rates" => Dict{String, Float64}(
      "28211330" => 50.0,  # Heat pumps recycling potential
      "27114000" => 75.0   # PV panels recycling potential
  )
  ```

### 2. Data-Driven Parameters (From Statistical Sources)

#### `current_collection_rates`
- **Type**: `Dict{String, Float64}` (by product × geo × year)
- **Source**: Eurostat waste statistics (env_waselee, env_wasbat, etc.)
- **Purpose**: Actual percentage of products collected for recycling
- **Data location**: Table `product_collection_rates_YYYY`
- **Note**: Refurbishment rates largely unavailable in official statistics

#### `material_composition`
- **Type**: DataFrame (product × material × percentage)
- **Source**: External databases (Ecodesign studies, LCA databases)
- **Purpose**: Material breakdown of products for recycling calculations
- **Data location**: Table `product_material_composition_YYYY`
- **Example structure**:
  - Product: "28211330" (heat pump)
  - Materials: steel (60%), copper (20%), aluminum (10%), plastics (10%)

#### `material_recycling_rates`
- **Type**: DataFrame (material × geo × year × rate)
- **Source**: Eurostat waste treatment statistics (env_wastrt)
- **Purpose**: Recovery rates for each material type
- **Data location**: Table `material_recycling_rates_YYYY`
- **Example**: Steel: 85-90%, Copper: 80-85%, Plastics: 10-30%

#### `product_weights_tonnes`
- **Type**: Dict{String, Float64} (by product × geo × year)
- **Source**: Calculated from PRODCOM quantity/value ratios
- **Purpose**: Convert piece counts to tonnes
- **Data location**: Table `product_average_weights_YYYY`
- **Calculation**: total_tonnes / total_pieces from PRODCOM data

### 3. Calculated Indicators

#### Material Recovery Rate (Calculated)
- **Type**: Float64 (per product)
- **Calculation**: Σ(material_weight% × material_recycling_rate%)
- **Purpose**: Actual material recovery when product is recycled
- **Example**: Heat pump with 60% steel (90% recovery) + 20% copper (85% recovery) = 71% overall recovery

#### Strategy-Specific Material Savings
- **Refurbishment**: 100% × product_weight × refurbishment_rate
- **Recycling**: material_recovery_rate × product_weight × collection_rate
- **Units**: Both tonnes and EUR (using production values)

## Parameter Sources and Basis

### 1. Research-Based Parameters (Configuration)

Potential rates in the configuration should be based on:
- **Refurbishment potential**: Technical feasibility studies, product lifetime extension research
- **Recycling potential**: Infrastructure assessments, collection system capabilities
- EU policy targets and circular economy action plans
- Best-in-class examples from leading regions/companies

### 2. Data-Driven Parameters (Statistical Sources)

Data parameters come from:
- **Current rates**: Eurostat waste statistics, national reporting
- **Material composition**: Ecodesign studies, LCA databases, industry declarations
- **Material recycling rates**: Waste treatment statistics, recovery facility data
- **Product weights**: PRODCOM production statistics (quantity/value ratios)

### 2. Parameter Processing

When loaded into the system, parameters undergo specific transformations:
- PRODCOM codes: Dots are removed for internal consistency (e.g., "28.21.13.30" → "28211330")
- Potential rates: Must be research-based, not arbitrary values
- Data parameters: Fetched and updated annually from statistical sources
- Validation: All values are checked against their defined ranges and constraints

### 3. Parameter Validation

When configuring parameters:
- Only potential rates go in config file
- Current rates and weights come from data sources
- Refurbishment + recycling potential should not exceed 100%
- Document research sources for potential rates
- Data parameters are updated automatically when fetched

## Database Storage

Parameters are stored in the processed database as separate tables:

1. **`parameters_potential_rates`**
   - Stores research-based potential rates from config
   - Includes product_code, potential_refurbishment_rate, potential_recycling_rate
   - Created from configuration file

2. **Data parameter tables** (Year-specific)
   - `product_collection_rates_YYYY`: Current collection rates from waste statistics
   - `product_material_composition_YYYY`: Material breakdown of products
   - `material_recycling_rates_YYYY`: Recovery rates by material type
   - `product_average_weights_YYYY`: Calculated from PRODCOM data

## Strategy-Specific Calculations

The enhanced framework calculates material savings by circular strategy:

### Material Recovery Calculation

```sql
-- For each product, calculate weighted material recovery rate
material_recovery_rate = SUM(
    material_composition.percentage * 
    material_recycling_rates.recovery_rate
)
```

### Refurbishment Material Savings

```sql
refurbishment_savings_tonnes = 
    potential_refurbishment_rate * 
    apparent_consumption_tonnes

refurbishment_savings_eur = 
    potential_refurbishment_rate * 
    apparent_consumption_value_eur
```

### Recycling Material Savings

```sql
recycling_savings_tonnes = 
    collection_rate * 
    material_recovery_rate * 
    apparent_consumption_tonnes

recycling_savings_eur = 
    collection_rate * 
    material_recovery_rate * 
    apparent_consumption_value_eur
```

### Key Differences

- **Refurbishment**: 100% material savings (entire product preserved)
- **Recycling**: Material-specific recovery rates applied
- **Data-driven**: Uses actual collection rates and material recovery data
- **Transparent**: Separates strategy contributions to total savings

## Best Practices

1. **Configuration File**: Only research-based potential rates
2. **Document Sources**: Add comments citing studies/reports for potential rates
3. **Data Fetching**: Let the system fetch current rates and weights from data
4. **Strategy Balance**: Ensure refurbishment + recycling ≤ 100%
5. **Material Data**: Wait for material composition data before fine-tuning rates
6. **Version Control**: Track configuration changes to document assumption evolution
7. **Validation First**: Always validate configuration before processing



## Parameter Versioning and Traceability

The dual approach (config + data) provides:
- **Research assumptions**: Tracked in git via config file
- **Current reality**: Updated from statistical sources
- **Gap analysis**: Difference shows improvement potential
- **Reproducibility**: Both assumptions and data stored in database

This separation ensures:
- Research-based targets remain stable
- Current performance tracked automatically
- Results show realistic improvement potential
- Full traceability of all parameters used

## Related Documentation

- [Configuration Guide](configuration-guide.md) - Step-by-step instructions for setting up an analysis
- [Methodology](methodology.md) - How parameters are used in analysis
- [Database Schema - Processed](database-schema-processed.md) - Parameter table structures
- [Data Sources](data-sources.md) - Context for parameter selection
