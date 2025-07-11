# Configuration Guide

## Purpose

This guide provides step-by-step instructions for analysts to set up and configure a CirQuant analysis. It explains the practical aspects of working with the configuration file, including how to add products, modify parameters, and validate your setup.

For detailed metadata about parameter meanings and valid values, see the [Parameters Reference](parameters-reference.md).

## Overview

CirQuant uses a centralized configuration file (`config/products.toml`) to manage all product definitions and analysis parameters. This guide explains how to configure products for circularity analysis.

## Configuration File Location

The main configuration file is located at:
```
cirquant/config/products.toml
```

This single file contains:
- Product definitions (ID, name, codes)
- Product-specific parameters (weight, circularity rates)
- All information needed for data fetching and processing

## File Structure

### Basic Product Definition

Each product is defined as a TOML section with the following structure:

```toml
[products.product_key]
id = 1                              # Unique integer ID
name = "Product Name"               # Human-readable name
prodcom_codes = ["XX.XX.XX.XX"]     # PRODCOM code(s) - currently one per product
hs_codes = ["XXXX.XX", "YYYY.YY"]   # HS codes - can have multiple

[products.product_key.parameters]
weight_kg = 100.0                   # Product weight in kilograms
unit = "piece"                      # Unit of measurement (typically "piece")
current_circularity_rate = 5.0      # Current recycling/reuse rate (0-100%)
potential_circularity_rate = 45.0   # Achievable rate with best practices (0-100%)
```

### Example Product Entry

```toml
[products.heat_pumps]
id = 1
name = "Heat pumps"
prodcom_codes = ["28.21.13.30"]
hs_codes = ["8418.69"]

[products.heat_pumps.parameters]
weight_kg = 100.0
unit = "piece"
current_circularity_rate = 5.0
potential_circularity_rate = 45.0
```

## Required Fields

### Product Level
- `id`: Unique integer identifier for the product
- `name`: Descriptive name of the product
- `prodcom_codes`: Array of PRODCOM codes (with dots, e.g., "28.21.13.30")
- `hs_codes`: Array of HS codes (can have multiple)

### Parameters Section
- `weight_kg`: Product weight in kilograms (positive number)
- `unit`: Unit of measurement (string, typically "piece")
- `current_circularity_rate`: Current material recirculation rate (0-100)
- `potential_circularity_rate`: Achievable rate with innovations (0-100)

## Adding a New Product

1. **Choose a unique key**: Use lowercase with underscores (e.g., `solar_inverters`)

2. **Assign a unique ID**: Check existing products and use the next available integer

3. **Add the product section**:
```toml
[products.solar_inverters]
id = 14
name = "Solar inverters"
prodcom_codes = ["27.11.50.00"]  # Must be valid PRODCOM code with dots
hs_codes = ["8504.40"]            # Must be valid HS code(s)

[products.solar_inverters.parameters]
weight_kg = 25.0                  # Research typical product weight
unit = "piece"
current_circularity_rate = 2.0    # Research current recycling rates
potential_circularity_rate = 40.0 # Based on material composition and technology
```

## Modifying Existing Products

To update product parameters:

1. **Locate the product** in `products.toml`
2. **Update the values** directly
3. **Save the file**
4. **Restart Julia** or reload the module to apply changes

Example: Updating circularity rates
```toml
[products.pv_panels.parameters]
weight_kg = 20.0
unit = "piece"
current_circularity_rate = 5.0      # Updated from 3.0
potential_circularity_rate = 70.0   # Updated from 65.0
```

## Validation

The system automatically validates the configuration when processing starts. To manually validate:

```julia
using CirQuant
AnalysisConfigLoader.validate_product_config()
```

### Validation Checks

1. **Required fields**: All fields listed above must be present
2. **Data types**: 
   - IDs must be integers
   - Weights and rates must be numbers
   - Names and codes must be strings
3. **Value ranges**:
   - Weights must be positive
   - Circularity rates must be 0-100%
   - Potential rate must be ≥ current rate
4. **Uniqueness**:
   - Product IDs must be unique
   - PRODCOM codes should not duplicate

### Common Validation Errors

```
ERROR: Product 'heat_pumps' missing parameter: weight_kg
```
**Solution**: Add the missing parameter to the product's parameters section

```
ERROR: Product 'batteries' potential_circularity_rate (25.0) must be >= current_circularity_rate (30.0)
```
**Solution**: Ensure potential rate is higher than current rate

```
ERROR: Duplicate product ID found: 5
```
**Solution**: Assign a unique ID to each product

## How Configuration is Used

### Data Fetching
- PRODCOM codes are used to fetch production data from Eurostat
- HS codes are used to fetch trade data (imports/exports)

### Data Processing
- Product weights convert piece counts to tonnes
- Circularity rates calculate material savings potential
- Product names provide human-readable labels in outputs

### Analysis Parameters
The configuration automatically populates:
- `current_circularity_rates` dictionary
- `potential_circularity_rates` dictionary  
- `product_weights_tonnes` dictionary

## Best Practices

1. **Code Format**:
   - PRODCOM codes: Include dots (e.g., "27.11.40.00")
   - HS codes: Can include dots or not (e.g., "8541.43" or "854143")

2. **Weight Estimates**:
   - Research typical product weights
   - Use manufacturer specifications when available
   - Document sources in comments if needed

3. **Circularity Rates**:
   - Base on published studies or regulations
   - Current rate: What's achieved today
   - Potential rate: Best-case with current technology

4. **Product Keys**:
   - Use descriptive, lowercase names
   - Separate words with underscores
   - Keep consistent with product category

## Important Notes

### Single PRODCOM Code Limitation
Currently, each product must have exactly ONE PRODCOM code in the array. While the structure supports multiple codes, the system expects one code per product.

### HS Code Handling
Products can have multiple HS codes. These are stored as comma-separated values and properly handled during data fetching.

### No Special Entries
Only actual products should be in the configuration. Do not add:
- Prefix codes (like "2720" for batteries category)
- Component entries (like "battery_cell")
- Aggregate categories (except if they have valid PRODCOM codes like "26.20")

## Troubleshooting

### Configuration not loading
```julia
# Check file location
isfile("config/products.toml")

# Validate configuration
AnalysisConfigLoader.validate_product_config()
```

### Changes not taking effect
- Restart Julia session after modifying products.toml
- Or reload the module: `using CirQuant`

### Data fetching issues
- Verify PRODCOM codes are valid and include dots
- Check HS codes match Eurostat's format
- Ensure products are properly defined before fetching

## Example: Complete Product Addition Workflow

1. **Research the product**:
   - Find PRODCOM code from Eurostat
   - Identify corresponding HS codes
   - Research typical weight and circularity data

2. **Add to configuration**:
```toml
[products.electric_motors]
id = 15
name = "Electric motors"
prodcom_codes = ["27.11.10.00"]
hs_codes = ["8501.10", "8501.20"]

[products.electric_motors.parameters]
weight_kg = 50.0
unit = "piece"
current_circularity_rate = 15.0
potential_circularity_rate = 60.0
```

3. **Validate**:
```julia
using CirQuant
AnalysisConfigLoader.validate_product_config()
```

4. **Update database**:
```julia
write_product_conversion_table()
```

5. **Fetch data**:
```julia
fetch_prodcom_data("2023-2023")
fetch_comext_data("2023-2023")
```

## Related Documentation

- [Parameters Reference](parameters-reference.md) - Metadata reference with detailed descriptions of what each parameter means
- [Data Sources](data-sources.md) - Information about PRODCOM and HS codes
- [Methodology](methodology.md) - How parameters are used in analysis