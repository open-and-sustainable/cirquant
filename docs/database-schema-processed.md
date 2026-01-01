---
title: Processed Database Schema
nav_order: 7
---

# Processed Database Schema

## Overview

The processed database contains transformed and analysis-ready data derived from the raw database through PRQL queries. All data is harmonized, with consistent units and calculated indicators.

**Database**: `CirQuant-database/processed/CirQuant_2010-2024.duckdb`

## Table Types

### Persistent Tables
These tables remain in the database after processing:
- `circularity_indicators_YYYY` - Main analysis results
- `production_trade_YYYY` - Combined production and trade data with PRODCOM fallback
- `country_aggregates_YYYY` - Pre-calculated country-level aggregates
- `product_aggregates_YYYY` - Pre-calculated product-level EU aggregates
- `product_unit_values_YYYY` - EUR per kg / per unit by flow using product weights and values
- `country_code_mapping` - PRODCOM to ISO country code mappings
- `parameters_circularity_rate` - Product-specific circularity parameters
- `product_mapping_codes` - PRODCOM to HS code mappings
- `product_weights_YYYY` - Config weights plus derived mass/counts from available data
- `product_collection_rates_YYYY` - Product collection rates derived from WEEE datasets
- `product_material_composition_YYYY` - UMP-derived material composition by product
- `material_recycling_rates_YYYY` - UMP-derived material recovery rates by WEEE category
- `product_material_recovery_rates_YYYY` - Material-weighted recovery rates by product
- `circularity_indicators_by_strategy_YYYY` - Strategy-specific circularity indicators

### Temporary Tables
These tables are created during processing and removed in step 9:
- `prodcom_converted_YYYY` - Unit-converted PRODCOM data
- `production_temp_YYYY` - Intermediate production data
- `trade_temp_YYYY` - Intermediate trade data
- `production_trade_harmonized_YYYY` - Intermediate harmonized data before PRODCOM fallback

## Core Tables

### Table: `circularity_indicators_YYYY`

Annual circularity indicators combining production and trade data.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM or combined product code |
| product_name | VARCHAR | Human-readable product label |
| year | INTEGER | Reference year |
| geo | VARCHAR | Country code or "EU27" |
| level | VARCHAR | "country" or "EU" aggregate |
| production_volume_tonnes | DOUBLE | Production quantity in tonnes |
| production_value_eur | DOUBLE | Production value in EUR |
| import_volume_tonnes | DOUBLE | Import quantity in tonnes |
| import_value_eur | DOUBLE | Import value in EUR |
| export_volume_tonnes | DOUBLE | Export quantity in tonnes |
| export_value_eur | DOUBLE | Export value in EUR |
| apparent_consumption_tonnes | DOUBLE | Production + Imports - Exports |
| apparent_consumption_value_eur | DOUBLE | Monetary value of apparent consumption |
| current_circularity_rate_pct | DOUBLE | Current material recirculation rate (%) |
| potential_circularity_rate_pct | DOUBLE | Potential rate using uplift mean (%) |
| potential_circularity_rate_pct_ci_lower | DOUBLE | Potential rate using uplift CI lower (%) |
| potential_circularity_rate_pct_ci_upper | DOUBLE | Potential rate using uplift CI upper (%) |
| collection_rate_pct | DOUBLE | Collection rate (%) used for recycling savings |
| material_recovery_rate_pct | DOUBLE | Material recovery rate (%) used for recycling savings |
| estimated_material_savings_tonnes | DOUBLE | Potential material savings |
| estimated_monetary_savings_eur | DOUBLE | Estimated monetary savings |
| estimated_material_savings_tonnes_ci_lower | DOUBLE | Potential material savings (CI lower) |
| estimated_material_savings_tonnes_ci_upper | DOUBLE | Potential material savings (CI upper) |
| estimated_monetary_savings_eur_ci_lower | DOUBLE | Estimated monetary savings (CI lower) |
| estimated_monetary_savings_eur_ci_upper | DOUBLE | Estimated monetary savings (CI upper) |
| current_recycling_savings_tonnes | DOUBLE | Current recycling material savings (tonnes) |
| current_recycling_savings_eur | DOUBLE | Current recycling material savings (EUR) |

### Table: `production_trade_YYYY`

Combined production and trade data with PRODCOM fallback applied.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots |
| year | INTEGER | Reference year |
| geo | VARCHAR | ISO 2-letter country code or "EU27" |
| level | VARCHAR | "country" or "EU" aggregate |
| production_volume_tonnes | DOUBLE | Production quantity in tonnes |
| production_value_eur | DOUBLE | Production value in EUR |
| import_volume_tonnes | DOUBLE | Import quantity in tonnes (COMEXT or PRODCOM fallback) |
| import_value_eur | DOUBLE | Import value in EUR (COMEXT or PRODCOM fallback) |
| export_volume_tonnes | DOUBLE | Export quantity in tonnes (COMEXT or PRODCOM fallback) |
| export_value_eur | DOUBLE | Export value in EUR (COMEXT or PRODCOM fallback) |

### Table: `product_mapping_codes`

Mapping between PRODCOM and HS classification systems.

| Column | Type | Description |
|--------|------|-------------|
| product_id | INTEGER | Unique identifier |
| product | VARCHAR | Product category name |
| prodcom_code | VARCHAR | PRODCOM code (with dots, e.g., "27.11.40.00") |
| hs_codes | VARCHAR | Comma-separated HS codes (e.g., "8541.43") |

### Table: `country_code_mapping`

Mapping between PRODCOM numeric country codes and ISO 2-letter codes.

| Column | Type | Description |
|--------|------|-------------|
| prodcom_code | VARCHAR | PRODCOM numeric code (e.g., "001" for France) |
| iso_code | VARCHAR | ISO 2-letter code (e.g., "FR" for France) |
| country_name | VARCHAR | Full country name |

### Table: `prodcom_unit_conversions`

Conversion factors for harmonizing different units to tonnes.

| Column | Type | Description |
|--------|------|-------------|
| prodcom_code | VARCHAR | PRODCOM code |
| original_unit | VARCHAR | Original unit (KG, L, M3, etc.) |
| conversion_factor | DOUBLE | Factor to convert to tonnes |
| conversion_method | VARCHAR | Method used (direct, density-based, average) |
| notes | VARCHAR | Additional conversion notes |

### Table: `country_aggregates_YYYY`

Pre-calculated country-level aggregates for performance.

| Column | Type | Description |
|--------|------|-------------|
| geo | VARCHAR | Country code |
| year | INTEGER | Reference year |
| total_production_tonnes | DOUBLE | Total production across products |
| total_production_value_eur | DOUBLE | Total production value |
| total_imports_tonnes | DOUBLE | Total imports |
| total_imports_value_eur | DOUBLE | Total import value |
| total_exports_tonnes | DOUBLE | Total exports |
| total_exports_value_eur | DOUBLE | Total export value |
| trade_balance_tonnes | DOUBLE | Exports - Imports |
| trade_balance_value_eur | DOUBLE | Export value - Import value |

### Table: `product_aggregates_YYYY`

Pre-calculated product-level EU aggregates.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | Product code |
| product_name | VARCHAR | Product name |
| year | INTEGER | Reference year |
| eu_production_tonnes | DOUBLE | Total EU production |
| eu_imports_intra_tonnes | DOUBLE | Intra-EU imports |
| eu_imports_extra_tonnes | DOUBLE | Extra-EU imports |
| eu_exports_intra_tonnes | DOUBLE | Intra-EU exports |
| eu_exports_extra_tonnes | DOUBLE | Extra-EU exports |
| eu_apparent_consumption_tonnes | DOUBLE | EU-wide apparent consumption |

### Table: `product_weights_YYYY`

Config weights combined with any observed/derived mass and counts.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots |
| geo | VARCHAR | ISO 2-letter country code |
| year | INTEGER | Reference year |
| weight_kg_config | DOUBLE | Fallback/config weight per unit (kg) |
| total_mass_tonnes | DOUBLE | Mass derived or observed (tonnes); may be missing if not derivable |
| unit_counts | DOUBLE | Unit counts observed or derived; may be missing if not derivable |
| source | VARCHAR | How the row was built (`prodcom_counts_config_mass`, `comext_mass_config_counts`, `combined`, `config`) |

### Table: `product_collection_rates_YYYY`

Collection rates derived from Eurostat WEEE datasets (`env_waselee` / `env_waseleeos`).

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots |
| year | INTEGER | Reference year |
| geo | VARCHAR | Geography code from WEEE dataset |
| collection_rate_pct | DOUBLE | Collection rate (%) |
| source | VARCHAR | Raw table name used (`env_waselee_YYYY` or `env_waseleeos_YYYY`) |

### Table: `product_material_composition_YYYY`

Material composition by product and material derived from UMP sankey flows.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots |
| year | INTEGER | Reference year |
| geo | VARCHAR | Geography code (currently `EU27_2020`) |
| material | VARCHAR | Material label from UMP sankey (`layer_4`) |
| material_mass_mg | DOUBLE | Material mass (Mg) aggregated from UMP flows |
| product_mass_mg | DOUBLE | Total product mass (Mg) for the product |
| material_weight_pct | DOUBLE | Material share by mass (%) |
| source | VARCHAR | `UMP_sankey` |

### Table: `material_recycling_rates_YYYY`

Material recovery rates by WEEE category derived from UMP sankey flows.

| Column | Type | Description |
|--------|------|-------------|
| year | INTEGER | Reference year |
| weee_category | VARCHAR | UMP WEEE category (e.g., `WEEE_Cat1`) |
| material | VARCHAR | Material label from UMP sankey (`layer_4`) |
| recovered_mass_mg | DOUBLE | Recovered mass (Mg) |
| lost_mass_mg | DOUBLE | Loss mass (Mg) from landfill/dissipation |
| recovery_rate_pct | DOUBLE | Recovery rate (%) |
| geo | VARCHAR | Geography code (currently `EU27_2020`) |
| source | VARCHAR | `UMP_sankey` |

### Table: `product_material_recovery_rates_YYYY`

Material-weighted recovery rates by product.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots |
| year | INTEGER | Reference year |
| geo | VARCHAR | Geography code (currently `EU27_2020`) |
| material_recovery_rate_pct | DOUBLE | Material-weighted recovery rate (%) |
| source | VARCHAR | `UMP_sankey` |

### Table: `product_unit_values_YYYY`

Value-per-unit and value-per-kg metrics for production, imports, and exports using processed values plus product weights.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots |
| geo | VARCHAR | ISO 2-letter country code |
| year | INTEGER | Reference year |
| flow | VARCHAR | One of `production`, `import`, `export` |
| value_eur | DOUBLE | Monetary value for the flow |
| mass_tonnes | DOUBLE | Mass for the flow (tonnes) |
| unit_counts | DOUBLE | Unit counts (observed from product_weights or derived via weight) |
| value_per_unit_eur | DOUBLE | EUR per unit (value / unit_counts) |
| value_per_kg_eur | DOUBLE | EUR per kg (value / mass_kg) |
| source | VARCHAR | How counts were obtained (`counts_from_product_weights`, `derived_from_weight`, `mass_only`, `value_only`) |

## Parameter Tables

These tables store the analysis parameters used during processing, ensuring reproducibility and traceability.

### Table: `parameters_circularity_rate`

Stores product-specific circularity rate assumptions used in calculations.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots (e.g., "28211330") |
| circularity_uplift_mean | DOUBLE | Global uplift mean applied to current rate (%) |
| circularity_uplift_sd | DOUBLE | Global uplift standard deviation (percentage points) |
| circularity_uplift_ci_lower | DOUBLE | Global uplift CI lower bound (percentage points) |
| circularity_uplift_ci_upper | DOUBLE | Global uplift CI upper bound (percentage points) |
| current_refurbishment_rate | DOUBLE | Current refurbishment rate for this product (%) |
| last_updated | VARCHAR | Timestamp of last parameter update |

### Table: `circularity_indicators_by_strategy_YYYY`

Strategy-specific circularity indicators for refurbishment and recycling.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots |
| year | INTEGER | Reference year |
| geo | VARCHAR | Country code or "EU27" |
| level | VARCHAR | "country" or "EU" aggregate |
| strategy | VARCHAR | `refurbishment` or `recycling` |
| rate_pct | DOUBLE | Strategy rate (%) used for savings |
| material_recovery_rate_pct | DOUBLE | Material recovery rate (%) for recycling |
| apparent_consumption_tonnes | DOUBLE | Apparent consumption (tonnes) |
| apparent_consumption_value_eur | DOUBLE | Apparent consumption (EUR) |
| material_savings_tonnes | DOUBLE | Strategy material savings (tonnes) |
| material_savings_eur | DOUBLE | Strategy material savings (EUR) |
| production_reduction_tonnes | DOUBLE | Avoided production (tonnes) |
| production_reduction_eur | DOUBLE | Avoided production (EUR) |

### Table: `parameters_recovery_efficiency` (Optional)

Material recovery efficiency rates by recycling method. This table is only created if `recovery_efficiency` parameters are provided in the configuration file.

| Column | Type | Description |
|--------|------|-------------|
| method | VARCHAR | Recycling/recovery method name |
| efficiency_rate | DOUBLE | Recovery efficiency (0-1) |
| material_type | VARCHAR | Type of material (optional) |
| notes | VARCHAR | Additional method details |

## Data Transformations Applied

### Unit Harmonization
- All quantities converted to tonnes where possible
- Conversion factors applied based on product type:
  - Direct conversion (KG → tonnes: ÷1000)
  - Density-based (L → tonnes using product-specific density)
  - Standard assumptions for piece counts

### Value Calculations
- All monetary values in EUR
- Missing values handled as zero values

### Derived Indicators
1. **Apparent Consumption** = Production + Imports - Exports
2. **Trade Balance** = Exports - Imports
3. **Import Dependency** = Imports / (Production + Imports)
4. **Export Intensity** = Exports / Production

### Geographic Aggregations
- Country-level data preserved from source
- EU27 aggregates reported by EUROSTAT and not computed as sum because individual may be omitted because of condifentiality

## PRQL Transformation Process

The transformation from raw to processed involves:

1. **Product Mapping**: Join PRODCOM and COMEXT data using mapping table
2. **Unit Conversion**: Apply conversion factors to standardize units
3. **Aggregation**: Sum values by product/country/year
4. **Indicator Calculation**: Compute derived metrics
5. **Quality Checks**: Flag suspicious values or gaps

Example PRQL transformation:
```prql
from prodcom_raw
join product_mapping (==prodcom_code)
derive tonnes = case [
  unit == "KG" => value / 1000,
  unit == "T" => value,
  true => null
]
group {product_code, geo, year} (
  aggregate {
    production_tonnes = sum tonnes,
    production_value = sum value_eur
  }
)
```

## Usage Notes

1. **Time Coverage**:
   - Production data: 1995-2023 (where available)
   - Trade data: 2010-2024 (current dataset scope)
   - Combined indicators: 2010-2024

2. **Missing Data**:
   - NULL indicates no data available
   - 0 indicates reported zero value
