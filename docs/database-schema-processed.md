# Processed Database Schema

## Overview

The processed database contains transformed and analysis-ready data derived from the raw database through PRQL queries. All data is harmonized, with consistent units and calculated indicators.

**Database**: `CirQuant-database/processed/CirQuant_2002-2023.duckdb`

## Table Types

### Persistent Tables
These tables remain in the database after processing:
- `circularity_indicators_YYYY` - Main analysis results
- `production_trade_YYYY` - Combined production and trade data with PRODCOM fallback
- `country_aggregates_YYYY` - Pre-calculated country-level aggregates
- `product_aggregates_YYYY` - Pre-calculated product-level EU aggregates
- `country_code_mapping` - PRODCOM to ISO country code mappings
- `parameters_circularity_rate` - Product-specific circularity parameters
- `product_mapping_codes` - PRODCOM to HS code mappings
- `product_average_weights_YYYY` - Data-driven product weights derived from PRODCOM

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
| potential_circularity_rate_pct | DOUBLE | Achievable rate with innovations (%) |
| estimated_material_savings_tonnes | DOUBLE | Potential material savings |
| estimated_monetary_savings_eur | DOUBLE | Estimated monetary savings |

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

### Table: `product_average_weights_YYYY`

Data-driven average weight per product and geography, calculated from PRODCOM production quantities expressed in tonnes and in pieces.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots |
| geo | VARCHAR | ISO 2-letter country code or "EU27_2020" for EU aggregate |
| year | INTEGER | Reference year |
| average_weight_kg | DOUBLE | Calculated average weight per unit (kg) |
| tonnes_observed | DOUBLE | Total tonnes observed in the PRODCOM data used for the calculation |
| units_observed | DOUBLE | Total unit count observed (pieces) |
| calculation_date | VARCHAR | Timestamp when the weight calculation was executed |

## Parameter Tables

These tables store the analysis parameters used during processing, ensuring reproducibility and traceability.

### Table: `parameters_circularity_rate`

Stores product-specific circularity rate assumptions used in calculations.

| Column | Type | Description |
|--------|------|-------------|
| product_code | VARCHAR | PRODCOM code without dots (e.g., "28211330") |
| current_circularity_rate | DOUBLE | Current material recirculation rate for this product (%) |
| potential_circularity_rate | DOUBLE | Achievable rate with innovations for this product (%) |
| last_updated | VARCHAR | Timestamp of last parameter update |

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
   - Trade data: 2002-2023 (COMEXT limitation)
   - Combined indicators: 2002-2023

2. **Missing Data**:
   - NULL indicates no data available
   - 0 indicates reported zero value
