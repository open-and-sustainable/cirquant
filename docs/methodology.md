# CirQuant Methodology

## Research Overview

CirQuant (Circular Economy Quantification) is a data processing and analysis framework designed to measure and track circular economy indicators for strategic product categories in the European Union. The project focuses on quantifying potential material flows, production patterns, and trade dynamics to assess potentials of the circular economy transition.

## Theoretical Framework

### Circular Economy Metrics

The circular economy aims to minimize waste and maximize resource efficiency through:
- Reducing raw material consumption
- Reusing products and components
- Recycling materials at end-of-life
- Recovering energy from non-recyclable waste

To measure progress, we need quantitative indicators that capture:
1. **Material flows**: How much is produced, imported, exported, and consumed
2. **Resource efficiency**: Value generated per unit of material
3. **Trade patterns**: Dependencies on external markets for materials and products
4. **Temporal trends**: Changes over time indicating transition progress

### Product Selection Rationale

The methodology focuses on key product categories that are:

1. **Strategically important** for EU industrial policy
2. **Resource-intensive** in production or use
3. **Subject to circular economy regulations** (batteries, electronics)
4. **Critical for green transition** (heat pumps, solar panels)

Selected categories include:
- **Heat pumps & refrigeration**: Essential for energy transition
- **Photovoltaic equipment**: Renewable energy infrastructure
- **ICT equipment**: High material value, rapid replacement cycles
- **Batteries**: Critical materials, recycling potential

## Data Integration Approach

### Combining PRODCOM and COMEXT

The methodology integrates two complementary data sources:

1. **PRODCOM (Production Statistics)**
   - Provides: Production volumes and values within the EU
   - Granularity: 8-10 digit product codes
   - Coverage: Individual products manufactured in EU facilities

2. **COMEXT (Trade Statistics)**
   - Provides: Import/export flows between EU and world
   - Granularity: 6-digit HS codes
   - Coverage: All traded goods crossing EU borders

### Harmonization Strategy

Product codes are mapped between systems:
- PRODCOM codes (e.g., "27.11.40.00") → HS codes (e.g., "8541.43")
- Many-to-many relationships handled through mapping tables

### Country Code Harmonization

The system addresses differences in geographic coding between data sources:
- **PRODCOM**: Uses numeric codes (e.g., "001" for France, "004" for Germany)
- **COMEXT**: Uses ISO 2-letter codes (e.g., "FR" for France, "DE" for Germany)

A country code mapping table is created to harmonize these differences, ensuring data from both sources can be properly merged by geography.

### Trade Data Consolidation

When processing trade statistics, both PRODCOM and COMEXT contain import/export data:
- **PRODCOM indicators**: IMPVAL, EXPVAL (values), IMPQNT, EXPQNT (quantities)
- **COMEXT indicators**: VALUE_EUR, QUANTITY_KG for flows 1 (imports) and 2 (exports)

The system prioritizes data sources as follows:
1. **Primary source**: COMEXT data is used for all trade statistics when available
2. **Fallback**: PRODCOM trade indicators are only used when COMEXT data is missing for specific product/year combinations

This approach ensures consistency and avoids potential discrepancies between the two sources, as COMEXT provides more comprehensive coverage at the HS code level.

## Analytical Framework

### Core Indicators

1. **Production Metrics**
   - Annual production volume (PRODQNT)
   - Production value (PRODVAL)
   - Unit values (PRODVAL/PRODQNT)

2. **Trade Balance**
   - Import volumes and values (IMPQNT, IMPVAL)
   - Export volumes and values (EXPQNT, EXPVAL)
   - Net trade position (exports - imports)

3. **Apparent Consumption**
   - Formula: Production + Imports - Exports
   - Indicates domestic material use

4. **Trade Intensity**
   - Intra-EU trade (INT_EU27_2020)
   - Extra-EU trade (EXT_EU27_2020)
   - Dependency ratios

## Data Processing Pipeline

### 1. Data Acquisition

Automated fetching from Eurostat APIs with:
- Rate limiting
- Error handling

### 2. Data Cleaning

- Unit harmonization (converting all quantities to tonnes where possible)
- Missing value handling
- Consistency checks between related indicators

### 3. Data Storage - Raw Database

- **Purpose**: Store fetched data exactly as received from APIs
- **Technology**: DuckDB for efficient analytical queries
- **Structure**: Separate tables for each dataset and year
  - PRODCOM: `prodcom_ds_XXXXXX_YYYY`
  - COMEXT: `comext_ds_XXXXXX_YYYY`
- **Data types**: String storage for mixed data types (values and units)

### 4. Data Transformation - PRQL Processing

- **Purpose**: Transform raw data into analysis-ready format
- **Technology**: PRQL (Pipelined Relational Query Language)
- **Process**:
  - Read from raw database tables
  - Apply product mappings (PRODCOM ↔ HS codes)
  - Harmonize units (convert to tonnes where possible)
  - Calculate derived indicators
  - Handle missing values and data quality issues

### 5. Data Storage - Processed Database

- **Purpose**: Store transformed, analysis-ready data
- **Structure**: Unified tables with consistent schema
  - Annual circularity indicator tables
  - Product mapping reference tables
  - Aggregated metrics by country/product
- **Benefits**:
  - Consistent units across products
  - Pre-calculated indicators
  - Optimized for analytical queries

### 6. Analysis Generation

- SQL queries on processed database
- Indicator calculations
- Export to various formats for reporting

## Parameter Management

### ANALYSIS_PARAMETERS Global Variable

To avoid hardcoding values and ensure reproducibility, the system uses a centralized `ANALYSIS_PARAMETERS` global variable in the CirQuant module. This variable is populated at module initialization by loading parameters from the `config/products.toml` file. This approach provides:

1. **Centralized configuration**: All analysis parameters in one place
2. **Traceability**: Parameters are stored in the processed database
3. **Flexibility**: Easy to update parameters without modifying code
4. **Transparency**: Clear record of assumptions used in analysis

The configuration dictionary contains:

- **Current circularity rates**: Existing material recirculation percentages by product (each product has its own explicit rate)
- **Potential circularity rates**: Achievable rates with best practices/innovations (each product has its own explicit rate)
- **Product weights**: Conversion factors from kilograms to tonnes for each product

### Parameter Storage in Database

During processing, these parameters are automatically stored in the processed database as parameter tables:
- **`parameters_circularity_rate`**: Contains product-specific circularity rates for each product (always created)
- **`parameters_recovery_efficiency`**: Contains recovery efficiency rates by method (only created if recovery_efficiency parameters are provided in the configuration)

This ensures that:
- Analysis results can be reproduced exactly
- Parameter changes are tracked over time
- Different scenarios can be compared

### Parameter Usage in PRQL Queries

The parameters are accessed within PRQL transformation queries through joins with parameter tables. For example, the `update_circularity_parameters.prql` query:

```prql
from ci = circularity_indicators_{{YEAR}}
join pcr = parameters_circularity_rate (ci.product_code == pcr.product_code)
derive {
    current_circularity_rate_pct = pcr.current_circularity_rate,
    potential_circularity_rate_pct = pcr.potential_circularity_rate,
    estimated_material_savings_tonnes = apparent_consumption_tonnes *
        (pcr.potential_circularity_rate - pcr.current_circularity_rate) / 100.0
}
```

This approach allows dynamic recalculation of indicators when parameters are updated.

### Configuration File System

As of the latest version, CirQuant has moved from hardcoded parameters to a configuration-driven approach:

1. **Central Configuration File**: All product definitions and parameters are stored in `config/products.toml`
2. **Automatic Loading**: Parameters are loaded at module initialization using `AnalysisConfigLoader`
3. **Validation**: Configuration is validated before any data processing begins
4. **Flexibility**: Products can be added, modified, or removed without changing code

The configuration file structure:
```toml
[products.product_key]
id = 1
name = "Product Name"
prodcom_codes = ["XX.XX.XX.XX"]
hs_codes = ["XXXX.XX"]

[products.product_key.parameters]
weight_kg = 100.0
unit = "piece"
current_circularity_rate = 5.0
potential_circularity_rate = 45.0
```

This approach provides:
- **Separation of concerns**: Configuration separate from code
- **Easy maintenance**: Non-technical users can update parameters
- **Version control**: Changes to parameters are tracked in git
- **Validation**: Automatic checking for completeness and consistency

For detailed configuration instructions, see the [Configuration Guide](configuration-guide.md).

## Limitations and Assumptions

### Data Limitations

1. **Confidentiality**: Some production data suppressed for business confidentiality
2. **Coverage gaps**: Not all EU countries report all products
3. **Classification changes**: Product codes evolve over time
4. **Unit heterogeneity**: Different products use different quantity units

### Methodological Assumptions

1. **Product homogeneity**: Products within a code are treated as equivalent
2. **Trade attribution**: Imports/exports assigned to reporter country
3. **Production location**: Assumed to occur in reporting country
4. **Time consistency**: Annual data represents full-year activity

## Quality Assurance

### Validation Checks

1. **Cross-source validation**: Compare PRODCOM trade data with COMEXT
2. **Temporal consistency**: Flag unusual year-over-year changes
3. **Balance checks**: Ensure value/quantity relationships are plausible
4. **Completeness monitoring**: Track data availability by country/product/year

### Transparency Measures

- All data sources clearly documented
- Processing steps reproducible through PRQL scripts
- Database schemas documented separately
- Assumptions explicitly stated
- Limitations acknowledged

## Database Architecture

The two-database approach ensures:
1. **Raw database**: Preserves original data for traceability
2. **Processed database**: Provides clean data for analysis

Detailed database structures are documented in:
- [Raw Database Schema](database-schema-raw.md)
- [Processed Database Schema](database-schema-processed.md)



## References

- Eurostat PRODCOM Methodology: [link]
- Eurostat COMEXT User Guide: [link]
- EU Circular Economy Action Plan: [link]
- Product Classification Correspondence Tables: [link]
