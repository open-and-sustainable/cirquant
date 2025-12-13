# ![DatAdapt_logo](images/CirQuant_logo_vsmall.png) CirQuant

## Project Description

CirQuant aims to **quantify the potentials of circular economy** transitions for strategic product categories in the European Union by analyzing material flows, production patterns, and trade dynamics. The framework calculates potential material savings achievable through different circular strategies (refurbishment and recycling), taking into account material composition and material-specific recycling rates for more accurate assessments.

https://doi.org/10.17605/OSF.IO/U6SF3

## Documentation

Detailed documentation is available in the [`docs/`](docs/) folder:

- **[Methodology](docs/methodology.md)**: Research approach, theoretical framework, and analytical methods
- **[Configuration Guide](docs/configuration-guide.md)**: Step-by-step guide for analysts to setup an analysis and understand the configuration file format
- **[Data Sources](docs/data-sources.md)**: Details about PRODCOM and COMEXT datasets, indicators, and known issues
- **[Raw Database Schema](docs/database-schema-raw.md)**: Structure of the raw data tables (PRODCOM/COMEXT)
- **[Processed Database Schema](docs/database-schema-processed.md)**: Structure of the transformed analysis tables
- **[Parameters Reference](docs/parameters-reference.md)**: Metadata reference with detailed descriptions of parameter meanings and valid values

### Components:

- **ProdcomAPI**: External package that handles communication with the Eurostat PRODCOM API
- **ComextAPI**: External package that handles communication with the Eurostat COMEXT API
- **CirQuant**: This package, which focuses on:
  - Fetching data using ProdcomAPI and ComextAPI
  - Storing data in the raw DuckDB database
  - Transforming and analyzing the data
- **DataTransform Module**: Core transformation component that:
  - Creates year-specific circularity indicator tables
  - Executes PRQL queries for flexible data extraction
  - Maps between PRODCOM and HS product classifications
  - Calculates apparent consumption and circularity metrics

## Configuration

CirQuant uses a configuration file (`config/products.toml`) that serves two distinct purposes:

### 1. Analysis Scope Definition
- **Product selection**: Which products to include in the analysis
- **Code mappings**: PRODCOM codes (production) and HS codes (trade)
- **Physical properties**: Product weights for unit conversions

### 2. Research-Based Assumptions (Key Analysis Inputs)
- **Potential refurbishment rates**: Achievable product reuse percentages
- **Potential recycling rates**: Achievable collection and recovery rates
  - These are the critical assumptions that determine analysis outcomes
  - Should be based on: technical feasibility studies, policy targets, industry reports

The **potential rates by strategy are the core research inputs** - they directly determine the calculated material savings and policy implications.

### 3. Data-Driven Parameters (From Statistics)
- **Current collection/recycling rates**: From Eurostat waste statistics
- **Material composition**: Product material breakdown (forthcoming)
- **Material recycling rates**: Recovery rates by material type
- **Average product weights**: Calculated from PRODCOM data

Before using the system:

1. Review the product definitions in `config/products.toml`
2. Add or modify products as needed (see [Configuration Guide](docs/configuration-guide.md))
3. Validate the configuration:
   ```julia
   using CirQuant
   validate_product_config()
   ```

## Usage

### Complete Workflow Example

```julia
using CirQuant

# Step 1: Validate configuration
validate_product_config()

# Step 2: Fetch data for specific years
# Fetch all data sources (PRODCOM, COMEXT, and circular economy data)
fetch_combined_data("2020-2023")

# Or fetch individually:
# fetch_prodcom_data("2020-2023")  # Production data
# fetch_comext_data("2020-2023")   # Trade data
# fetch_material_composition_data("2020-2023")  # Material breakdown (stub)
# fetch_material_recycling_rates_data("2020-2023")  # Material recovery rates (stub)
# fetch_product_weights_data("2020-2023")  # Average weights from PRODCOM (stub)
# fetch_product_collection_rates_data("2020-2023")  # Collection rates (stub)

# Step 3: Process the fetched data
results = process_data("2020-2023")

# View processing results
println("Processed $(results[:processed_years]) years successfully")
```

### Data Fetching Options

```julia
# Fetch data for a single year
fetch_combined_data("2023")

# Fetch data for a range
fetch_combined_data("2020-2023")

# Fetch PRODCOM with specific datasets (default is ds-056120)
fetch_prodcom_data("2022-2023", ["ds-056120", "ds-056121"])

# Fetch COMEXT (uses HS codes from config/products.toml)
fetch_comext_data("2022-2023")
```

### Data Processing

```julia
# Process all available years (2002-2024)
process_data()

# Process specific years
process_data("2020-2023")

# Process with custom options
process_data("2022", cleanup_temp_tables=false, prql_timeout=600)

# Use test mode (processes only year 2002 from test database)
process_data(use_test_mode=true)
```

## Data Transformation

The DataTransform module handles the processing of raw PRODCOM and COMEXT data into structured circularity indicators.

### Overview

The module provides:
- Creation of structured tables for circularity indicators by strategy (year-specific)
- Loading and using product conversion mappings between PRODCOM and HS codes
- Execution of PRQL queries to extract data from raw tables
- Transformation and combination of production and trade data
- Integration of material composition and recycling rates (when available)
- Calculation of apparent consumption and strategy-specific material savings

### Data Structure

Each year's circularity indicators table contains:

**Dimensions (Identifiers):**
- `product_code`: PRODCOM or combined code
- `product_name`: Human-readable product label
- `year`: Reference year
- `geo`: Spatial level (EU country code or "EU27")
- `level`: "country" or "EU" aggregate

**Key Indicators:**

Production:
- `production_volume_tonnes`: Quantity produced (tonnes)
- `production_value_eur`: Production value (€)

Trade:
- `import_volume_tonnes`: Quantity imported (tonnes)
- `import_value_eur`: Value of imports (€)
- `export_volume_tonnes`: Quantity exported (tonnes)
- `export_value_eur`: Value of exports (€)

Apparent Consumption:
- `apparent_consumption_tonnes`: production + imports - exports
- `apparent_consumption_value_eur`: Monetary value of apparent consumption

Circularity Indicators by Strategy:
- **Refurbishment**:
  - `refurbishment_material_savings_tonnes`: Material saved through product reuse
  - `refurbishment_material_savings_eur`: Economic value of refurbishment
  - `refurbishment_production_reduction_tonnes`: Avoided new production
- **Recycling**:
  - `recycling_material_savings_tonnes`: Material recovered through recycling
  - `recycling_material_savings_eur`: Economic value of recycled materials
  - Material recovery based on composition and material-specific rates

### Usage Example

```julia
using CirQuant

# Process data for year 2009
year = 2009

# 1. Create circularity table for the year
success = create_circularity_table(year, db_path=DB_PATH_PROCESSED, replace=true)

# 2. Validate table structure
validation = validate_circularity_table(year, db_path=DB_PATH_PROCESSED)

# 3. Inspect raw data tables
table_info = inspect_raw_tables(DB_PATH_RAW, year)

# 4. Process data using PRQL queries
using CirQuant.CircularityProcessor

prql_files = Dict(
    "production" => "src/DataTransform/production_data.prql",
    "trade" => "src/DataTransform/trade_data.prql"
)

results = CircularityProcessor.process_year_data(
    year,
    raw_db_path=DB_PATH_RAW,
    processed_db_path=DB_PATH_PROCESSED,
    prql_files=prql_files,
    replace=true
)
```

### Parameter Management

CirQuant loads all analysis parameters from the `config/products.toml` configuration file. This includes:

- **Circularity rates**: Current and potential material recirculation percentages for each product
- **Product weights**: Conversion factors for different units (pieces to tonnes)

These parameters are:
- Loaded automatically when the module starts
- Validated to ensure completeness and consistency
- Stored in the processed database during transformation

Parameter tables created in processed database:
- `parameters_circularity_rate`: Product-specific circularity rates
- `parameters_recovery_efficiency`: Material recovery rates (if configured)

Configuration benefits:
- Enable exact reproduction of analysis results
- Track parameter changes through version control
- Support easy updates without code changes

### Raw Database Structure

The raw database contains year-specific tables:
- PRODCOM: `prodcom_ds_056120_YYYY`, `prodcom_ds_056121_YYYY`
- COMEXT: `comext_DS_045409_YYYY`

Where `YYYY` is the 4-digit year (e.g., 2009).

### PRQL Query Support

The module uses PRQL (Pipelined Relational Query Language) for data extraction. PRQL files support year substitution using the `{{YEAR}}` placeholder, allowing flexible queries across different years.



## Getting Started

1. Clone this repository
2. Install dependencies:
   ```julia
   using Pkg
   Pkg.activate(".")
   Pkg.instantiate()
   ```
3. Configure your products in `config/products.toml` (or use defaults)
4. Run a basic analysis:
   ```julia
   using CirQuant
   
   # Validate configuration
   validate_product_config()
   
   # Fetch and process one year of data
   fetch_combined_data("2023")
   process_data("2023")
   ```

## Configuration-Driven Analysis

The system automatically uses products defined in `config/products.toml`:

- **13 product categories** are pre-configured (heat pumps, PV panels, ICT equipment, batteries, etc.)
- **Automatic code mapping**: PRODCOM codes are used for production data, HS codes for trade data
- **Enhanced analysis approach**:
  - Distinguishes between refurbishment (100% material savings) and recycling strategies
  - Incorporates material composition for accurate recycling recovery rates
  - Uses data-driven weights and collection rates (when implemented)
- **Quantifying potentials by strategy**: Shows material savings opportunities from:
  - Increased refurbishment/reuse
  - Improved recycling collection and recovery
  - Better material separation and processing

**Key Point**: The analysis results depend on research-based potential rates for each circular strategy. The framework is designed to incorporate actual data on material composition, current collection rates, and material-specific recycling rates as they become available.

To add new products or modify parameters, see the [Configuration Guide](docs/configuration-guide.md).

## License
The software in this repository is licensed under the [MIT license](LICENSE).

The resulting data is licensed under the [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) license because of constraints to commercial use of orginal data.
