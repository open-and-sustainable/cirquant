# ![DatAdapt_logo](images/CirQuant_logo_vsmall.png) CirQuant

## Project Description
https://doi.org/10.17605/OSF.IO/U6SF3

## Documentation

Detailed documentation is available in the [`docs/`](docs/) folder:

- **[Methodology](docs/methodology.md)**: Research approach, theoretical framework, and analytical methods
- **[Data Sources](docs/data-sources.md)**: Details about PRODCOM and COMEXT datasets, indicators, and known issues
- **[Raw Database Schema](docs/database-schema-raw.md)**: Structure of the raw data tables (PRODCOM/COMEXT)
- **[Processed Database Schema](docs/database-schema-processed.md)**: Structure of the transformed analysis tables
- **[Parameters Reference](docs/parameters-reference.md)**: Guide to configuring ANALYSIS_PARAMETERS (metadata, structure, validation)

## Architecture Overview

CirQuant has been refactored to use the external ProdcomAPI package for fetching data from the Eurostat PRODCOM API. This separation of concerns allows for:

1. A cleaner codebase with focused responsibilities
2. The ability to use the ProdcomAPI package independently of CirQuant
3. Simplified maintenance and updates

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

## Usage

### Data Fetching

```julia
# Import the package
using CirQuant

# Fetch default datasets (ds-056120 and ds-056121) for years 2020-2021
fetch_prodcom_data("2020-2021")

# Fetch specific datasets
fetch_prodcom_data("2022-2022", ["ds-056120"])

# Fetch data without saving to database (for exploration)
df = CirQuant.fetch_prodcom_dataset("ds-056120", 2022)

# Get information about available datasets
datasets = CirQuant.get_available_datasets()
```

For a complete example, see `fetch_example.jl` in the repository.

## Data Transformation

The DataTransform module handles the processing of raw PRODCOM and COMEXT data into structured circularity indicators.

### Overview

The module provides:
- Creation of structured tables for circularity indicators (year-specific)
- Loading and using product conversion mappings between PRODCOM and HS codes
- Execution of PRQL queries to extract data from raw tables
- Transformation and combination of production and trade data
- Calculation of apparent consumption and circularity metrics

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

Circularity Indicators:
- `current_circularity_rate_pct`: % of material currently recirculated
- `potential_circularity_rate_pct`: % achievable with digital innovations
- `estimated_material_savings_tonnes`: Potential tonnes saved
- `estimated_monetary_savings_eur`: Estimated € saved

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

CirQuant uses a centralized `ANALYSIS_PARAMETERS` constant to manage all analysis parameters, avoiding hardcoded values and ensuring reproducibility. This constant is defined in the main CirQuant module and contains:

- **Circularity rates**: Current and potential material recirculation percentages
- **Product weights**: Conversion factors for different units (pieces to tonnes)
- **Trade parameters**: Intra-EU vs Extra-EU trade distribution assumptions
- **Recovery efficiency**: Material recovery rates by recycling method

These parameters are automatically stored in the processed database during transformation, creating parameter tables that:
- Enable exact reproduction of analysis results
- Track parameter changes over time
- Support scenario comparisons

Example of accessing parameters:
```julia
# View default analysis parameters
CirQuant.ANALYSIS_PARAMETERS

# Parameters are automatically used during processing
process_raw_to_processed()  # Uses ANALYSIS_PARAMETERS internally

# Parameter tables created in processed database:
# - parameters_circularity_rate
# - parameters_trade_share
# - parameters_recovery_efficiency
```

### Raw Database Structure

The raw database contains year-specific tables:
- PRODCOM: `prodcom_ds_056120_YYYY`, `prodcom_ds_056121_YYYY`
- COMEXT: `comext_DS_045409_YYYY`

Where `YYYY` is the 4-digit year (e.g., 2009).

### PRQL Query Support

The module uses PRQL (Pipelined Relational Query Language) for data extraction. PRQL files support year substitution using the `{{YEAR}}` placeholder, allowing flexible queries across different years.

For a complete processing example, see `examples/process_year_2009.jl`.

## Getting Started

1. Clone this repository
2. Install dependencies:
   ```julia
   using Pkg
   Pkg.activate(".")
   Pkg.instantiate()
   ```
3. Run the example script:
   ```
   julia fetch_example.jl
   ```

## License
The software in this repository is licensed under the [MIT license](LICENSE).

The resulting data is licensed under the [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) license because of constraints to commercial use of orginal data.
