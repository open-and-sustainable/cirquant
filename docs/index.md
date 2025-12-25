---
title: Home
nav_order: 1
---

# CirQuant Documentation

CirQuant quantifies the **circular economy potential** of strategic product categories in the European Union by combining production (PRODCOM), trade (COMEXT), and circularity parameters. The framework estimates material savings from refurbishment and recycling strategies using product-level assumptions on weights, material composition, and achievable recovery rates. The project DOI is https://doi.org/10.17605/OSF.IO/U6SF3 and the published documentation will be available at http://equicirco.github.io/cirquant.

## Purpose and Approach

- **Objective**: Measure how far Europe can reduce raw material demand by refurbishing or recycling key product groups (heat pumps, PV panels, ICT equipment, batteries, etc.).
- **Analytical flow**: Configure product scope → fetch statistics → transform them into harmonised indicator tables → quantify strategy-specific material savings.
- **Components**:
  - **ProdcomAPI** and **ComextAPI** (external packages) fetch Eurostat data.
  - **CirQuant** orchestrates validation, data ingestion, and transformation.
  - **DataTransform** creates year-specific circularity tables via PRQL, maps PRODCOM↔HS codes, and computes apparent consumption plus strategy metrics.

## Documentation Map

All references live in this documentation set:
- [Methodology](methodology.md) – research framing, scope definition, and analytical logic.
- [Configuration Guide](configuration-guide.md) – how to tailor `config/products.toml`.
- [Data Sources](data-sources.md) – PRODCOM/COMEXT coverage, indicators, caveats.
- [Raw Database Schema](database-schema-raw.md) and [Processed Schema](database-schema-processed.md) – DuckDB layouts used across the workflow.
- [Parameters Reference](parameters-reference.md) – meanings and valid ranges of each configurable field.

Use these documents for deeper dives; this page focuses on high-level flow.

## Configuration Model (`config/products.toml`)

The configuration drives every analysis and covers three information blocks:

1. **Analysis scope** – product list, PRODCOM codes, HS codes, and physical properties needed for unit conversion.
2. **Research-based assumptions** – potential refurbishment and recycling rates derived from studies, policy targets, or industry evidence. These values *directly* determine calculated material savings.
3. **Data-driven parameters** – current collection rates, material composition (forthcoming), material recycling efficiencies, and average product weights estimated from official statistics.

Before running CirQuant:
1. Review or edit `config/products.toml` (see the Configuration Guide if changes are needed).
2. Validate the file:
   ```julia
   using CirQuant
   validate_product_config()
   ```

Parameter tables created during processing (e.g., `parameters_circularity_rate`, `parameters_recovery_efficiency`) are stored in the processed DuckDB to ensure reproducibility and version tracking.

## Workflow

### Getting started
1. Clone the repository and install dependencies:
   ```julia
   using Pkg
   Pkg.activate(".")
   Pkg.instantiate()
   ```
2. Configure products (or keep defaults) and run:
   ```julia
   using CirQuant

   validate_product_config()
   fetch_combined_data("2023")
   process_data("2023")
   ```

### Full analysis example

```julia
using CirQuant

# 1. Validate configuration
validate_product_config()

# 2. Fetch Eurostat data (combined or per-source)
fetch_combined_data("2020-2023")
# fetch_prodcom_data("2020-2023", ["ds-059358", "ds-059359"])
# fetch_comext_data("2022-2023")
# fetch_material_composition_data("2020-2023")           # stub
# fetch_material_recycling_rates_data("2020-2023")       # stub
# fetch_product_collection_rates_data("2020-2023")       # stub

# 3. Process the transformed dataset
results = process_data("2020-2023")
println("Processed $(results[:processed_years]) years successfully")
```

### Processing variants

```julia
# Process all available years (2002-2024)
process_data()

# Targeted processing
process_data("2020-2023")

# Advanced options
process_data("2022", cleanup_temp_tables=false, prql_timeout=600)
process_data(use_test_mode=true)   # uses 2002 test data
```

### Fetching options and test DB
- Parallel per-year PRODCOM fetch: set env vars in `fetch_data.sh`, e.g. `PARALLEL_YEARS=true MAX_PARALLEL_YEARS=2 RATE_LIMIT_SECONDS=0.6 RATE_LIMIT_JITTER=0.2 ./fetch_data.sh "2020-2023"`.
- Quick test snapshot (recent years, two products): `./fetch_test_data.sh "2022-2023" "heat_pumps,pv_panels"` populates `CirQuant-database/raw/test.duckdb` so you can develop while the full raw download runs. The script also accepts the parallel/rate-limit env vars.

## Data Transformation & Indicators

The **DataTransform** module turns raw PRODCOM/COMEXT tables into harmonised circularity indicators by:
- Building year-specific strategy tables with consistent dimensions (`product_code`, `product_name`, `year`, `geo`, `level`).
- Executing PRQL queries (with `{{YEAR}}` placeholders) for production and trade flows.
- Loading mapping tables that align PRODCOM production codes with HS trade codes.
- Integrating material composition and recycling rates when available.
- Calculating apparent consumption (`production + imports - exports`) and strategy outcomes.

Key metrics stored per product-year:
- **Production**: `production_volume_tonnes`, `production_value_eur`.
- **Trade**: `import/export_volume_tonnes`, `import/export_value_eur`.
- **Apparent consumption**: tonnage and euro values.
- **Refurbishment**: `refurbishment_material_savings_tonnes`, `..._eur`, `refurbishment_production_reduction_tonnes`.
- **Recycling**: `recycling_material_savings_tonnes`, `..._eur`, plus material recovery values informed by composition and specific rates.

### Working with circularity tables

```julia
using CirQuant
year = 2009

success = create_circularity_table(year, db_path=DB_PATH_PROCESSED, replace=true)
validation = validate_circularity_table(year, db_path=DB_PATH_PROCESSED)
table_info = inspect_raw_tables(DB_PATH_RAW, year)

using CirQuant.CircularityProcessor
prql_files = Dict(
    "production" => "src/DataTransform/production_data.prql",
    "trade" => "src/DataTransform/trade_data.prql"
)

results = CircularityProcessor.process_year_data(
    year;
    raw_db_path=DB_PATH_RAW,
    processed_db_path=DB_PATH_PROCESSED,
    prql_files=prql_files,
    replace=true
)
```

## Databases and PRQL Support

- **Raw DuckDB** (`DB_PATH_RAW`): holds per-year tables `prodcom_ds_059358_YYYY`, `prodcom_ds_059359_YYYY`, and `comext_DS_045409_YYYY`.
- **Processed DuckDB** (`DB_PATH_PROCESSED`): stores circularity indicator tables plus parameter snapshots.
- **PRQL (Pipelined Relational Query Language)** files in `src/DataTransform/` are parameterised through `{{YEAR}}` placeholders, enabling the same query templates across multiple periods.

This separation keeps ingestion, transformation, and analysis reproducible while enabling analysts to inspect intermediate outputs.
