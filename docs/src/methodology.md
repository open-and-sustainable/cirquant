# CirQuant Methodology

This document describes how CirQuant quantifies circular economy potentials for strategic EU product categories. It focuses on the analytical approach, implementation details, and the main limitations encountered when working with official production and trade statistics.

## Analytical Scope

- **Goal**: Estimate how much material savings can be unlocked through refurbishment and recycling strategies for selected products such as heat pumps, PV modules, ICT equipment, and batteries.
- **Unit of analysis**: Product-year-country combinations, with aggregates at EU and national levels.
- **Selection criteria**: Products are prioritised when they are policy relevant, material intensive, covered by circular economy regulations, or core to the clean-energy transition.

## Data Sources and Harmonisation

### PRODCOM (production)
- Provides production volumes and values reported by EU manufacturers.
- Uses detailed PRODCOM product codes (8–10 digits) and numeric country identifiers.
- Includes limited trade data that can act as a fallback when external trade data is missing.

### COMEXT (trade)
- Provides import/export flows at the HS 6-digit level, with ISO alpha-2 country codes.
- Serves as the primary source for trade volumes and values.

### Mapping and alignment
- Product correspondence tables link PRODCOM codes to HS codes, handling many-to-many relations.
- A country code dictionary converts numeric PRODCOM identifiers into ISO codes to enable merging.
- Trade consolidation follows two passes: COMEXT data is loaded first, and PRODCOM trade values are only applied when COMEXT reports zero to avoid double counting.

## Processing Workflow

1. **Acquisition** – Automatically download PRODCOM and COMEXT datasets for the specified years. Requests include rate limiting and retry logic to cope with API throttling.
2. **Raw storage** – Persist each dataset exactly as received in DuckDB tables named `prodcom_ds_<code>_<year>` and `comext_ds_<code>_<year>` for traceability.
3. **Cleaning & unit handling** – Convert all quantities to tonnes whenever possible, apply consistent currencies (EUR), and run sanity checks on unit/value pairs.
4. **Transformation via PRQL** – Parameterised PRQL scripts orchestrated from Julia perform the heavy lifting:
   - Create mapping tables for PRODCOM↔HS and for country codes.
   - Produce intermediate tables (`prodcom_converted_YYYY`, `production_temp_YYYY`, `trade_temp_YYYY`).
   - Harmonise production and trade into `production_trade_harmonized_YYYY`.
   - Apply the PRODCOM fallback to build `production_trade_YYYY`.
5. **Circular indicator construction** – Enrich the harmonised dataset with:
   - Apparent consumption (production + imports − exports).
   - Material composition and collection rates when available.
   - Refurbishment and recycling savings derived from product assumptions.
6. **Aggregation and exports** – Build EU and country aggregates, store final circularity tables in the processed DuckDB, and expose them for reporting or further analysis.

## Circular Indicators

- **Production**: `production_volume_tonnes`, `production_value_eur`, and derived unit values.
- **Trade**: import/export volumes and values expressed in tonnes/EUR, plus net trade balances.
- **Apparent consumption**: tonnes and EUR to approximate domestic use.
- **Refurbishment indicators**: potential material savings and avoided production when refurbished units replace new products.
- **Recycling indicators**: recoverable material volumes and values, based on composition matrices and material-specific recovery efficiencies.
- **Strategy comparison**: current vs potential rates for each strategy, enabling scenario analysis.

## Parameter Management

All assumptions reside in `config/products.toml`:

- **Scope configuration** – product lists, PRODCOM codes, HS codes, weights, and units.
- **Research-based rates** – potential refurbishment and recycling rates collected from studies, policy targets, or expert input.
- **Data-driven inputs** – current collection rates, material composition, and weight estimates (progressively populated as new datasets are integrated).

During runtime the configuration is validated, loaded into the `ANALYSIS_PARAMETERS` structure, and written to processed DuckDB tables such as `parameters_circularity_rate` and `parameters_recovery_efficiency`. PRQL queries join against these tables so that updating `products.toml` immediately affects indicator calculations without code changes.

## Limitations and Potential Issues

### Data constraints
- **Confidentiality flags** – PRODCOM suppresses sensitive company data, yielding gaps that cannot be filled through public sources.
- **Coverage differences** – Some countries or years do not report every product, especially newer green technologies.
- **Classification drift** – Code definitions change over time, requiring periodic updates to mapping tables and sometimes preventing long time-series comparisons.
- **Unit heterogeneity** – Not all products report in tonnes; when conversion factors are missing, additional research is needed before an item can be included.

### Methodological assumptions
- **Product homogeneity** – Each code aggregates products with potentially different material compositions and lifetimes.
- **Trade attribution** – Imports and exports are assigned to the reporting country even if goods are in transit, which can overstate domestic availability.
- **Apparent consumption** – The standard formula ignores stock changes and re-exports, so it approximates but does not perfectly measure actual use.
- **Potential rates** – Published refurbishment/recycling potentials may overestimate what is technically or economically feasible in the short term; sensitivity checks are recommended.

### Practical risks and mitigation
- **API instability** – Automated fetchers include retries, but persistent outages require downloading CSV extracts manually and loading them into the raw database.
- **Mapping errors** – Misaligned PRODCOM↔HS correspondences can skew trade balances. Regularly review the mapping tables and cross-check against Eurostat correspondence files.
- **Parameter drift** – When analysts update `products.toml`, validation must run (`validate_product_config()`) to prevent missing or inconsistent fields from entering the pipeline.

## Quality Assurance

- **Cross-source checks** – Compare COMEXT trade totals with PRODCOM fallback data to detect anomalies.
- **Temporal validation** – Flag implausible year-on-year swings for manual review.
- **Material composition sanity** – Ensure that composition percentages sum to 100% and match known product bills of materials.
- **Strategy totals** – Confirm that refurbishment, recycling, and residual flows align with apparent consumption to avoid double counting.
- **Logging and provenance** – Each processing step emits status logs; DuckDB keeps both intermediate and final tables so analysts can audit transformations.

## Database Architecture

CirQuant separates ingestion from analysis:

- **Raw DuckDB** preserves the exact API payloads for reproducibility and auditing.
- **Processed DuckDB** stores harmonised production-trade tables, parameter snapshots, and circularity indicators ready for analysis.

Detailed schemas for both databases are described in `database-schema-raw.md` and `database-schema-processed.md`.

## Further Reading

- Eurostat PRODCOM methodology documentation (latest release).
- Eurostat COMEXT user guide for data extraction and indicator definitions.
- EU Circular Economy Action Plan for strategic context and policy targets.
