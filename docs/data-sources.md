# Data Sources

This note summarises every dataset currently (or soon to be) used by CirQuant and explains how each maps onto the raw DuckDB schema documented in `docs/database-schema-raw.md`. Use it to check provenance, coverage, and limitations before running large fetch jobs or interpreting results.

## 1. Source inventory

| Source | Role | Raw table pattern | Time span | Notes |
|--------|------|------------------|-----------|-------|
| **PRODCOM** (Eurostat) | Production and limited trade indicators by PRODCOM code | `prodcom_ds_059358_YYYY`, `prodcom_ds_059359_YYYY` | 1995–present | DS-059358 (sold production/trade) fetched by default; DS-059359 (total production) optional |
| **COMEXT** (Eurostat) | Trade flows by HS6 code (imports/exports, intra/extra EU) | `comext_ds_059341_YYYY` | 2002–present | Primary source for trade data |
| **COMEXT CN8** (Eurostat) | Trade flows by CN8 code (imports/exports, intra/extra EU) with supplementary units | `comext_ds_045409_YYYY` | 2002–present | Needed to obtain supplementary (count) units when available; indicators: `QUANTITY_IN_100KG`, `SUPPLEMENTARY_QUANTITY` |
| **Waste statistics** (Eurostat) | Collection rates for electronics/batteries | `env_waselee_YYYY`, `env_wasbat_YYYY` (planned) | Various | Integration pending |
| **Waste treatment / material recovery** | Recovery efficiencies by material | `env_wastrt_YYYY` (planned) | Various | Provides recovery percentages |
| **Material composition datasets** | Product bill-of-materials | `<dataset>_YYYY` (to be defined) | TBD | Under assessment (Ecodesign studies, PEF, LCA) |
| **Derived weights** | Average mass per product | `product_average_weights_YYYY` (processed DB) | Derived annually | Computed from PRODCOM quantities/values |

Only PRODCOM and COMEXT populate the raw DuckDB today; other sources will follow the same naming convention once integrated.

## 2. PRODCOM (Production statistics)

### 2.1 Datasets

- **DS-059358** – Annual PRODCOM data by PRODCOM_LIST (NACE Rev. 2). Provides sold production plus export/import indicators for each declarant country and EU aggregates (time range 1995–2023 in the current database snapshot).
- **DS-059359** – Total production dataset. Useful when sold production figures diverge from total output; not fetched by default because it often returns empty responses due to confidentiality. Request explicitly via the `custom_datasets` argument.

### 2.2 Raw schema alignment

Both datasets are written to tables named `prodcom_ds_<dataset>_<year>` exactly as described in `docs/database-schema-raw.md`. Key columns include:
- `prccode` / `prodcom_code_original` – PRODCOM code without/with dots.
- `indicators` / `indicators_label` – Indicator codes such as `PRODVAL`, `PRODQNT`, `EXPVAL`, `EXPQNT`, `IMPVAL`, `IMPQNT`, `QNTUNIT`.
- `decl` / `decl_label` – Declarant country code and label.
- `value` – Stored as VARCHAR to handle numbers, units, and flags (e.g., `:c` for confidential).
- Metadata timestamps (`update_data`, `timestamp_data`, etc.) for traceability.

Indicator definitions:
- `PRODVAL` – Production value (EUR)
- `PRODQNT` – Production quantity (units vary; see `QNTUNIT`)
- `EXPVAL`, `EXPQNT` – Export value/quantity (DS-059358)
- `IMPVAL`, `IMPQNT` – Import value/quantity (DS-059358)
- `QNTUNIT` – Unit label (KG, L, M3, etc.)
- Total production indicators – Reported in DS-059359; available fields mirror Eurostat’s total production outputs for the requested year.

### 2.3 Usage notes

- PRODCOM trade indicators act as a fallback only when COMEXT has zeros for a product-year combination.
- Confidentiality frequently suppresses `PRODQNT` at EU aggregates; the system logs empty responses but continues processing.
- When fetching DS-059359 for total production:
  ```julia
  fetch_prodcom_data("2017-2023", ["ds-059359"])  # Only total production dataset
  fetch_prodcom_data("2017-2023", ["ds-059358", "ds-059359"])  # Fetch both
  ```
- Fetching options:
  - Parallel per-year fetch is available via `parallel_years=true` and `max_parallel_years` (defaults to 2). A shared rate limiter (`rate_limit_seconds`, `rate_limit_jitter`) is applied across workers to avoid API bursts.
  - The shell wrapper `fetch_data.sh` exposes these as env vars (e.g., `PARALLEL_YEARS=true MAX_PARALLEL_YEARS=2 RATE_LIMIT_SECONDS=0.6`).
  - For a fast development snapshot, `fetch_test_data.sh "2022-2023" "heat_pumps,pv_panels"` writes to `CirQuant-database/raw/test.duckdb`; adjust `DB_PATH` to target another test database. Defaults use a slightly lower rate limit for quicker iterations.
  - Limit scope with `product_keys_filter` (e.g., `["heat_pumps","pv_panels"]`) when calling `fetch_prodcom_data` to keep test downloads small and focused.

## 3. COMEXT (Trade statistics)

### 3.1 Dataset

- **DS-059341** – International trade for EU and partner countries since 2002, disaggregated by HS2/HS4/HS6 products. CirQuant requests HS6 records matching the product mapping table, filtered by partner grouping and flow type.

### 3.2 Raw schema alignment

Rows are stored in `comext_ds_059341_YYYY`. Important columns (see `docs/database-schema-raw.md` for the full list):
- `product` – HS6 code without dots; `hs_code_query` retains the queried string.
- `flow` / `flow_code` – `1` for imports, `2` for exports (with `flow_label` text).
- `partner` / `partner_code` / `partner_label` – e.g., `INT_EU27_2020` (intra-EU) or `EXT_EU27_2020` (extra-EU).
- `indicators` – `VALUE_EUR` or `QUANTITY_KG`.
- `value` – Stored as VARCHAR because Eurostat may return scientific notation or placeholders.
- `fetch_date` – Timestamp when the call was executed.

### 3.3 Query strategy

To manage rate limits and file sizes:
1. **Partner split** – Separate calls for `INT_EU27_2020` and `EXT_EU27_2020`.
2. **Flow split** – Imports (`flow=1`) and exports (`flow=2`).
3. **Product filtering** – Only HS6 codes defined in `config/products.toml`.
4. **Rate limiting** – 0.5s delay after successful calls, 1s after failures.
5. **Error handling** – Failures are logged; fetch routines continue so a single outage does not halt the workflow.

COMEXT is treated as the authoritative source for trade indicators; PRODCOM fallback logic only replaces zero values to avoid double counting.

## 4. Complementary circular-economy data (planned)

The following datasets extend the analysis but have not yet been loaded into the raw DuckDB. When implemented, they will follow the same naming pattern (`<dataset>_<year>`).

### 4.1 Waste collection statistics

- **`env_waselee`** – WEEE data for electronic equipment.
- **`env_wasbat`** – Battery waste statistics.
- **Content** – Annual collection or recycling rates by product category and country.
- **Use** – Provides `current_circularity_rate` values grounded in official statistics, replacing placeholder assumptions in the configuration.

### 4.2 Waste treatment / recovery efficiency

- **`env_wastrt`** – Waste treatment operations by material.
- **Content** – Recovery rates for materials such as steel, aluminium, copper, plastics.
- **Use** – Supplies material-specific recycling efficiencies for calculating recovered tonnes/euros in the processed database.

### 4.3 Material composition datasets

- **Sources under review** – Ecodesign preparatory studies, Product Environmental Footprint (PEF) datasets, commercial LCA databases.
- **Content** – Product × material × mass share, potentially with year or technology differentiators.
- **Use** – Enables weighted material recovery calculations in PRQL transformations.

### 4.4 Derived average weights

- **Method** – Compute `total_tonnes / total_units` using PRODCOM observations to populate `product_average_weights_YYYY`.
- **Status** – Stored in the processed database; backfills missing `weight_kg` assumptions from the configuration.

## 5. Product scope and mappings

CirQuant currently focuses on:
- Heat pumps & refrigeration equipment
- Photovoltaic equipment
- ICT hardware (phones, computers, monitors, storage, aggregate ICT categories)
- Batteries (Li-ion and other chemistries)

Each product entry in `config/products.toml` lists PRODCOM and HS codes which feed the fetch routines. Where available, CN8 codes are also provided to enable COMEXT CN8 queries (needed to obtain supplementary item counts). The mapping table constructed from this configuration ensures that PRODCOM (`prccode`) and HS/CN8 (`product`) codes can be reconciled during transformation.

## 6. Fetching and processing strategy

### 6.1 Trade data priority
- **Primary** – COMEXT (`VALUE_EUR`, `QUANTITY_KG`) for imports/exports.
- **Fallback** – PRODCOM indicators only when COMEXT delivers `0` or missing values for the same product-year. Replacement happens during the harmonisation phase when building `production_trade_harmonized_YYYY` and `production_trade_YYYY`.

### 6.2 Performance safeguards
- **Rate limiting** – 0.5 s delay after success, 1 s after failure (both APIs).
- **Query batching** – PRODCOM: one call per indicator-year-product; COMEXT: one call per HS6-year-flow-partner set.
- **Error logging** – Failures are recorded; processing continues with partial data if necessary.
- **Fallback storage** – If DuckDB writes fail, CSV backups are generated so no fetch is lost.

### 6.3 Handling non-default datasets
Use the `custom_datasets` argument when calling `fetch_prodcom_data` to include DS-059359 or other PRODCOM datasets. This keeps the default workflow lightweight while still supporting targeted research.

## 7. Known issues and coverage gaps

1. **Confidentiality** – DS-059359 and certain PRODCOM indicators return empty results for EU aggregates or small countries. Expect gaps and rely on COMEXT/other sources when possible.
2. **Trade gaps** – Some HS codes lack trade entries in early years or for specific partner combinations. Logged as empty results; analysts may need to cross-check or adjust mapping codes.
3. **Unit heterogeneity** – `QNTUNIT` varies widely; downstream conversion scripts must handle litres, pieces, square metres, etc.
4. **Historical limits** – PRODCOM extends to 1995, but COMEXT starts in 2002, constraining the overlap period available for joint analyses.

## 8. Database storage recap

The raw DuckDB mirrors Eurostat responses (see `docs/database-schema-raw.md`). Table names and columns match those described above, ensuring:
- **Traceability** – metadata columns (`fetch_date`, timestamps, original codes) track every API call.
- **Consistency** – one table per dataset-year combination.
- **Extensibility** – future sources (waste statistics, material composition) will adopt the same naming convention so documentation remains valid.

Processed tables (e.g., `product_material_composition_YYYY`, `material_recycling_rates_YYYY`, `product_collection_rates_YYYY`, `product_average_weights_YYYY`, `circularity_indicators_by_strategy_YYYY`) build on these inputs and are described in `docs/database-schema-processed.md`.
