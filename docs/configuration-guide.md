# Configuration Guide

CirQuant keeps every research assumption in a single TOML file: `config/products.toml`. This guide highlights how to define the product focus of your analysis, where the key inputs come from, and how to validate the configuration before running any pipelines.

Use the [Parameters Reference](parameters-reference.md) for field-level details and the [Methodology](methodology.md) document to understand how these inputs propagate through the workflow.

## 1. What belongs in the configuration?

Each product entry answers two questions:

1. **Scope** – Which products are in focus? Defined via PRODCOM and HS codes plus a descriptive name.
2. **Potential** – What share of apparent consumption can realistically be refurbished or recycled? Captured through the `current_circularity_rate` and `potential_circularity_rate` fields inside each product’s `parameters` table, along with optional weight assumptions.

Everything else (e.g., observed collection rates, material composition, recovery efficiencies) is fetched from statistical sources and therefore kept out of the configuration file.

### 1.1 Typical evidence sources

| Input | Primary sources | Notes |
|-------|-----------------|-------|
| `prodcom_codes` | Eurostat PRODCOM catalogue, correspondence tables | Always include dots (`28.21.13.30`). Avoid placeholders or aggregate prefixes unless Eurostat publishes them as valid codes. |
| `hs_codes` | Eurostat COMEXT metadata, WCO HS explanatory notes | Provide every HS6 needed to capture imports/exports for the selected product. |
| `weight_kg` | Manufacturer datasheets, ecodesign BoM studies, PRODCOM-derived averages | Only required when official data lacks mass units. |
| `current_circularity_rate` | Eurostat waste statistics, industry surveys, SME interviews | Can be provisional placeholders until official statistics are integrated. |
| `potential_circularity_rate` | Systematic literature reviews, policy targets, expert elicitation | Should reflect technical/economic potential; cite sources in comments or commit messages. |

## 2. File anatomy

```toml
[products.product_key]
id = 1
name = "Heat pumps"
prodcom_codes = ["28.21.13.30"]
hs_codes = ["8418.69"]

[products.product_key.parameters]
weight_kg = 100.0
unit = "piece"
current_circularity_rate = 5.0
potential_circularity_rate = 45.0
```

- `products.product_key` – use descriptive snake_case keys (e.g., `batteries_li_ion`). Keys appear in logs and database table names.
- `id` – unique integer for ordering.
- `name` – human-readable label in outputs.
- `prodcom_codes` / `hs_codes` – arrays of codes with dots for readability.
- `parameters` – nested table storing weights and circularity assumptions. `unit` mirrors the PRODCOM unit description to help interpret `weight_kg`.

## 3. Defining or updating products

1. **Research the product**  
   - Confirm it matches the selection criteria (policy relevance, material intensity, etc.).  
   - Collect PRODCOM and HS codes, typical weights, and evidence for current/potential rates.

2. **Add or edit the TOML block**  
   - Append a new `[products.<key>]` section followed by `[products.<key>.parameters]`.  
   - Ensure IDs do not collide and maintain consistent naming conventions.

3. **Document provenance**  
   - Add inline comments or mention sources in the commit message (e.g., “Potential from 2024 JRC refurbishability study”).

4. **Validate**  
   - Run `validate_product_config()` before fetching or processing data.

Example:

```toml
[products.solar_inverters]
id = 14
name = "Solar inverters"
prodcom_codes = ["27.11.50.00"]
hs_codes = ["8504.40"]

[products.solar_inverters.parameters]
weight_kg = 35.0                       # Manufacturer data sheet
unit = "piece"
current_circularity_rate = 8.0         # Industry survey (2023)
potential_circularity_rate = 60.0      # Literature review on repair/refurb potential
```

## 4. Validation workflow

```julia
using CirQuant
validate_product_config()
```

Validation checks that:
- Required fields exist and use the correct data types.
- `potential_circularity_rate` and `current_circularity_rate` fall within 0–100.
- `potential >= current`.
- Product IDs and PRODCOM codes remain unique.

Fix any reported issues before continuing; downstream scripts assume a valid configuration.

## 5. How the configuration drives the pipeline

- **Data fetching** uses `prodcom_codes` to download PRODCOM tables and `hs_codes` to request COMEXT data. Missing or incorrect codes mean the product’s data will never enter the database.
- **Transformation** writes configuration-derived parameters into the `parameters_circularity_rate` table inside the processed DuckDB. PRQL scripts join against this table when computing refurbishment/recycling savings.
- **Scenario analysis** simply swaps TOML files. Maintain variants (e.g., `products_low_potential.toml`, `products_high_potential.toml`) and pass their path into your Julia session before running validation and data fetching.

## 6. Decision checklist

Before committing configuration changes:
- Have you recorded where each potential rate or weight assumption comes from?
- Do selection criteria still hold for every product (strategic importance, regulation, material intensity)?
- Are all PRODCOM/HS codes up to date with the latest Eurostat releases?
- Does validation pass without warnings?
- Are alternative scenarios documented if uncertainty is high?

## 7. Troubleshooting

- **Configuration not loading** – confirm the file path (`config/products.toml`) and rerun `validate_product_config()`.
- **Changes not applied** – restart the Julia session or re-import the CirQuant module after editing the file.
- **Data fetch issues** – double-check code formatting (PRODCOM must include dots) and ensure the product key/ID is unique.
- **Missing trade or production data** – revisit mapping tables; mis-specified codes prevent data from joining correctly.

## 8. Related documentation

- [Parameters Reference](parameters-reference.md) – Field definitions and runtime behaviour.
- [Methodology](methodology.md) – Analytical flow and how parameters influence indicators.
- [Data Sources](data-sources.md) – Provenance and caveats for PRODCOM/HS data and supplementary datasets.
