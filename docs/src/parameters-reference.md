# Parameters Reference Guide

The purpose of this guide is to describe every configurable or derived parameter used by CirQuant, how it is stored, and how it affects the analysis. Use it alongside the [Configuration Guide](configuration-guide/) when editing `config/products.toml`.

## 1. Configuration structure

All research assumptions live in a single TOML file. Each product has one block with metadata, a nested `prodcom_codes` table keyed by nomenclature epoch, and a `parameters` table:

```toml
[prodcom_epochs.legacy]
start_year = 1995
end_year = 2007

[prodcom_epochs.nace_rev2]
start_year = 2008
end_year = 2100

[products.product_key]
id = 1
name = "Heat pumps"
hs_codes = ["8418.69"]

[products.product_key.prodcom_codes]
legacy = ["28.21.13.30"]
nace_rev2 = ["28.25.13.80"]

[products.product_key.parameters]
weight_kg = 100.0
unit = "piece"
current_circularity_rate = 5.0
potential_circularity_rate = 45.0
```

### 1.1 Product block
- `id` *(Int)* – Numeric identifier used for ordering; must be unique.
- `name` *(String)* – Human-readable label shown in outputs.
- `prodcom_codes` *(Table of Array{String})* – Codes grouped by PRODCOM epoch (e.g., `legacy`, `nace_rev2`). Each entry lists dot-formatted codes valid for that time span.
- `hs_codes` *(Array{String})* – One or more HS6 codes, dot-separated for readability.

### 1.2 Parameters block
- `weight_kg` *(Float)* – Average product mass per unit. Enables conversion from pieces to tonnes when data only reports counts.
- `unit` *(String)* – Native PRODCOM unit (e.g., `piece`, `set`, `kg`). Used for validation, not for conversions.
- `current_circularity_rate` *(Float, %)* – Share of apparent consumption currently refurbished or recycled. Placeholder until official statistics are integrated.
- `potential_circularity_rate` *(Float, %)* – Research-based outlook for the same strategy. Drives the “potential” indicators.

**Constraints**
- Rates are expressed from 0 to 100. Validate that `potential ≥ current`.
- Refurbishment and recycling are modelled through the same fields; if both strategies are analysed separately, define additional products or document how the split is handled.

## 2. Research vs data-driven inputs

| Input type | Source | Storage | Purpose |
|------------|--------|---------|---------|
| Product metadata + circularity rates | `config/products.toml` | `parameters_circularity_rate` table in processed DB | Sets analysis scope and potential rates |
| Material composition | External studies / future datasets | `product_material_composition_YYYY` | Determines recoverable material mix |
| Collection rates | Eurostat waste statistics | `product_collection_rates_YYYY` | Constrains recycling volumes |
| Material recycling efficiencies | Waste treatment statistics | `material_recycling_rates_YYYY` | Applies material-specific recovery factors |
| Average weights | Config + derivations | `product_weights_YYYY` | Stores config `weight_kg` and any derived mass/counts from PRODCOM/COMEXT |

Only the first row is edited manually; the rest are fetched or calculated and written to DuckDB by the pipeline.

## 3. Field reference

### 3.1 Identification and scope
- **`products.<key>`**: Free-text key; used in logs and table names. Keep lowercase with underscores.
- **`id`**: Incremental integer; collisions break validation.
- **`name`**: Appears in dashboards and exports.

### 3.2 Code mappings
- **`prodcom_epochs.*`**: Optional top-level table defining the start/end year for each nomenclature epoch. Defaults cover the legacy (≤2007) and NACE Rev.2 (2008+) lists.
- **`prodcom_codes`**: Nested table within each product. Keys must match an epoch defined above and values are arrays of full codes (`28.21.13.30`). Multiple codes per epoch allow grouping related items.
- **`hs_codes`**: Provide the trade codes needed to fetch COMEXT data. List every HS6 relevant to the product.

### 3.3 Physical parameters
- **`weight_kg`**: Required when PRODCOM reports in pieces, sets, or other non-mass units. Leave as `nothing` only if weights are derived from data.
- **`unit`**: Mirrors the PRODCOM unit description. Helps analysts interpret the `weight_kg` assumption.

### 3.4 Circular rates
- **`current_circularity_rate`**: Snapshot of today’s refurbishment/recycling performance. Defaults can be conservative placeholders.
- **`potential_circularity_rate`**: Ambition or technical potential used for scenario analysis. Must be evidence-based (policy target, industry roadmap, etc.).

## 4. Runtime handling

1. `validate_product_config()` checks schema compliance (required fields, numeric ranges, unique IDs, epoch definitions).
2. The configuration is loaded into the `ANALYSIS_PARAMETERS` structure at package initialisation.
3. During processing, parameter rows are written to `parameters_circularity_rate` (and `parameters_recovery_efficiency` if material-level data exists) in the processed DuckDB so downstream steps can join on them.
4. PRQL scripts use these tables to compute refurbishment/recycling savings, e.g.:

```prql
from ci = circularity_indicators_{{YEAR}}
join p = parameters_circularity_rate (ci.product_code == p.product_code)
derive {
    potential_rate_pct = p.potential_circularity_rate,
    potential_savings_tonnes = apparent_consumption_tonnes *
        (p.potential_circularity_rate - p.current_circularity_rate) / 100
}
```

## 5. Derived indicators

Although not configured directly, several metrics depend on the parameters above:

- **Material recovery rate** = Σ(material share × material recovery efficiency). Requires composition + recovery tables.
- **Refurbishment savings** = `potential_rate × apparent_consumption`. Assumes 100% material preservation.
- **Recycling savings** = `collection_rate × material_recovery_rate × apparent_consumption`.
- **Value equivalents** = Multiply tonnage savings by `apparent_consumption_value_eur`.

When composition or collection data is missing, the system can still run but recycling savings will remain zero or rely on placeholder values.

## 6. Validation checklist

- Keep refurbishment + recycling narratives consistent. Document assumptions in Git commits or inline comments.
- Ensure all products referenced in analysis scripts exist in `products.toml`.
- Update mapping codes whenever Eurostat revises PRODCOM/HS classifications, and add new epochs if year ranges change.
- Rerun `validate_product_config()` after every edit; CI jobs depend on it.
- Store evidence for potential rates (studies, policy targets) in project notes or docstrings for auditability.

## 7. Best practices

1. **Single source of truth** – edit only `config/products.toml`; never hardcode parameters elsewhere.
2. **Version changes** – use Git history to track parameter updates and link them to methodological notes.
3. **Scenario management** – create alternative TOML files (e.g., `products_high_potential.toml`) and pass their path into workflows when exploring scenarios.
4. **Documentation** – whenever a new data-driven parameter (composition, collection, recovery) becomes available, document its provenance in `data-sources.md` and ensure the relevant DuckDB tables are populated.
5. **Consistency** – keep naming conventions (`products.<snake_case>`) stable to avoid rewriting PRQL joins or analysis notebooks.
6. **Validation first** – always run `validate_product_config()` (and fix errors) before fetching or processing data.

## Related Documentation

- [Configuration Guide](configuration-guide/) – How to edit `products.toml`.
- [Methodology](methodology/) – Analytical workflow and indicator definitions.
- [Database Schema – Processed](database-schema-processed/) – Storage layout for parameter tables.
- [Data Sources](data-sources/) – Provenance for data-driven parameters.
