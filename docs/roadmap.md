---
title: Development Roadmap
nav_order: 8
---

# CirQuant Roadmap - Missing Features

This document outlines the data needs and computation steps required to enhance CirQuant's circular economy analysis capabilities.

## Data Development Priorities

- Integrate the Urban Mine Platform battery dataset once it is published (extend UMP battery loader and add raw tables).

## Computation Steps to Implement

Computation and transformation remain the main focus, with the data development priorities listed above.

Step 1 (material-weighted recovery rates) is now implemented using UMP sankey-derived composition and recovery tables.
Step 2 (current recycling material savings) is now implemented using WEEE collection rates and UMP-derived recovery rates.
Step 3 (strategy-specific indicators) is now implemented via `circularity_indicators_by_strategy_YYYY`.

Note: Current refurbishment rates remain largely unknown/estimated

### Step 3: Calculate Strategy-Specific Indicators

**For Refurbishment:**
- material_savings_tonnes = refurbishment_rate × apparent_consumption_tonnes
- material_savings_eur = refurbishment_rate × apparent_consumption_value_eur
- production_reduction_tonnes = refurbishment_rate × apparent_consumption_tonnes
- production_reduction_eur = refurbishment_rate × apparent_consumption_value_eur

**For Recycling:**
- material_savings_tonnes = recycling_rate × material_recovery_rate × apparent_consumption_tonnes
- material_savings_eur = recycling_rate × material_recovery_rate × apparent_consumption_value_eur
- production_reduction_tonnes = 0 (materials return to production)
- production_reduction_eur = 0

### Step 4: Calculate Potential Scenarios
- Use potential rates from config (these remain as research-based assumptions)
- Apply improved rates for each strategy
- Calculate potential savings using same formulas

## Config File Changes

The `config/products.toml` should contain ONLY:
- Product identification (id, name)
- Code mappings (PRODCOM, HS codes)
- Potential circularity rates by strategy (research-based targets)

Remove from config:
- Current weights (fetch from data)
- Current circularity rates (fetch from waste statistics)
- Any other data that should come from databases

## Database Schema Additions

**Raw database tables (Eurostat dataset format):**
- Material composition: Dataset ID to be determined
- `env_wastrt_YYYY`: Waste treatment statistics for material recycling rates
- `env_waselee_YYYY`: WEEE statistics for electronics collection rates
- `env_wasbat_YYYY`: Battery waste statistics for battery collection rates
- Additional waste datasets to be identified

**Processed database tables (meaningful names):**
- `product_material_composition_YYYY` (rows: product × geo)
- `material_recycling_rates_YYYY` (rows: material × geo)
- `product_weights_YYYY` (rows: product × geo)
- `product_collection_rates_YYYY` (rows: product × geo)
- `circularity_indicators_by_strategy_YYYY` (rows: product × geo × strategy)
