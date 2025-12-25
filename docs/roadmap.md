---
title: Development Roadmap
nav_order: 8
---

# CirQuant Roadmap - Missing Features

This document outlines the data needs and computation steps required to enhance CirQuant's circular economy analysis capabilities.

## Computation Steps to Implement

All current work focuses on computation and transformation; no additional data development items are planned at this time.

### Step 1: Calculate Material-Weighted Recovery Rates
For each product:
```sql
material_recovery_rate = SUM(material_weight_pct * material_recycling_rate)
```

### Step 2: Calculate Current Recycling Material Savings
```sql
current_recycling_savings = collection_rate * material_recovery_rate * apparent_consumption
```
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
