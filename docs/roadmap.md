# CirQuant Roadmap - Missing Features

This document outlines the data needs and computation steps required to enhance CirQuant's circular economy analysis capabilities.

## Data Development Needs

### 1. Data-Driven Average Product Weights

**Requirements:**
- Average weight per unit for each product
- Derived from PRODCOM quantity/value data
- Annual data where available
- By country if significant variations exist
- Replace current hardcoded weights in config

**Potential Sources:**
- Calculate from PRODCOM: total_tonnes / total_units
- EU Energy Label database for validation
- Industry association statistics

**Implementation:**
- Raw database: Derived from existing PRODCOM tables
- Processed database: `product_average_weights_YYYY`
- Structure: Rows by product × geo
- Computed during data processing from PRODCOM quantity/value ratios

### 2. Product Material Composition Data

**Requirements:**
- Material breakdown (% by weight) for each product
- Common materials: steel, aluminum, copper, plastics, glass, electronics, etc.
- Data structure: Product-specific, annual data (2002-2023)
- By country and EU aggregates

**Potential Sources:**
- EU Ecodesign preparatory studies
- Product Category Rules (PCR) documents
- Industry material declaration databases

**Implementation:**
- Raw database: Dataset ID unknown - needs research
- Processed database: `product_material_composition_YYYY`
- Structure: Rows by product × geo
- Annual updates to reflect evolving product designs and materials

### 3. Material-Specific Recycling Rates

**Requirements:**
- Recycling/recovery rates for each material type
- Annual data (2002-2023)
- By EU country and EU aggregates
- Stored in database, not config

**Potential Sources:**
- Eurostat waste statistics (env_wastrt)
- Material flow accounts (env_ac_mfa)
- National waste reports

**Implementation:**
- Raw database: `env_wastrt_YYYY` (waste treatment statistics)
- Processed database: `material_recycling_rates_YYYY`
- Structure: Rows by material × geo (country code)
- Annual updates from Eurostat API

### 4. Current Collection/Recycling Rates

**Requirements:**
- Product collection rates (% sent to recycling facilities)
- Product-specific where available (WEEE, batteries)
- Annual data (2002-2023)
- By country and EU aggregates
- Note: Refurbishment rates largely unavailable in official statistics

**Potential Sources:**
- WEEE collection statistics (env_waselee)
- Battery collection data (env_wasbat)
- General waste statistics for other products

**Implementation:**
- Raw database: `env_waselee_YYYY` (WEEE), `env_wasbat_YYYY` (batteries), others unknown
- Processed database: `product_collection_rates_YYYY`
- Structure: Rows by product × geo
- Fetched from Eurostat waste datasets where available

### 5. Product-Specific Unit Values

**Requirements:**
- EUR per tonne for each product
- Already available from PRODCOM
- Need consistent calculation: value / quantity

**Implementation:**
- Calculate during processing from existing PRODCOM data

## Computation Steps to Implement

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
- `product_average_weights_YYYY` (rows: product × geo)
- `product_collection_rates_YYYY` (rows: product × geo)
- `circularity_indicators_by_strategy_YYYY` (rows: product × geo × strategy)
