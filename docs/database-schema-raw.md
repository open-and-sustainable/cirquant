# Raw Database Schema

## Overview

The raw database stores data exactly as fetched from Eurostat APIs without transformation. This preserves the original data structure and allows for full traceability.

**Database**: `CirQuant-database/raw/CirQuant_2002-2023.duckdb`

## Table Naming Convention

Tables follow a consistent naming pattern:
- PRODCOM: `prodcom_ds_XXXXXX_YYYY`
- COMEXT: `comext_ds_XXXXXX_YYYY`
- Waste treatment/collection: `env_<dataset>_YYYY`

Where:
- `XXXXXX` is the dataset ID (with hyphens replaced by underscores)
- `YYYY` is the 4-digit year

## PRODCOM Tables

### Table: `prodcom_ds_059358_YYYY`

Annual PRODCOM data for sold production, imports, and exports.

| Column | Type | Description |
|--------|------|-------------|
| update_data | VARCHAR | Data update timestamp |
| time | VARCHAR | Time period (year) |
| prccode | VARCHAR | PRODCOM code without dots (e.g., "27114000") |
| indicators | VARCHAR | Indicator code (PRODVAL, PRODQNT, etc.) |
| freq_label | VARCHAR | Frequency label ("Annual") |
| timestamp_data | VARCHAR | Data timestamp |
| value | VARCHAR | Numeric value or unit string (stored as string) |
| timestamp_global | VARCHAR | Global update timestamp |
| indicators_label | VARCHAR | Human-readable indicator name |
| update_structure | VARCHAR | Structure update timestamp |
| decl | VARCHAR | Declarant code (country) |
| freq | VARCHAR | Frequency code ("A" for annual) |
| prccode_label | VARCHAR | Product description |
| decl_label | VARCHAR | Country name |
| dataset_updated | VARCHAR | Dataset update date |
| time_label | VARCHAR | Year label |
| prodcom_code_original | VARCHAR | PRODCOM code with dots (e.g., "27.11.40.00") |

**Indicators in DS-059358:**
- `PRODVAL`: Production value in EUR
- `PRODQNT`: Production quantity
- `EXPVAL`: Export value in EUR
- `EXPQNT`: Export quantity
- `IMPVAL`: Import value in EUR
- `IMPQNT`: Import quantity
- `QNTUNIT`: Unit of measurement (KG, L, M3, etc.)

### Table: `prodcom_ds_059359_YYYY`

Total production indicators (where available).

Same structure as DS-059358 but typically contains only the total production fields and units. Frequently sparse because of confidentiality restrictions on EU individual country data.

## COMEXT Tables

### Table: `comext_ds_059341_YYYY`

International trade data at 6-digit HS code level.

| Column | Type | Description |
|--------|------|-------------|
| freq | VARCHAR | Frequency code ("A" for annual) |
| reporter | VARCHAR | Reporter country code |
| partner | VARCHAR | Partner code (INT_EU27_2020, EXT_EU27_2020) |
| product | VARCHAR | HS 6-digit code without dots |
| flow | VARCHAR | Trade flow code (1=Import, 2=Export) |
| indicators | VARCHAR | Indicator code (VALUE_EUR, QUANTITY_KG) |
| time | VARCHAR | Year |
| value | VARCHAR | Numeric value (stored as string, may use scientific notation) |
| freq_label | VARCHAR | Frequency label |
| reporter_label | VARCHAR | Reporter country name |
| partner_label | VARCHAR | Partner description |
| product_label | VARCHAR | Product description |
| flow_label | VARCHAR | Flow description ("IMPORT" or "EXPORT") |
| indicators_label | VARCHAR | Indicator description |
| time_label | VARCHAR | Year label |
| hs_code_query | VARCHAR | HS code used in the query |
| indicator_query | VARCHAR | Indicator requested |
| partner_type | VARCHAR | Partner type (INTRA_EU, EXTRA_EU) |
| partner_code | VARCHAR | Original partner code |
| flow_type | VARCHAR | Flow type name |
| flow_code | BIGINT | Flow code (1 or 2) |
| fetch_date | TIMESTAMP | When data was fetched (format: YYYY-MM-DD HH:MM:SS.SSS) |

**Indicators:**
- `VALUE_EUR`: Trade value in EUR
- `QUANTITY_KG`: Trade quantity in kilograms

**Partners:**
- `INT_EU27_2020`: Intra-EU27 trade (from 2020)
- `EXT_EU27_2020`: Extra-EU27 trade (from 2020)

## Waste Treatment / Collection Tables

### Table: `env_wastrt_YYYY`
Waste treatment statistics by waste category and operation.

Key dimensions/columns (as returned by EurostatAPI):
- `freq`, `unit`, `hazard`, `wst_oper`, `waste`, `geo`, `time`, `value`
- Additional columns: `dataset`, `year`, `fetch_date`, `original_key`, `original_value`

### Table: `env_waseleeos_YYYY`
WEEE open-scope collection/sales data (post-2018 categories).

Key dimensions/columns:
- `freq`, `waste`, `wst_oper`, `unit`, `geo`, `time`, `value`
- Additional columns: `dataset`, `year`, `fetch_date`, `original_key`, `original_value`

### Table: `env_waspb_YYYY`
Portable battery sales/collection.

Key dimensions/columns:
- `freq`, `wst_oper`, `waste`, `unit`, `geo`, `time`, `value`
- Additional columns: `dataset`, `year`, `fetch_date`, `original_key`, `original_value`

## Data Types and Storage Decisions

1. **String Storage for Values**: The `value` column is VARCHAR to accommodate:
   - Numeric values (production quantities, trade values)
   - Unit strings (KG, L, M3 from QNTUNIT indicator)
   - Missing or suppressed data markers

2. **Preserved Original Codes**: Both cleaned (no dots) and original (with dots) versions of product codes are stored

3. **Metadata Columns**: Additional columns track query parameters and fetch timestamps for debugging and traceability

## Example Queries

### Get all production values for heat pumps in 2020:
```sql
SELECT * FROM prodcom_ds_059358_2020
WHERE prodcom_code_original = '28.21.13.30'
  AND indicators = 'PRODVAL';
```

### Get EU imports of batteries from outside EU in 2022:
```sql
SELECT * FROM comext_ds_059341_2022
WHERE product = '850760'
  AND partner_code = 'EXT_EU27_2020'
  AND flow = '1';
```

## Data Quality Notes

1. **Missing Data**: NULL or empty values are common due to:
   - Confidentiality suppression
   - No production/trade in that year
   - Data not yet reported

2. **Value Formats**: Numeric values may include:
   - Scientific notation (e.g., "2.1788009e8", "6.69011062e8")
   - Decimal separators
   - Special markers (":c" for confidential)

3. **Country Coverage**: Not all EU countries report all products
