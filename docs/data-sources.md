# Data Sources Documentation

## Overview

CirQuant fetches data from two primary Eurostat databases to analyze circular economy metrics for specific product categories:

1. **PRODCOM** - Production statistics for manufactured goods
2. **COMEXT** - International trade statistics (imports/exports)

## PRODCOM Data

### Datasets Used

- **DS-056120**: PRODCOM annual data by PRODCOM_LIST (NACE Rev. 2) - EU aggregates
  - Contains production, import, and export data at the EU aggregate level
  - Indicators: PRODVAL, PRODQNT, EXPVAL, EXPQNT, IMPVAL, IMPQNT, QNTUNIT
  - Time range: 1995-2023

- **DS-056121**: PRODCOM annual data by PRODCOM_LIST (NACE Rev. 2) - EU aggregates (from 2017)
  - Contains only PRODQNT and QNTUNIT indicators
  - **Note**: This dataset often appears empty because production quantity data is frequently not reported at the aggregate EU level for confidentiality reasons

### PRODCOM Indicators

- `PRODVAL`: Production value in EUR
- `PRODQNT`: Production quantity
- `EXPVAL`: Export value in EUR  
- `EXPQNT`: Export quantity
- `IMPVAL`: Import value in EUR
- `IMPQNT`: Import quantity
- `QNTUNIT`: Unit of measurement (stored as string values)

## COMEXT Data

### Dataset Used

- **DS-059341**: International trade of EU and non-EU countries since 2002 by HS2-4-6
  - Contains detailed trade flows at 6-digit HS code level
  - Time range: 2002-2023 (COMEXT data not available before 2002)
  - Indicators: VALUE_EUR, QUANTITY_KG

### Data Fetching Approach

To manage API limitations and data volume:

1. **Partner filtering**: 
   - `INT_EU27_2020`: Intra-EU trade
   - `EXT_EU27_2020`: Extra-EU trade

2. **Flow types**:
   - 1: Imports
   - 2: Exports

3. **Product filtering**: Uses 6-digit HS codes derived from the product mapping table

## Products of Interest

The system focuses on specific product categories relevant to circular economy analysis:

- Heat pumps and refrigeration equipment
- Photovoltaic equipment  
- ICT equipment (computers, servers, displays)
- Batteries (Li-ion and other types)

Products are mapped between PRODCOM codes (8-10 digits) and HS codes (6 digits) using the internal product conversion table.

## Data Fetching Strategy

### Rate Limiting
Both APIs implement rate limiting. The system includes:
- 0.5 second delay between successful API calls
- 1.0 second delay after failed requests

### Query Optimization
- PRODCOM: One query per indicator, year, and product code
- COMEXT: One query per indicator, partner, flow, year, and HS code

### Error Handling
- Failed queries are logged but don't stop the entire process
- Backup CSV files are created if database writes fail
- Empty results are expected for some product/year combinations

## Known Issues and Limitations

1. **DS-056121 Empty Data**: EU aggregate production quantities are often not published due to:
   - Statistical confidentiality rules
   - Insufficient country coverage
   - Data quality thresholds

2. **Missing Trade Data**: Some HS codes may have no trade data for specific:
   - Years (especially early years)
   - Partner combinations
   - Flow directions

3. **Unit Conversions**: QNTUNIT values vary by product and must be handled during analysis

4. **Historical Coverage**: 
   - PRODCOM: Available from 1995
   - COMEXT: Only available from 2002

## Database Storage

All fetched data is stored in DuckDB tables:
- PRODCOM: `prodcom_ds_XXXXXX_YYYY` (where XXXXXX is dataset ID, YYYY is year)
- COMEXT: `comext_ds_XXXXXX_YYYY`

Value columns are stored as strings to accommodate both numeric values and unit indicators.