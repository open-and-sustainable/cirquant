# CirQuant Roadmap - Missing Features

This document outlines key features and improvements that would enhance the CirQuant framework's capabilities and data quality.

## Priority Features

### 1. Data-Driven Current Circularity Rates

**Current State**: Current circularity rates are hardcoded in `config/products.toml` as assumptions.

**Proposed Improvement**: Fetch actual circularity/recycling rates from statistical sources:
- Integrate Eurostat waste statistics and material flow accounts
- Query circular material use rate (CMU) indicators from Eurostat
- Match product categories to waste codes and recycling statistics
- Calculate product-specific rates from waste treatment data

**Benefits**:
- Replace assumptions with measured data
- Enable tracking of circularity progress over time
- Improve credibility and objectivity of analysis
- Allow validation against official statistics

**Potential Data Sources**:
- Eurostat dataset: `env_wasrt` (Waste treatment statistics)
- Eurostat dataset: `env_ac_cur` (Circular material use rate)
- EEA waste statistics for specific product categories
- National recycling rate databases

### Future Enhancements

Additional features may be considered based on user needs and data availability.