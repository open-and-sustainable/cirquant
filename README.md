# ![DatAdapt_logo](images/CirQuant_logo_vsmall.png) CirQuant

## Project Description
https://doi.org/10.17605/OSF.IO/U6SF3

## Architecture Overview

CirQuant has been refactored to use the external ProdcomAPI package for fetching data from the Eurostat PRODCOM API. This separation of concerns allows for:

1. A cleaner codebase with focused responsibilities
2. The ability to use the ProdcomAPI package independently of CirQuant
3. Simplified maintenance and updates

### Components:

- **ProdcomAPI**: External package that handles communication with the Eurostat PRODCOM API
- **CirQuant**: This package, which focuses on:
  - Fetching data using ProdcomAPI
  - Storing data in the raw DuckDB database
  - Transforming and analyzing the data

## Usage

### Data Fetching

```julia
# Import the package
using CirQuant

# Fetch default datasets (ds-056120 and ds-056121) for years 2020-2021
fetch_prodcom_data("2020-2021")

# Fetch specific datasets
fetch_prodcom_data("2022-2022", ["ds-056120"])

# Fetch data without saving to database (for exploration)
df = CirQuant.fetch_prodcom_dataset("ds-056120", 2022)

# Get information about available datasets
datasets = CirQuant.get_available_datasets()
```

For a complete example, see `fetch_example.jl` in the repository.

## Getting Started

1. Clone this repository
2. Install dependencies:
   ```julia
   using Pkg
   Pkg.activate(".")
   Pkg.instantiate()
   ```
3. Run the example script:
   ```
   julia fetch_example.jl
   ```

## License
The software in this repository is licensed under the [MIT license](LICENSE).

The resulting data is licensed under the [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) license because of constraints to commercial use of orginal data.
