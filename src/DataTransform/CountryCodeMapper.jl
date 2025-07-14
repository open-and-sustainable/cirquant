module CountryCodeMapper

using DataFrames
using DuckDB, DBInterface

export get_country_code_mapping, harmonize_country_code, create_country_mapping_table, PRODCOM_TO_ISO_MAP, ISO_TO_PRODCOM_MAP

# Mapping from PRODCOM numeric codes to ISO 2-letter codes
const PRODCOM_TO_ISO_MAP = Dict{String, String}(
    "001" => "FR",  # France
    "003" => "NL",  # Netherlands
    "004" => "DE",  # Germany
    "005" => "IT",  # Italy
    "006" => "UK",  # United Kingdom (historical)
    "007" => "IE",  # Ireland
    "008" => "DK",  # Denmark
    "009" => "GR",  # Greece
    "010" => "PT",  # Portugal
    "011" => "ES",  # Spain
    "017" => "BE",  # Belgium
    "018" => "LU",  # Luxembourg
    "024" => "IS",  # Iceland
    "028" => "NO",  # Norway
    "030" => "SE",  # Sweden
    "032" => "FI",  # Finland
    "038" => "AT",  # Austria
    "046" => "MT",  # Malta
    "053" => "EE",  # Estonia
    "054" => "LV",  # Latvia
    "055" => "LT",  # Lithuania
    "060" => "PL",  # Poland
    "061" => "CZ",  # Czechia
    "063" => "SK",  # Slovakia
    "064" => "HU",  # Hungary
    "066" => "RO",  # Romania
    "068" => "BG",  # Bulgaria
    "091" => "SI",  # Slovenia
    "092" => "HR",  # Croatia
    "093" => "BA",  # Bosnia and Herzegovina
    "096" => "MK",  # North Macedonia
    "097" => "ME",  # Montenegro
    "098" => "RS",  # Serbia
    "600" => "CY",  # Cyprus
    "1110" => "EU15",  # EU15 totals
    "2027" => "EU27_2020",  # EU27 totals (2020 composition)
)

# Reverse mapping from ISO codes to PRODCOM numeric codes
const ISO_TO_PRODCOM_MAP = Dict{String, String}(
    v => k for (k, v) in PRODCOM_TO_ISO_MAP
)

# Additional mappings for special cases
const SPECIAL_MAPPINGS = Dict{String, String}(
    "EU27TOTALS" => "EU27_2020",
    "EU15TOTALS" => "EU15",
    "EU27" => "EU27_2020",
)

"""
    harmonize_country_code(code::String, source::Symbol)

Harmonize country codes to ISO 2-letter format.

# Arguments
- `code`: The country code to harmonize
- `source`: Either `:prodcom` or `:comext` to indicate the source system

# Returns
- The harmonized ISO 2-letter code, or the original code if no mapping exists

# Examples
```julia
harmonize_country_code("001", :prodcom)  # Returns "FR"
harmonize_country_code("FR", :comext)     # Returns "FR"
harmonize_country_code("2027", :prodcom)  # Returns "EU27_2020"
```
"""
function harmonize_country_code(code::String, source::Symbol)
    # Clean the input
    code = strip(code)

    if source == :prodcom
        # Check if it's a numeric PRODCOM code
        if haskey(PRODCOM_TO_ISO_MAP, code)
            return PRODCOM_TO_ISO_MAP[code]
        end
        # Check special mappings
        if haskey(SPECIAL_MAPPINGS, code)
            return SPECIAL_MAPPINGS[code]
        end
    elseif source == :comext
        # COMEXT already uses ISO codes, but check for special cases
        if haskey(SPECIAL_MAPPINGS, code)
            return SPECIAL_MAPPINGS[code]
        end
        # Return as-is if it's already an ISO code
        return code
    else
        error("Source must be either :prodcom or :comext")
    end

    # Return original if no mapping found
    @warn "No mapping found for country code: $code (source: $source)"
    return code
end

"""
    get_country_code_mapping()

Get the complete country code mapping as a DataFrame.

# Returns
DataFrame with columns:
- prodcom_code: PRODCOM numeric code
- iso_code: ISO 2-letter code
- country_name: Country name (if available)
"""
function get_country_code_mapping()
    # Create DataFrame from mapping
    prodcom_codes = collect(keys(PRODCOM_TO_ISO_MAP))
    iso_codes = [PRODCOM_TO_ISO_MAP[code] for code in prodcom_codes]

    # Country names (can be expanded)
    country_names = Dict{String, String}(
        "FR" => "France",
        "NL" => "Netherlands",
        "DE" => "Germany",
        "IT" => "Italy",
        "UK" => "United Kingdom",
        "IE" => "Ireland",
        "DK" => "Denmark",
        "GR" => "Greece",
        "PT" => "Portugal",
        "ES" => "Spain",
        "BE" => "Belgium",
        "LU" => "Luxembourg",
        "IS" => "Iceland",
        "NO" => "Norway",
        "SE" => "Sweden",
        "FI" => "Finland",
        "AT" => "Austria",
        "MT" => "Malta",
        "EE" => "Estonia",
        "LV" => "Latvia",
        "LT" => "Lithuania",
        "PL" => "Poland",
        "CZ" => "Czechia",
        "SK" => "Slovakia",
        "HU" => "Hungary",
        "RO" => "Romania",
        "BG" => "Bulgaria",
        "SI" => "Slovenia",
        "HR" => "Croatia",
        "BA" => "Bosnia and Herzegovina",
        "MK" => "North Macedonia",
        "ME" => "Montenegro",
        "RS" => "Serbia",
        "CY" => "Cyprus",
        "EU15" => "EU15 Total",
        "EU27_2020" => "EU27 Total (2020)",
    )

    # Get country names
    names = [get(country_names, iso, "") for iso in iso_codes]

    return DataFrame(
        prodcom_code = prodcom_codes,
        iso_code = iso_codes,
        country_name = names
    )
end

"""
    create_country_mapping_table(db_path::String; table_name::String = "country_code_mapping")

Create a country code mapping table in the database.

# Arguments
- `db_path`: Path to the DuckDB database
- `table_name`: Name of the table to create (default: "country_code_mapping")
"""
function create_country_mapping_table(db_path::String; table_name::String = "country_code_mapping")
    # Get mapping as DataFrame
    mapping_df = get_country_code_mapping()

    # Connect to database
    conn = DBInterface.connect(DuckDB.DB, db_path)

    try
        # Create table
        DBInterface.execute(conn, "DROP TABLE IF EXISTS $table_name")

        create_query = """
        CREATE TABLE $table_name (
            prodcom_code VARCHAR PRIMARY KEY,
            iso_code VARCHAR NOT NULL,
            country_name VARCHAR
        )
        """
        DBInterface.execute(conn, create_query)

        # Insert data
        for row in eachrow(mapping_df)
            insert_query = """
            INSERT INTO $table_name (prodcom_code, iso_code, country_name)
            VALUES ('$(row.prodcom_code)', '$(row.iso_code)', '$(row.country_name)')
            """
            DBInterface.execute(conn, insert_query)
        end

        @info "Created country code mapping table '$table_name' with $(nrow(mapping_df)) entries"
    finally
        DBInterface.close!(conn)
    end
end

end # module CountryCodeMapper
