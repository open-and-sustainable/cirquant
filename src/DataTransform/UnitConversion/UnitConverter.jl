module UnitConverter

using DataFrames

export convert_to_tonnes, get_unit_name, is_convertible_to_tonnes, UNIT_CONVERSION_FACTORS

"""
Unit conversion factors to tonnes based on PRODCOM metadata specifications.
Each entry contains: unit_code => (conversion_factor, unit_name, unit_description)
"""
const UNIT_CONVERSION_FACTORS = Dict{String, Tuple{Float64, String, String}}(
    # Mass units - direct conversion to tonnes
    "1000" => (1.0, "GT", "Gross tonnage"),
    "1050" => (1.0, "CGT", "Compensated Gross Tonne"),
    "1100" => (2.0e-7, "c/k", "Carats (1 metric carat = 2×10⁻⁴ kg)"),
    "1400" => (1.0e-6, "g", "Gram"),
    "1500" => (1.0e-3, "kg", "Kilogram"),

    # Chemical compound units (all in kg, so 1e-3 conversion to tonnes)
    "1510" => (1.0e-3, "kg Al2O3", "Kilogram of dialuminium trioxide"),
    "1511" => (1.0e-3, "kg B2O3", "Kilogram of diboron trioxide"),
    "1512" => (1.0e-3, "kg BaCO3", "Kilogram of barium carbonate"),
    "1513" => (1.0e-3, "kg Cl", "Kilogram of chlorine"),
    "1514" => (1.0e-3, "kg F", "Kilogram of fluorine"),
    "1515" => (1.0e-3, "kg HCl", "Kilogram of hydrogen chloride"),
    "1516" => (1.0e-3, "kg H2O2", "Kilogram of hydrogen peroxide"),
    "1517" => (1.0e-3, "kg KOH", "Kilogram of potassium hydroxide (caustic potash)"),
    "1518" => (1.0e-3, "kg K2O", "Kilogram of potassium oxide"),
    "1519" => (1.0e-3, "kg K2CO3", "Kilogram of potassium carbonate"),
    "1520" => (1.0e-3, "kg N", "Kilogram of nitrogen"),
    "1521" => (1.0e-3, "kg NaOH", "Kilogram of sodium hydroxide (caustic soda)"),
    "1522" => (1.0e-3, "kg Na2CO3", "Kilogram of sodium carbonate"),
    "1523" => (1.0e-3, "kg Na2S2O5", "Kilogram of sodium pyrosulphide"),
    "1524" => (1.0e-3, "kg PbO", "Kilogram of lead oxide"),
    "1525" => (1.0e-3, "kg P2O5", "Kilogram of phosphorus pentoxide (phosphoric anhydride)"),
    "1526" => (1.0e-3, "kg S", "Kilogram of sulphur"),
    "1527" => (1.0e-3, "kg SO2", "Kilogram of sulphur dioxide"),
    "1528" => (1.0e-3, "kg SiO2", "Kilogram of silicon dioxide"),
    "1529" => (1.0e-3, "kg TiO2", "Kilogram of titanium dioxide"),
    "1530" => (1.0e-3, "kg act. subst.", "Kilogram of active substance"),
    "1531" => (1.0e-3, "kg 90 % sdt", "Kilogram of substance 90% dry"),
    "1532" => (1.0e-3, "kg HF", "Kilogram of hydrogen fluoride"),
    "1534" => (1.0e-3, "kg H2SO4", "Kilogram of sulfuric acid"),

    # Volume units - require density assumptions
    "2000" => (1.0e-3, "l", "Litre (assuming water density ~1 kg/l)"),
    "2100" => (0.789e-3, "l alc 100%", "Litre pure (100%) alcohol (density ~0.789 kg/l)"),
    "2400" => (1.0, "m³", "Cubic metre (assuming water density ~1000 kg/m³)"),

    # Units that cannot be directly converted to mass
    "1200" => (NaN, "ce/el", "Number of elements"),
    "1300" => (NaN, "ct/l", "Carrying capacity in tonnes"),
    "1700" => (NaN, "km", "Kilometer"),
    "1800" => (NaN, "kW", "Kilowatt"),
    "1900" => (NaN, "1 000 kWh", "1 000 kilowatt hours"),
    "2200" => (NaN, "m", "Metre"),
    "2300" => (NaN, "m²", "Square metre"),
    "2500" => (NaN, "pa", "Number of pairs"),
    "2600" => (NaN, "p/st", "Number of items"),
    "2900" => (NaN, "TJ", "Terajoule (gross calorific value)")
)

# Additional common unit abbreviations that might appear in data
const UNIT_ALIASES = Dict{String, String}(
    "t" => "1000",          # tonnes
    "GT" => "1000",         # Gross tonnage
    "CGT" => "1050",        # Compensated Gross Tonne
    "g" => "1400",          # gram
    "kg" => "1500",         # kilogram
    "l" => "2000",          # litre
    "m3" => "2400",         # cubic metre
    "p/st" => "2600",       # pieces/items
    "pa" => "2500",         # pairs
    "m" => "2200",          # metre
    "m2" => "2300",         # square metre
    "kW" => "1800",         # kilowatt
    "TJ" => "2900"          # terajoule
)

"""
    convert_to_tonnes(value::Number, unit_code::String; default_factor::Float64=NaN)

Convert a value from its original unit to tonnes using PRODCOM unit codes.

# Arguments
- `value`: The numeric value to convert
- `unit_code`: The PRODCOM unit code (e.g., "1500" for kg) or unit abbreviation
- `default_factor`: Default conversion factor if unit is not found (default: NaN)

# Returns
- Converted value in tonnes, or NaN if conversion is not possible
"""
function convert_to_tonnes(value::Number, unit_code::String; default_factor::Float64=NaN)
    # Handle missing or zero values
    if ismissing(value) || isnan(value) || value == 0
        return value
    end

    # Clean and normalize the unit code
    unit_code = strip(unit_code)

    # Check if it's an alias first
    if haskey(UNIT_ALIASES, unit_code)
        unit_code = UNIT_ALIASES[unit_code]
    end

    # Get conversion factor
    if haskey(UNIT_CONVERSION_FACTORS, unit_code)
        factor, _, _ = UNIT_CONVERSION_FACTORS[unit_code]
        if isnan(factor)
            return NaN  # Unit cannot be converted to mass
        else
            return value * factor
        end
    else
        return isnan(default_factor) ? NaN : value * default_factor
    end
end

"""
    convert_to_tonnes(df::DataFrame, value_col::Symbol, unit_col::Symbol;
                     output_col::Symbol=:value_tonnes, keep_original::Bool=true)

Convert values in a DataFrame column to tonnes based on unit codes in another column.

# Arguments
- `df`: Input DataFrame
- `value_col`: Name of column containing numeric values
- `unit_col`: Name of column containing unit codes
- `output_col`: Name for the output column with converted values (default: :value_tonnes)
- `keep_original`: Whether to keep the original value column (default: true)

# Returns
- DataFrame with added column containing values converted to tonnes
"""
function convert_to_tonnes(df::DataFrame, value_col::Symbol, unit_col::Symbol;
                          output_col::Symbol=:value_tonnes, keep_original::Bool=true)
    result_df = copy(df)

    # Initialize output column
    result_df[!, output_col] = Vector{Union{Float64, Missing}}(undef, nrow(df))

    # Convert each row
    for i in 1:nrow(df)
        if ismissing(df[i, value_col]) || ismissing(df[i, unit_col])
            result_df[i, output_col] = missing
        else
            result_df[i, output_col] = convert_to_tonnes(
                df[i, value_col],
                string(df[i, unit_col])
            )
        end
    end

    # Remove original column if requested
    if !keep_original && value_col != output_col
        select!(result_df, Not(value_col))
    end

    return result_df
end

"""
    get_unit_name(unit_code::String)

Get the human-readable name for a unit code.

# Returns
- Tuple of (short_name, full_description) or ("Unknown", "Unknown unit") if not found
"""
function get_unit_name(unit_code::String)
    unit_code = strip(unit_code)

    # Check aliases first
    if haskey(UNIT_ALIASES, unit_code)
        unit_code = UNIT_ALIASES[unit_code]
    end

    if haskey(UNIT_CONVERSION_FACTORS, unit_code)
        _, name, desc = UNIT_CONVERSION_FACTORS[unit_code]
        return (name, desc)
    else
        return ("Unknown", "Unknown unit")
    end
end

"""
    is_convertible_to_tonnes(unit_code::String)

Check if a unit code can be converted to tonnes.

# Returns
- true if the unit can be converted to tonnes, false otherwise
"""
function is_convertible_to_tonnes(unit_code::String)
    unit_code = strip(unit_code)

    # Check aliases first
    if haskey(UNIT_ALIASES, unit_code)
        unit_code = UNIT_ALIASES[unit_code]
    end

    if haskey(UNIT_CONVERSION_FACTORS, unit_code)
        factor, _, _ = UNIT_CONVERSION_FACTORS[unit_code]
        return !isnan(factor)
    else
        return false
    end
end

"""
    get_all_convertible_units()

Get a DataFrame of all units that can be converted to tonnes.

# Returns
- DataFrame with columns: unit_code, unit_name, description, conversion_factor
"""
function get_all_convertible_units()
    units_data = []

    for (code, (factor, name, desc)) in UNIT_CONVERSION_FACTORS
        if !isnan(factor)
            push!(units_data, (
                unit_code = code,
                unit_name = name,
                description = desc,
                conversion_factor = factor
            ))
        end
    end

    return DataFrame(units_data)
end

"""
    get_non_convertible_units()

Get a DataFrame of all units that cannot be converted to tonnes.

# Returns
- DataFrame with columns: unit_code, unit_name, description
"""
function get_non_convertible_units()
    units_data = []

    for (code, (factor, name, desc)) in UNIT_CONVERSION_FACTORS
        if isnan(factor)
            push!(units_data, (
                unit_code = code,
                unit_name = name,
                description = desc
            ))
        end
    end

    return DataFrame(units_data)
end

end # module UnitConverter
