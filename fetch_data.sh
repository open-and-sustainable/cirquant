#!/bin/bash

# Get years parameter with default value
YEARS=${1:-"1995-2023"}

# Run the Julia script and call the fetch_prodcom_data function with years parameter
julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; CirQuant.fetch_prodcom_data(\"$YEARS\")" 2>&1 | tee prodcom.log
