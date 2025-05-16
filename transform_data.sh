#!/bin/bash

# Install the PRQL extension in DuckDB, this must be run only once on every computer
#julia --project=. -e 'push!(LOAD_PATH, "src"); using DatabaseAccess; DatabaseAccess.installPRQL_DuckDBextension()' 2>&1 | tee PRQL_extension.log

# Tranform the Damage raw data
julia --project=. -e 'push!(LOAD_PATH, "src"); using DatAdapt; DatAdapt.transform_data("damage")' 2>&1 | tee damage_transform.log
