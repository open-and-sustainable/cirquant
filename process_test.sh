#!/bin/bash

set -euo pipefail

YEARS=${1:-"2022-2023"}
SOURCE_DB=${2:-"CirQuant-database/raw/test.duckdb"}
TARGET_DB=${3:-"CirQuant-database/processed/test.duckdb"}

LOG_FILE=${4:-"process_test.log"}

rm -f "$TARGET_DB"

julia --project=. -e "push!(LOAD_PATH, \"src\"); using CirQuant; process_data(\"$YEARS\"; source_db=\"$SOURCE_DB\", target_db=\"$TARGET_DB\")" 2>&1 | tee "$LOG_FILE"
