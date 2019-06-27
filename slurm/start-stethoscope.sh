#!/bin/bash

DBNAME="${1?Usage: $0 DBNAME TRACES_DIR}"
TRACES_DIR="${2?Usage: $0 DBNAME TRACES_DIR}"

set -e -x

OUTPUTFILE="$TRACES_DIR/$(hostname)/proflog_$DBNAME__$(date +%Y-%m-%d_%H:%M:%S).json"

mkdir -p "$TRACES_DIR/$(hostname)"
rm -f "$OUTPUTFILE"
stethoscope -d "$DBNAME" -j -b 50 -o "$OUTPUTFILE"
