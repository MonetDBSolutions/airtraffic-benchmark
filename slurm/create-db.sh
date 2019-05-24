#!/bin/bash

: "${1?Usage: create-db.sh DBNAME [TRACES_DIR]}"
TRACES_DIR="$2"


set -e

# This will stop the script if there is something wrong with the
# monetdb command
monetdb status >/dev/null

if monetdb create "$1" 2>/dev/null; then
        # newly created
        monetdb release "$1"
        if [ "$TRACES_DIR" != "" ]; then
                T="$TRACES_DIR/$(hostname)"
                rm -rf "$T"
                mkdir -p "$T"
                monetdb set profilerlogpath="$T" "$1"
                monetdb set profilerbeatfreq=50 "$1"
                monetdb set mal_for_all=yes "$1"
        fi
fi
