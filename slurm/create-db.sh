#!/bin/bash

: "${1?Usage: create-db.sh DBNAME}"


set -e

# This will stop the script if there is something wrong with the
# monetdb command
monetdb status >/dev/null

if monetdb create "$1" 2>/dev/null; then
        # newly created
        monetdb release "$1"
else
        # existing
        monetdb start "$1" || true
fi
