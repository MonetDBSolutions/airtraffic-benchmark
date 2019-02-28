#!/bin/sh

DB="${1?first parameter: db name}"

set -e

HOSTNAME="$(hostname)"
MAPI_URL="$(monetdb status "$DB" | sed -n -e 's,.*\(mapi:monetdb://.*\),\1,p')"

echo "$HOSTNAME $MAPI_URL"
