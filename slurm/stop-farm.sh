#!/bin/sh

FARMDIR="${1?Usage: start-farm.sh FARM_TOP_LEVEL}"

set -e

FARM="$FARMDIR/$(hostname)"

kill $(cat "$FARM/sleep.pid")
