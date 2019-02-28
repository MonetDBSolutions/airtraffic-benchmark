#!/bin/bash

FARMDIR="${1?Usage: start-farm.sh FARM_TOP_LEVEL}"

set -e

FARM="$FARMDIR/$(hostname)"

if [ ! -d "$FARM" ]; then
	mkdir -p "$FARM"
fi

if [ ! -f "$FARM/.merovingian_properties" ]; then
	monetdbd create "$FARM"
	monetdbd set listenaddr=0.0.0.0 "$FARM"
fi

if monetdbd get status "$FARM" | grep "no monetdbd is serving this dbfarm" >/dev/null; then
	monetdbd start "$FARM"
fi

#echo "Farm $FARM is running"

sleep 40000000 &
echo $! >"$FARM/sleep.pid"
wait 2>/dev/null
exit 0
