#!/bin/bash

set -e

FARM="$PWD/FARM"

if [ $# = 0 ]; then
	echo "Usage: $0 NODEFILE..."
	echo "   Starts mserver5's as described by the NODEFILE,"
	echo "   with dbdir's under ./FARM."
	echo "   All nodes in the NODEFILE must be on localhost,"
	echo "   with different ports."
	exit 1
fi


killall -9 monetdbd mserver5 || true
rm -rf "$FARM"
mkdir "$FARM"
touch "$FARM/pids"

for f in "$@"
do
	echo "Reading node file $f"
	cat "$f" | while read node url datanodata
	do
		tail="${url##*/}"
		if [ "x$tail" != "x$node" ]; then
			echo "This script only works if the node name matches the url"
			echo "Maybe rename node '$node' to '$tail'?"
			exit 1
		fi
		tmp="${url%/*}"
		port="${tmp##*:}"
		echo "    starting $node on port $port (url $url)"
		mserver5 \
			--dbpath="$FARM/$node" \
			--set mapi_port="$port" \
			4>&1 >"$FARM/$node.log" \
			&
		echo "kill $!" >>"$FARM/pids"
	done
done
