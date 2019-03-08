#!/bin/sh

set -e

if [ "$1" != "$(hostname)" ]; then
        exit 0
fi

shift;
exec "$@"
