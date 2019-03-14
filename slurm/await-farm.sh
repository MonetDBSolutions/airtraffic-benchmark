#!/bin/bash

set -e

connect() {
        monetdb status >/dev/null 2>/dev/null
}

connect && exit 0

echo "$(hostname): waiting for MonetDB to start"

seq 120 | while ! connect; do sleep 1; done

if connect
then
        echo "MonetDB is running"
else
        exit 1
fi
