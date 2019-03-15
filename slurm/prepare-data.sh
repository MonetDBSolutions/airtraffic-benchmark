#!/bin/sh

NPROC="$(nproc || echo 1)" 

set -e

make -j "$NPROC" -C "$1" download
make -C "$1" drop schema insert

echo; echo "Node $(hostname) done loading"; echo
