#!/bin/bash

set -e

while ! nc -z localhost 50000; do
        sleep 1
done
