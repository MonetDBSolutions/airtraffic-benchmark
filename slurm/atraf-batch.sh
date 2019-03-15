#!/bin/bash

# Run the Air Traffic benchmark in a Slurm cluster
#
# Note: when run from within salloc(1), this script runs on the
# controller node, in the directory where salloc was invoked.
# When run from sbatch(1), it runs on one of the worker nodes,
# in a temporary directory.
#
# There are three relevant file system locations, configured using
# environment variables:
#
# ATRAF_DIR: location of the air traffic benchmark
#
# FARM_DIR: the database farm will be created in $FARM_DIR/$(hostname)
# (The additional hostname part makes it easier to place the farm dir
# on a shared file system, though a local filesystem will be faster.)
#
# EXPERIMENTS_DIR: the output of generate.py for a given experiment will be
# generated in the directory $EXPERIMENTS_DIR/$EXPERIMENT_NAME.  For the time
# being this MUST be on a shared file system.
#
# SCRIPTS_DIR: location of the helper scripts. Default: $ATRAF_DIR/slurm.

USAGE='Usage: sbatch -N3 atraf-batch.sh EXPERIMENTNAME SUBSET DURATION [GENERATE_OPTS...]'

: "${SLURM_JOB_ID?This script is meant to be run as a Slurm Job}"

set -e -x

EXPERIMENT="${1?    $USAGE}"
SUBSET="${2?    $USAGE}"
DURATION="${3?    $USAGE}"
shift; shift; shift

: "ATRAF_DIR=${ATRAF_DIR?}"
: "FARM_DIR=${FARM_DIR?}"
: "EXPERIMENTS_DIR=${EXPERIMENTS_DIR?}"
: "SCRIPTS_DIR=${SCRIPTS_DIR:=$ATRAF_DIR/slurm}"

test -f "$ATRAF_DIR/generate.py"

WORK_DIR="$EXPERIMENTS_DIR/$EXPERIMENT"
DBNAME="$EXPERIMENT"

# STEP 1: Make sure MonetDB is running
#
# Note: background job! Slurm will kill it when we're done
srun -l "$SCRIPTS_DIR/start-farm.sh" "$FARM_DIR" &
FARM_PID=$!

# Foreground job which waits until the farm is up
srun -l "$SCRIPTS_DIR/await-farm.sh" "$FARM_DIR"

# STEP 2: Create the database
srun -l "$SCRIPTS_DIR/create-db.sh" "$DBNAME"

# STEP 3: Generate the airtraffic files
mkdir -p "$WORK_DIR"
srun "$SCRIPTS_DIR/nodefile-entry.sh" "$DBNAME" \
        | sort | sed -e '1s/$/ nodata/' \
                     >"$WORK_DIR/nodefile"
MASTER_NODE="$(sed -n -e '1s/ .*//p' "$WORK_DIR/nodefile")"

"$ATRAF_DIR"/generate.py "$WORK_DIR/nodefile" "$SUBSET" "$WORK_DIR" "$@"
#
# In general, copy these files to the workers.
# For now we assume a shared filesystem

# STEP 4: Create and load the database
srun -l "$SCRIPTS_DIR/prepare-data.sh" "$WORK_DIR"

echo '==================================='

#srun -N1 -n1 --pty bash
srun -n1 -N1 -w "$MASTER_NODE" -D "$WORK_DIR" \
     ./bench.py "$DBNAME" --name "$EXPERIMENT" --duration "$DURATION" --max-queries 10000

echo '==================================='

srun -l "$SCRIPTS_DIR/stop-farm.sh" "$FARM_DIR"
wait $FARM_PID

echo DONE
