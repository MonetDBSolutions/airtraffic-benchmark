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
# GEN_DIR: the output of generate.py for a given experiment will be
# generated in the directory $GEN_DIR/$EXPERIMENT_NAME.  For the time
# being this MUST be on a shared file system.
#

USAGE='Usage: sbatch -N3 atraf-batch.sh EXPERIMENTNAME SUBSET DURATION [GENERATE_OPTS...]'

: "${SLURM_JOB_ID?This script is meant to be run as a Slurm Job}"

set -e -x

EXPERIMENT="${1?    $USAGE}"
SUBSET="${2?    $USAGE}"
DURATION="${3?    $USAGE}"
shift; shift; shift

: "${ATRAF_DIR?}"
: "${FARM_DIR?}"
: "${GEN_DIR?}"

test -f "$ATRAF_DIR/generate.py"

SCRIPTS="$ATRAF_DIR/slurm"
EXP_DIR="$GEN_DIR/$EXPERIMENT"
DBNAME="$EXPERIMENT"

# STEP 1: Make sure MonetDB is running
#
# Note: background job! Slurm will kill it when we're done
srun "$SCRIPTS/start-farm.sh" "$FARM_DIR" &
FARM_PID=$!

# Foreground job which waits until the farm is up
srun "$SCRIPTS/await-farm.sh" "$FARM_DIR"

# STEP 2: Create the database
srun -l "$SCRIPTS/create-db.sh" "$DBNAME"

# STEP 3: Generate the airtraffic files
mkdir -p "$EXP_DIR"
srun "$SCRIPTS/nodefile-entry.sh" "$DBNAME" \
        | sort | sed -e '1s/$/ nodata/' \
                     >"$EXP_DIR/nodefile"
MASTER_NODE="$(sed -n -e '1s/ .*//p' "$EXP_DIR/nodefile")"

"$ATRAF_DIR"/generate.py "$EXP_DIR/nodefile" "$SUBSET" "$EXP_DIR" "$@"
#
# In general, copy these files to the workers.
# For now we assume a shared filesystem

# STEP 4: Create and load the database
srun "$SCRIPTS/prepare-data.sh" "$EXP_DIR"

echo '==================================='

# I tried to run bench.py using srun -n 1 -N 1 -w "$MASTER_NODE"
# but it didn't work:
#
#   srun: error: slurm_receive_msgs: Transport endpoint is not connected
#   srun: error: Task launch for 2332.5 failed on node Q1F1: Transport endpoint is not connected
#   srun: error: Application launch failed: Transport endpoint is not connected
#   srun: Job step aborted: Waiting up to 32 seconds for job step to finish.
#   srun: error: Timed out waiting for job step to complete
#
# This happens consistently.
# Script run-on-node is a workaround
srun -D "$EXP_DIR" "$SCRIPTS/run-on-node.sh" "$MASTER_NODE" ./bench.py "$EXPERIMENT" "$DBNAME" "$DURATION"

echo '==================================='

srun "$SCRIPTS/stop-farm.sh" "$FARM_DIR"
wait $FARM_PID

