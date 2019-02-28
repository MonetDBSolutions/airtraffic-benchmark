#!/bin/sh

set -e

: "${SLURM_JOB_ID?This script is meant to be run as a Slurm Job}"

USAGE='Usage: sbatch -N3 .../path/to/atraf-batch.sh EXPERIMENTNAME SUBSET DURATION [GENERATE_OPTS...]'

EXPERIMENT="${1?    $USAGE}"
SUBSET="${2?    $USAGE}"
DURATION="${3?    $USAGE}"
shift; shift; shift
 
SCRIPTS="$(cd "$(dirname "$0")"; pwd)"
# assume the scripts dir is contained in the atraf dir
ATRAF_DIR="$(cd "$SCRIPTS/.."; pwd)"
FARM_DIR="$PWD/farms"
GEN_DIR="$PWD/gen/$EXPERIMENT"

DBNAME="$EXPERIMENT"

# STEP 0: is MonetDB on the path?
sh -c "monetdbd help" >/dev/null
sh -c "mserver5 --help" 2>/dev/null
sh -c "mclient --help" 2>/dev/null

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
mkdir -p "$GEN_DIR"
srun "$SCRIPTS/nodefile-entry.sh" "$DBNAME" \
        | sort | sed -e '1s/$/ nodata/' \
                     >"$GEN_DIR/nodefile"
MASTER_NODE="$(sed -n -e '1s/ .*//p' "$GEN_DIR/nodefile")"

"$ATRAF_DIR"/generate.py "$GEN_DIR/nodefile" "$SUBSET" "$GEN_DIR" "$@"
#
# In general, copy these files to the workers.
# For now we assume a shared filesystem

# STEP 4: Create and load the database
srun "$SCRIPTS/prepare-data.sh" "$GEN_DIR"

echo '==================================='

srun -N 1 -w "$MASTER_NODE" -D "$GEN_DIR" ./bench.py "$EXPERIMENT" "$DBNAME" "$DURATION"

echo '==================================='

srun "$SCRIPTS/stop-farm.sh" "$FARM_DIR"
wait $FARM_PID

