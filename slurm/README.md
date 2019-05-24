Slurm tooling
=============

This directory contains some helper scripts for running the Air Traffic
Benchmark in a Slurm cluster.

This is not trivial because the database schema needs to contain the network
addresses of the nodes, but we only know which nodes we will run on when the
job has already started. This means we need to generate the schema etc during
the job instead of in advance.

Currently the scripts assume to run on a shared file system but with some work
it would be possible to remove this restriction.

Description of the scripts
--------------------------

**atraf-batch.sh** This is the toplevel script that ties everything together.

**start-farm.sh** Invoked by `atraf-batch.sh` on all nodes to start the
`monetdbd` there.  It creates the farm if it doesn't exist and sets the listen
address to 0.0.0.0 so it can interact with the other nodes. Because slurm kills
all processes when a job step ends, this script does not terminate by itself.
Instead, the `srun` command that dispatches it is itself run in the background
to keep `monetdbd` running until the end of the job.

```
    srun -l "$SCRIPTS_DIR/start-farm.sh" "$FARM_DIR" &
```

**await-farm.sh** Because start-farm.sh is started in the background we don't
know when MonetDB is up.  This script is run on all nodes and waits until the
local `monetdbd` is reachable. The main script resumes when all nodes have
returned succesfully.

**create-db.sh** Run on all nodes. Creates the database used for the experiment.

**nodefile-entry.sh** Run on all nodes. Prints a single line of the form
`HOSTNAME MAPI_URL`. The output of the all nodes is concatenated to become the
nodefile. The main script then adds `nodata` to the first line which will become
the master node.

Then the master script runs generate.py to generate the schema files etc. It
takes the mapi urls from the generated node file so the nodes can access each
other All other nodes will access the generated files through the shared file
system.

**prepare-data.sh** Run on all nodes. Downloads the required data and
initializes the database.

If traces are being collected, the master script also enables the profiler on
all nodes. This does not require a helper script.

Then it starts the regular **bench.py** to run the benchmark.

After the benchmark, if `$DROP_AFTER` is `yes`, the databases are stopped and
destroyed. Finally, **stop-farm.sh** is run on all nodes, which reads the pid
file left by **start-farm.sh** to stop `monetdbd`.

Driver script
-------------

The `atraf-batch.sh` requires quite some configuration. It takes this via
environment variables. The easiest way to arrange this is to write an additional
wrapper shell script which is specific to the environment you are running the
experiment in which sets all these variables and then invokes `atraf-batch.sh`.
This script, typically called `driver.sh`, is then invoked using Slurms `sbatch`
command. This invocation might look something like this:

```
    sbatch \
        -J singleton \
        -d singleton \
        -t 180 \
        --switches=1 \
        -o 'slurm-%j-2yr_3x.out' \
        -c4 -n 3 \
        ./driver.sh single_2yr_3x 2yr 3600
```

It is often convenient to also add some lines to driver.sh which for example
move the resulting csv files to central location, etc.

Here is the driver.sh used in a recent experiment:

```bash
#!/bin/bash

set -e

unset LC_ALL LANG LC_COLLATE LC_NUMERIC LC_TIME LC_MONETARY LC_MESSAGES
export LANG=C LC_CTYPE=C LC_TIME=C

# this sets the path to MonetDB
source /etc/profile
module load MonetDB/11.31.13-gmpolf-2018b

# Because other users may use this script for debugging the slurm cluster,
# we have to be careful where we try to write data.
# BASE is readonly, HOME is writable.

BASE=/home/mdbs/ATRAF
export ATRAF_DIR="$BASE/airtraffic-benchmark"   # readonly
DOWNLOAD_DIR="$BASE/atraf-data"                 # already downloaded, readonly

export FARM_DIR="$HOME/atraf-dbfarms"           # writable
export EXPERIMENTS_DIR="$HOME/atraf-experiments" # writable
export RESULTS_DIR="$HOME/atraf-results"        # writable

export DROP_AFTER=yes

mkdir -p "$RESULTS_DIR"

srun -l hostname
srun -l killall mserver5 monetdbd || true

if "$ATRAF_DIR/slurm/atraf-batch.sh" "$@" --download-dir="$DOWNLOAD_DIR"
then
        cp -v "$EXPERIMENTS_DIR/$1/$1.csv" "$RESULTS_DIR"
        touch "$HERE/state/$1.ok"
else
        exit 1
fi
```

Finally, this script has to be invoked a number of times for each experiment. I
tend to write yet another experiment-specific shell script to generate all of
these, somewhat like the following:

```bash
#!/bin/bash

set -e

subsets="1mo 3mo 12mo 2yr 4yr 6yr 13yr 20yr all double"
subsets="3mo 12mo 2yr 4yr 6yr 13yr"
subsets="6yr 13yr 20yr all"
sizes="10 7 4 3 1"

echo "#!/bin/bash"
echo

i=0
for s in $subsets
do
        echo "## $s"
        for w in $sizes
        do
                name="${s}_${w}x"
                if [ -f "$HOME/atraf-results/$name.csv" ]; then
                        echo -n "# "
                fi
                echo test -f "$HOME/atraf-results/$name.csv" '||' \
                     sbatch -J "$name" -o "'logs/slurm-%j-$name.out'" -c4 -n $w \
                        ./driver.sh "$name" "$s" 5400
        done
        echo
done
```

The output of this script can then be inspected and passed to a shell for
execution.

Debugging
---------

There's a lot that can go wrong and neither Slurm nor all these scripts are very
transparent. So take it one step at a time.  

1. Get familiar with Slurm. With `srun`, `salloc` and `sbatch`.  The `-n` and
   `-c` options. Use for example srun -n4 nproc to see how many cpu cores you
   get assigned.  Use the `-c` option to control this explicitly if needed.

2. Allocate some nodes for interactive use with `salloc`. Within the salloc,
   execute the commands from `start-batch.sh` manually. See how (not whether!)
   they fail and fix them. Write a driver.sh and try to get `start-batch.sh` to
   work when invoked manually from within the salloc.

3. Invoke the `driver.sh` outside the salloc, with `sbatch`. Subtle differences
   will come up. Fix them.

4. Optional but recommended. Write a gen.sh to generate the sbatch invocations.
   You are going to run lots of batches and you are going to have to revisit
   this so automating this step will save you time even if it sounds like
   overkill.

5. Run your experiment.  I often keep something like
   `watch squeue -u mdbs -t ALL -S i`
   running to keep an eye on progress.

