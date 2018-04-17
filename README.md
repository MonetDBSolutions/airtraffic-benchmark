Airtraffic Benchmark
====================

Scripts to run the Air Traffic benchmark on a MonetDB cluster.

Bla bla explanation Air Traffic.


How to set up the benchmark
---------------------------

Note: many of these steps can be [automated with Ansible](ansible.md).

First make sure MonetDB is installed and running on all nodes
and that a database has been created on each of them.

Then, on your local system or any other convenient location, create
a nodefile which lists the node names with the corresponding
mapi url and whether the node should store any data:

```
    node1 mapi:monetdb://node1:50000/atraf nodata
    node2 mapi:monetdb://node2:50000/atraf data
    node3 mapi:monetdb://node3:50000/atraf data
```

In the above we have a 'master node' node1 which stores no data
but coordinates the other nodes, and two 'slave nodes' node2 and
node3 which hold data.  In this setup, the queries would normally
be executed on node1 though the other are also able to.

Then run `generate.py NODEFILE SUBSET OUTPUTDIR`.  NODEFILE is the
above file.  SUBSET is the name of one of the subdirectories of
the `subsets` directory, for example `1mo`, `3mo`, `2yr`, `13yr`
or `all`.  Each of these contains a list of data files to load
and the expected answers for the queries when run on that data.

The command will create OUTPUTDIR if necessary and populate it with
various files, in particular SQL files and a Makefile.  This directory
should be copied to all nodes.

Now on each node you can:

- run `make ping-NODENAME` to verify connectivity to a specific node.

- run `make ping` to verify connectivity to the current node,
  as identified by the `hostname` command.

- run `make ping-all` to verify connectivity to all nodes.

- run `make download-NODENAME` to download and decompress the data files for
  the given node.  This benefits from parallellism, so you can run
  `make -j8 download-node1` to download and decompress 8 files simultaneously.

- run `make download` to run the downloads for the current node,
  as identified by the `hostname` command or the `NODENAME=` parameter to Make.

- run `make download-all` to run the downloads for all nodes on the current node.
  This can be convenient if there is a shared filesystem across all nodes.

- run `make schema-NODENAME` to execute the CREATE statements for a node.

- run `make schema` to execute the CREATE statements for the current node.

- run `make schema-all` to execute the CREATE statements for all nodes.

- run `make drop-NODENAME` to drop the schema for the given node.

- run `make drop` to drop the schema for the current node.

- run `make drop-all` to drop the schema on all nodes.

- run `make insert` to execute the COPY INTO statements
  that read the downloaded data for the given current node.

The `make schema` and `make insert` steps are separate because that
turns out to most convenient when troubleshooting.

There is no `make insert-all` because the COPY INTO statement works
with absolute paths and the nodes do not know where on the other nodes
the data is stored.

- run `make validate` on the master node to run the queries and
  compare the results with expected results.

The recommended sequence is to run the following steps on each node:
`make ping-all`, `make download`, `make schema` and `make insert`.
Then, on the master node, `make validate`.

The Makefile accepts an additional parameter `NODENAME=bla` that can
be used to override the output of the `hostname` command.  There are
more settings, these are all documented at the top of the Makefile.


How to run the benchmark
------------------------

When the benchmark has been set up and `make ping-all` and `make validate`
report no errors, you can run the benchmark by running

```
    bench.py CONFIG DB [TIMEOUT]
```

CONFIG is a label which is used to determine the name of the output file
`CONFIG.csv`.  To simplify concatenating and comparing the output of
different experiments, the config name is also listed as the first field
of every row in the output file.

DB is the name or url of the database to connect to.

Finally, the optional TIMEOUT gives the number of seconds after which the
benchmark script will stop sending more queries.  Typical values would be
300 (5 minutes) or 1800 (30 minutes).
