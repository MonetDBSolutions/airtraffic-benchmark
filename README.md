Airtraffic Benchmark
====================

Scripts to run the Air Traffic benchmark on a MonetDB cluster.

Bla bla explanation Air Traffic.

How to set up
-------------

Note: many of these steps can be [automated with Ansible](ansible.md).

First make sure MonetDB is installed and running on all nodes
and that a database has been created on each of them.

Then, on your local system or any other convenient location, create
a nodefile which lists the node names with the corresponding
mapi url:

```
    node1 mapi:monetdb://node1:50000/atraf
    node2 mapi:monetdb://node2:50000/atraf
    node3 mapi:monetdb://node3:50000/atraf
```

Then run `generate.py NODEFILE SUBSET OUTPUTDIR`.  This will create
OUTPUTDIR if necessary and populate it with various files,
in particular SQL files and a Makefile.

In the above command, `SUBSET` refers to one of the subdirectories
of the `subsets` directory, for example `1mo`, `3mo`, `12mo`, `2yr`,
`4yr`, `6yr` and `13yr`.  Each of these contains a list of data files
to load and the expected answers for the queries when run on that data.

After running the script, copy the outputdir to each of the nodes.

Now on each node you can:

- run `make ping-NODENAME` to verify connectivity to a specific node.

- run `make ping` to verify connectivity to the current node,
  as identified by the `hostname` command.

- run `make ping-all` to verify connectivity to all nodes.

- run `make download-NODENAME` to download and decompress the data files for
  the given node.  This benefits from parallellism, so you can run
  `make -j8 download-node1` to download and decompress 8 files simultaneously.

- run `make download` to run the downloads for the current node,
  as identified by the `hostname` command.

- run `make download-all` to run the downloads for all nodes on the current node.
  This can be convenient if there is a shared filesystem across all nodes.

- run `make schema-NODENAME` to execute the CREATE statements for a node.

- run `make schema` to execute the CREATE statements for the current node
  as identified by the `hostname` command.

- run `make schema-all` to execute the CREATE statements for all nodes.

- run `make drop-NODENAME` to drop the schema for the given node.

- run `make drop` to drop the schema for the current node
  as identified by the `hostname` command.

- run `make drop-all` to drop the schema on all nodes.

- run `make insert` to execute the COPY INTO statements
  that read the downloaded data for the given current node.

- run `make insert-NODENAME` to execute the COPY INTO statements
  that read the downloaded data for the given node.

- run `make insert-all` to run the COPY INTO's for all nodes.  Benefits from
  parallellism.

The `make schema` and `make insert` steps are separate because that
turns out to most convenient when trouble shooting.

- run `make validate` on the master node to run the queries and
  compare the results with expected results.

- bla
