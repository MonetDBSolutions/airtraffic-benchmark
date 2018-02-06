Airtraffic Benchmark
====================

Scripts to run the Air Traffic benchmark on a MonetDB cluster.

Bla bla explanation Air Traffic.

How to set up
-------------

Note: many of these steps can be automated with Ansible, 
see BLA.

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

TODO: Explain SUBSET

Copy the outputdir to each of the nodes.

Now on each node you can:

- run `make check-NODENAME` to verify connectivity to a specific node.

- run `make check` to verify connectivity to all nodes.

- run `make download-NODENAME` to download and decompress the data files for
  the given node.  Benefits from parallellism, e.g., `make -j8 download-node1`
  to download and decompress 8 files simultaneously.

- run `make downloads` to run all downloads, this can be convenient if there
  is a shared filesystem across all nodes.

- run `make schema-NODENAME` to execute the CREATE statements for a node.

- make `make schemas` to execute the CREATE statements for all nodes.

- run `make insert-NODENAME` to execute the COPY INTO statements
  that read the downloaded data for the given node.

- run `make inserts` to run the COPY INTO's for all nodes.  Benefits from
  parallellism.

The `make schema` and `make insert` steps are separate because it is not
uncommon for one to fail where the other works.

- run `make validate` on the master node to run the queries and
  compare the results with expected results.

- bla
