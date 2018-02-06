#!/usr/bin/env python2

import os
import sys

import schema_sql

class ErrMsg(Exception):
    def __init__(self, msg):
	self.message = msg

class Config:
    # Use __slots__ so misspelled attributes give an error
    __slots__ = [
    	'basedir', 
	'nodefile', 
	'subset', 
	'subsetdir', 
	'outputdir',
	'outputsetdir',
	'nodes',
	'masternode',
	'distributed',
	'urls',
	'binprefixes',
	'parts',
	'partitions',
    ]

    def __init__(self):
	self.urls = {}
	self.binprefixes = {}
	self.nodes = []
	self.parts = []
	self.partitions = {}

    def __getitem__(self, i):
	return getattr(self, i)

    def dump(self):
	for a in self.__slots__:
	    print "%s = %r" % (a, getattr(self, a, None))

    def for_node(self, node):
	return NodeConfig(self, node)

class NodeConfig(Config):
    __slots__ = Config.__slots__ + [
	'node',
	'url',
	'binprefix',
	'partition',
	'node_suffix',
    ]

    def __init__(self, config, node):
	for a in Config.__slots__:
	    if hasattr(config, a):
		setattr(self, a, getattr(config, a))
	self.node = node
	self.url = config.urls[node]
	self.binprefix = config.binprefixes[node]
	self.partition = config.partitions[node][:]
	self.node_suffix = "" if not config.distributed else "_%s" % self.node

class Part:
    __slots__ = ['name', 'file', 'lines', 'year', 'month']

    def __init__(self, filename, lines):
	self.name = filename[28:-4]
	self.file = filename
	self.lines = lines
	year, month = self.name.split('_')
	self.year = int(year)
	self.month = int(month)

    def __getitem__(self, i):
	return getattr(self, i)

    def __repr__(self):
    	return "Part(file='%s')" % self.file



def read_nodefile(path, config):
    lineno = 0
    for line in open(path):
	lineno += 1

	line = line.strip()
	if not line:
	    continue

	words = line.split() + 3 * [None]
	[n, u, d] = words[:3]
	if not n or not u:
	    raise ErrMsg("Error on line %d of %s" % (lineno, path))
	if n in config.urls:
	    raise ErrMsg("Duplicate node name %s in %s" % (n, path))

	config.nodes.append(n)
	config.urls[n] = u
	prefix = None
	if not d:
	    prefix = ''
	else:
	    if d.endswith(os.path.sep):
		prefix = d
	    else:
		prefix = d + os.path.sep
	config.binprefixes[n] = prefix
	config.partitions[n] = []

    config.masternode = config.nodes[0]
    config.distributed = len(config.nodes) > 1

def read_subset(config):
    for line in open(os.path.join(config.subsetdir, 'inputs.txt')):
	line = line.strip()
	if not line:
	    continue
	[input, lines] = line.split()
	lines = int(lines)
	part = Part(input, lines)
	config.parts.append(part)

def partition(config):
    if len(config.nodes) == 1:
	nodes = config.nodes[:]
    else:
	# leave master alone, use only slaves
	nodes = config.nodes[1:]

    for part, node in zip(config.parts, len(config.parts) * nodes):
	config.partitions[node].append(part)


def write_makefile(config):
    f = open(os.path.join(config.outputdir, 'Makefile'), 'w')

    print >>f, "# Generated file, do not edit"
    print >>f
    print >>f, "RSYNC_LOCATION = rsync://catskill.da.cwi.nl:14215/atraf-data"
    print >>f
    print >>f, "default:"
    print >>f, "\t@echo Hello, world"
    print >>f

    print >>f, "check:",
    for n in config.nodes:
	print >>f, "check-%s" % n,
    print >>f
    for n in config.nodes:
	c = config.for_node(n)
	print >>f, "check-%(node)s:" % c
	print >>f, "\t%(binprefix)smclient -d %(url)s -ftab -s 'select id from sys.tables where false'" % c
    print >>f

    print >>f, "downloads:",
    for n in config.nodes:
	c = config.for_node(n)
	if not c.partition:
	    continue
	print >>f, "download-%s" % n,
    print >>f
    for n in config.nodes:
	c = config.for_node(n)
	if not c.partition:
	    continue
	print >>f, "download-%(node)s: \\" % c
	for p in c.partition:
	    backslash = "\\" if p != c.partition[-1] else ""
	    print >>f, "\t\tdata/%s %s" % (p.file, backslash)
    print >>f
    for p in config.parts:
	print >>f, "data/%(file)s: data/%(file)s.xz" % p
	print >>f, "\txz -d <$^ >$@.tmp"
	print >>f, "\tmv $@.tmp $@"
	print >>f, "data/%(file)s.xz: data/.dir" % p
	print >>f, "\trsync $(RSYNC_LOCATION)/%(file)s.xz $@.tmp" % p
	print >>f, "\tmv $@.tmp $@"
    print >>f

    print >>f, "data/.dir:"
    print >>f, "\tmkdir -p data"
    print >>f, "\ttouch $@"
    print >>f

    print >>f, "schemas:",
    for n in config.nodes:
	print >>f, "schema-%s" % n,
    print >>f
    for n in config.nodes:
	c = config.for_node(n)
	print >>f, "schema-%s:" % n
	print >>f, "\t%(binprefix)smclient -d %(url)s %(subset)s/schema-%(node)s.sql " % c
    print >>f


def write_schema(conf):
    f = open(os.path.join(conf.outputsetdir, 'schema-%(node)s.sql' % conf), 'w')
    schema_sql.generate_schema(f, conf)


def main(argv0, nodefile=None, subset=None, outputdir=None):
    """Entry point"""

    config = Config()

    if not nodefile or not subset or not outputdir:
	raise ErrMsg("Usage: %s NODEFILE SUBSET OUTPUTDIR" % argv0)

    config.subset = subset
    config.basedir = os.path.abspath(os.path.dirname(argv0))
    config.nodefile = os.path.abspath(os.path.join(config.basedir, nodefile))
    config.subsetdir = os.path.abspath(os.path.join(config.basedir, 'subsets', subset))
    config.outputdir = os.path.abspath(outputdir)  # not relative to basedir
    config.outputsetdir = os.path.join(config.outputdir, config.subset)

    if not os.path.isfile(config.nodefile):
	raise ErrMsg("Node file %s does not exist" % config.nodefile)
    if not os.path.isdir(config.subsetdir):
	raise ErrMsg("Subset directory %s does not exist" % config.subsetdir)

    for d in [config.outputdir, config.outputsetdir]:
	if not os.path.isdir(d):
	    os.makedirs(d)

    read_nodefile(config.nodefile, config)
    read_subset(config)
    partition(config)

    config.dump()

    write_makefile(config)
    for node in config.nodes:
	write_schema(config.for_node(node))


if __name__ == "__main__":
    try:
	sys.exit(main(*sys.argv) or 0)
    except ErrMsg, e:
	print >>sys.stderr, e.message
	sys.exit(1)
