#!/usr/bin/env python2

import os
import StringIO
import sys

import schema_sql

class ErrMsg(Exception):
	def __init__(self, msg):
		self.message = msg

class FileWriter:
	def __init__(self, basedir):
		self.basedir = basedir
		self.files = {}
	
	def open(self, path):
		sio = StringIO.StringIO()
		self.files[path] = sio
		return sio

	def write_all(self):
		for path, sio in self.files.items():
			p = os.path.join(self.basedir, path)
			old_contents = open(p).read() if os.path.exists(p) else None
			new_contents = sio.getvalue()
			if new_contents != old_contents:
				print "Writing %s" % path
				open(p, 'w').write(new_contents)
			else:
				print "Not changing %s" % path

class Config:
	# Use __slots__ so misspelled attributes give an error
	__slots__ = [
		'basedir',
		'nodefile',
		'subset',
		'subsetdir',
		'outputdir',
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


def write_makefile(writer, config):
	f = writer.open('Makefile')

	print >>f, "# Generated file, do not edit"
	print >>f
	print >>f, "DATA_LOCATION = https://s3.eu-central-1.amazonaws.com/atraf/atraf-data"
	print >>f, "DATA_DIR = ../atraf-data"
	print >>f
	print >>f, "# Will be invoked as $(FETCH) TARGET_FILE SOURCE_URL"
	print >>f, "# Alternative: curl -s -o"
	print >>f, "FETCH = wget -q -O"
	print >>f
	print >>f, "HOSTNAME := $(shell hostname)"
	print >>f
	print >>f, "default:"
	print >>f, "\t@echo Hello, world"
	print >>f

	print >>f, "ping: ping-$(HOSTNAME)"
	print >>f, "ping-all:",
	for n in config.nodes:
		print >>f, "ping-%s" % n,
	print >>f
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "ping-%(node)s:" % c
		print >>f, "\t%(binprefix)smclient -d %(url)s -ftab -s 'select id from sys.tables where false'" % c
	print >>f

	print >>f, "download: download-$(HOSTNAME)"
	print >>f, "download-all:",
	for n in config.nodes:
		c = config.for_node(n)
		if not c.partition:
			continue
		print >>f, "download-%s" % n,
	print >>f
	for n in config.nodes:
		c = config.for_node(n)
		if not c.partition:
			print >>f, "download-%(node)s:" % c
			continue
		print >>f, "download-%(node)s: \\" % c
		for p in c.partition:
			backslash = "\\" if p != c.partition[-1] else ""
			print >>f, "\t\t$(DATA_DIR)/%s %s" % (p.file, backslash)
	print >>f
	for p in config.parts:
		print >>f, "$(DATA_DIR)/%(file)s: $(DATA_DIR)/%(file)s.xz" % p
		print >>f, "\txz -d <$^ >$@.tmp"
		print >>f, "\tmv $@.tmp $@"
		print >>f, "$(DATA_DIR)/%(file)s.xz: $(DATA_DIR)/.dir" % p
		print >>f, "\t$(FETCH) $@.tmp $(DATA_LOCATION)/%(file)s.xz" % p
		print >>f, "\tmv $@.tmp $@"
	print >>f

	print >>f, "# The downloaded files have a dependency on this file."
	print >>f, "# To avoid redownloading them we set the timestamp far in the past"
	print >>f, "$(DATA_DIR)/.dir:"
	print >>f, "\tmkdir -p $(DATA_DIR)"
	print >>f, "\ttouch -t 198510260124 $@"
	print >>f

	print >>f, "schema: schema-$(HOSTNAME)"
	print >>f, "schema-all:",
	for n in config.nodes:
		print >>f, "schema-%s" % n,
	print >>f
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "schema-%s:" % n
		print >>f, "\t%(binprefix)smclient -d %(url)s schema-%(node)s.sql " % c
	print >>f

	print >>f, "drop: drop-$(HOSTNAME)"
	print >>f, "drop-all:",
	for n in config.nodes:
		print >>f, "drop-%s" % n,
	print >>f
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "drop-%s:" % n
		print >>f, "\t%(binprefix)smclient -d %(url)s -s 'DROP SCHEMA IF EXISTS atraf CASCADE' " % c
	print >>f

	print >>f, "insert: insert-$(HOSTNAME)"
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "insert-%s:" % n
		print >>f, "\t<insert-%(node)s.sql sed -e 's,@DATA_DIR@,$(abspath $(DATA_DIR)),' | %(binprefix)smclient -d %(url)s" % c
	print >>f


def write_schema(writer, conf):
	f = writer.open('schema-%(node)s.sql' % conf)
	schema_sql.generate_schema(f, conf)

def write_inserts(writer, conf):
	f = writer.open('insert-%(node)s.sql' % conf)
	schema_sql.generate_inserts(f, conf)


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

	if not os.path.isfile(config.nodefile):
		raise ErrMsg("Node file %s does not exist" % config.nodefile)
	if not os.path.isdir(config.subsetdir):
		raise ErrMsg("Subset directory %s does not exist" % config.subsetdir)

	for d in [config.outputdir]:
		if not os.path.isdir(d):
			os.makedirs(d)

	read_nodefile(config.nodefile, config)
	read_subset(config)
	partition(config)

	#config.dump()
	writer = FileWriter(config.outputdir)
	write_makefile(writer, config)
	for node in config.nodes:
		write_schema(writer, config.for_node(node))
		write_inserts(writer, config.for_node(node))
	writer.write_all()

	return 0


if __name__ == "__main__":
	try:
		status = main(sys.argv[0], *sys.argv[1:])
		sys.exit(status or 0)
	except ErrMsg, e:
		print >>sys.stderr, e.message
		sys.exit(1)
