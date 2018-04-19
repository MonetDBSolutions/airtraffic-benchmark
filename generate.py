#!/usr/bin/env python2

import glob
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
		self.executable = {}
	
	def open(self, path, executable=False):
		sio = StringIO.StringIO()
		self.files[path] = sio
		self.executable[path] = executable
		return sio

	def write_all(self):
		written_files = 0
		unchanged_files = 0

		for path, sio in sorted(self.files.items()):
			p = os.path.join(self.basedir, path)
			old_contents = open(p).read() if os.path.exists(p) else None
			new_contents = sio.getvalue()
			modified = False # so far
			if new_contents != old_contents:
				print "Writing %s" % path
				d = os.path.dirname(p)
				if not os.path.isdir(d):
					os.makedirs(d)
				open(p, 'w').write(new_contents)
				modified |= True
			old_perms = os.stat(p).st_mode
			if self.executable[path]:
				new_perms = old_perms | 0o111
			else:
				new_perms = old_perms & ~0o111
			if new_perms != old_perms:
				os.chmod(p, new_perms)
				modified |= True
			if modified:
				written_files += 1
			else:
				unchanged_files += 1
		print "Wrote %d files, left %d unchanged" % (written_files, unchanged_files)


class Config:
	# Use __slots__ so misspelled attributes give an error
	__slots__ = [
		'basedir',
		'nodefile',
		'subset',
		'subsetdir',
		'sqldir',
		'outputdir',
		'nodes',
		'datanodes',
		'distributed',
		'urls',
		'parts',
		'partitions',
		'queries',
	]

	def __init__(self):
		self.urls = {}
		self.nodes = []
		self.datanodes = []
		self.parts = []
		self.partitions = {}
		self.queries = {}

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
		'partition',
		'node_suffix',
	]

	def __init__(self, config, node):
		for a in Config.__slots__:
			if hasattr(config, a):
				setattr(self, a, getattr(config, a))
		self.node = node
		self.url = config.urls[node]
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
		if not d or d == 'data':
			has_data = True
		elif d == 'nodata':
			has_data = False
		else:
			raise ErrMsg("Third argument on line %d must be either 'data' or 'nodata'" % lineno)
			
		config.nodes.append(n)
		config.urls[n] = u
		config.partitions[n] = []
		if has_data:
			config.datanodes.append(n)

	config.distributed = len(config.nodes) > 1

	if not config.nodes:
		raise ErrMsg("Nodefile must define at least one node")
	if not config.datanodes:
		raise ErrMsg("There must be at least one data node")

def read_subset(config):
	for line in open(os.path.join(config.subsetdir, 'inputs.txt')):
		line = line.strip()
		if not line:
			continue
		[input, lines] = line.split()
		lines = int(lines)
		part = Part(input, lines)
		config.parts.append(part)
	config.parts = sorted(config.parts, key=lambda p: (p.year, p.month))

def read_queries(config):
	for p in glob.glob(os.path.join(config.sqldir, 'q??.sql')):
		n = os.path.splitext(os.path.basename(p))[0]
		q = open(p).read()
		config.queries[n] = q


def partition(config):
	for part, node in zip(config.parts, len(config.parts) * config.datanodes):
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
	print >>f, "NODENAME := $(shell hostname -s)"
	print >>f, "DB_URL=$(DB_URL_$(NODENAME))"
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "DB_URL_%(node)s=%(url)s" % c

	print >>f
	print >>f, "default:"
	print >>f, "\t@echo This is node $(NODENAME) with url $(DB_URL)"
	print >>f

	print >>f, "ping: ping-$(NODENAME)"
	print >>f, "ping-all:",
	for n in config.nodes:
		print >>f, "ping-%s" % n,
	print >>f
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "ping-%(node)s:" % c
		print >>f, "\t$(MCLIENT_PREFIX)mclient -d %(url)s -ftab -s 'select id from sys.tables where false'" % c
	print >>f

	print >>f, "download: download-$(NODENAME)"
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

	print >>f, "schema: schema-$(NODENAME)"
	print >>f, "schema-all:",
	for n in config.nodes:
		print >>f, "schema-%s" % n,
	print >>f
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "schema-%s:" % n
		print >>f, "\t$(MCLIENT_PREFIX)mclient -d %(url)s schema-%(node)s.sql " % c
	print >>f

	print >>f, "drop: drop-$(NODENAME)"
	print >>f, "drop-all:",
	for n in config.nodes:
		print >>f, "drop-%s" % n,
	print >>f
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "drop-%s:" % n
		print >>f, "\t$(MCLIENT_PREFIX)mclient -d %(url)s -s 'DROP SCHEMA IF EXISTS atraf CASCADE' " % c
	print >>f

	print >>f, "insert: insert-$(NODENAME)"
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "insert-%s:" % n
		print >>f, "\t<insert-%(node)s.sql sed -e 's,@DATA_DIR@,$(abspath $(DATA_DIR)),' | $(MCLIENT_PREFIX)mclient -d %(url)s" % c
	print >>f

	print >>f, "validate: validate-rowcount",
	for q in sorted(config.queries.keys()):
		print >>f, "\\\n\t\tvalidate-%s" % q,
	print >>f

	print >>f
	print >>f, "validate-rowcount:"
	print >>f, "\tmkdir -p output"
	print >>f, "\t$(MCLIENT_PREFIX)mclient -f csv -d $(DB_URL) -s 'SELECT \"Year\", \"Month\", COUNT(*) AS \"Rows\" FROM atraf.ontime GROUP BY \"Year\", \"Month\" ORDER BY \"Year\", \"Month\"' >output/rowcount.csv"
	print >>f, "\tcmp answers/rowcount.csv output/rowcount.csv"
	for q in sorted(config.queries.keys()):
		print >>f, "validate-%s:" % q
		print >>f, "\tmkdir -p output"
		print >>f, "\t$(MCLIENT_PREFIX)mclient -f csv -d $(DB_URL) sql/%s.sql >output/%s.csv" % (q, q)
		print >>f, "\tcmp answers/%s.csv output/%s.csv" % (q, q)



def write_schema(writer, conf):
	f = writer.open('schema-%(node)s.sql' % conf)
	schema_sql.generate_schema(f, conf)

def write_inserts(writer, conf):
	f = writer.open('insert-%(node)s.sql' % conf)
	schema_sql.generate_inserts(f, conf)

def write_sql(writer, config):
	for name, query in sorted(config.queries.items()):
		f = writer.open(os.path.join('sql/%s.sql' % name))
		f.write(query)

def write_files_from_dir(writer, fromdir, todir):
	for name in os.listdir(fromdir):
		p = os.path.join(fromdir, name)
		if os.path.isfile(p):
			contents = open(p).read()
			f = writer.open(os.path.join(todir, name))
			f.write(contents)

def write_bench_script(writer, config):
    script = 'bench.py'
    content = open(script).read()
    writer.open(script, True).write(content)

def main(argv0, nodefile=None, subset=None, outputdir=None):
	"""Entry point"""

	config = Config()

	if not nodefile or not subset or not outputdir:
		raise ErrMsg("Usage: %s NODEFILE SUBSET OUTPUTDIR" % argv0)

	config.subset = subset
	config.basedir = os.path.abspath(os.path.dirname(argv0))
	config.nodefile = os.path.abspath(nodefile)  # not relative to basedir
	config.subsetdir = os.path.abspath(os.path.join(config.basedir, 'subsets', subset))
	config.sqldir = os.path.abspath(os.path.join(config.basedir, 'sql'))
	config.outputdir = os.path.abspath(outputdir)  # not relative to basedir

	if not os.path.isfile(config.nodefile):
		raise ErrMsg("Node file %s does not exist" % config.nodefile)
	if not os.path.isdir(config.subsetdir):
		raise ErrMsg("Subset directory %s does not exist" % config.subsetdir)

	read_nodefile(config.nodefile, config)
	read_subset(config)
	read_queries(config)
	partition(config)

	#config.dump()
	writer = FileWriter(config.outputdir)
	write_makefile(writer, config)
	write_sql(writer, config)
	write_files_from_dir(writer, config.subsetdir, 'answers')
	write_bench_script(writer, config)
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
