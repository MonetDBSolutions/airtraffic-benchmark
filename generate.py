#!/usr/bin/env python2

import argparse
import glob
import os
import StringIO
import re
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


class Config(object):
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
		'compression',
		'load_compressed',
		'data_location',
		'premade_location',
		'download_dir',
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
		'premade',
		'premade_archive_name',
	]

	def __init__(self, config, node):
		for a in Config.__slots__:
			if hasattr(config, a):
				setattr(self, a, getattr(config, a))
		self.node = node
		self.url = config.urls[node]
		self.partition = config.partitions[node][:]
		self.node_suffix = "" if not config.distributed else "_%s" % self.node
		self.premade = "%s_%dx_%d" % (
			config.subset,
			len(config.nodes),
			config.nodes.index(node),
		)
		self.premade_archive_name = self.premade + '.tar.' + config.compression

class Part:
	__slots__ = ['name', 'load_file', 'fetch_file', 'lines', 'year', 'month']

	def __init__(self, load_file, fetch_file, lines):
		self.name = re.search(r'_(\d\d\d\d_\d+)', load_file).group(1)
		self.load_file = load_file
		self.fetch_file = fetch_file
		self.lines = lines
		year, month = self.name.split('_')
		self.year = int(year)
		self.month = int(month)

	def __getitem__(self, i):
		return getattr(self, i)

	def __repr__(self):
		return "Part(load_file='%s')" % self.load_file



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
		[filename, lines] = line.split()
		lines = int(lines)
		fetch_file = filename + '.' + config.compression
		load_file = filename if not config.load_compressed else fetch_file
		part = Part(load_file, fetch_file, lines)
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
	print >>f, "DATA_LOCATION = %(data_location)s" % config
	print >>f, "PREMADE_LOCATION = %(premade_location)s" % config
	print >>f, "DOWNLOAD_DIR = %(download_dir)s" % config
	print >>f
	print >>f, "# Will be invoked as $(FETCH) TARGET_FILE SOURCE_URL"
	print >>f, "# Alternative: curl -s -o"
	if config.data_location.startswith('rsync'):
		print >>f, 'FETCH = swapargs() { X="$$1"; shift; Y="$$1"; shift; rsync "$$Y" "$$X" "$$@"; }; swapargs'
	else:
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
	print >>f, "ping-all:"," ".join("ping-%s" % n for n in config.nodes)
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "ping-%(node)s:" % c
		print >>f, "\t$(MCLIENT_PREFIX)mclient -d %(url)s -ftab -s 'select id from sys.tables where false'" % c
	print >>f

	print >>f, "download: download-$(NODENAME)"
	print >>f, "download-all:"," ".join("download-%s" % n for n in config.nodes if config.for_node(n).partition)
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "download-%(node)s:" % c,
		for p in sorted(set(p.load_file for p in c.partition)):
			print >>f, "\\\n\t\t$(DOWNLOAD_DIR)/%s" % p,
		print >>f, ""
	print >>f
	seen = set() # instances of Part are unequal even if they're equal
	for p in config.parts:
		if p.fetch_file in seen:
		    continue
		seen.add(p.fetch_file)
		if p.load_file != p.fetch_file:
			print >>f, "$(DOWNLOAD_DIR)/%(load_file)s: $(DOWNLOAD_DIR)/%(fetch_file)s" % p
			if p.fetch_file.endswith('.xz'):
				print >>f, "\txz -d <$^ >$@.tmp"
			elif p.fetch_file.endswith('gz'):
				print >>f, "\tgzip -d <$^ >$@.tmp"
			else:
				raise ErrMsg("Don't know how to decompress %s" % p.fetch_file)
			print >>f, "\tmv $@.tmp $@"
		print >>f, "$(DOWNLOAD_DIR)/%(fetch_file)s: $(DOWNLOAD_DIR)/.dir" % p
		print >>f, "\t$(FETCH) $@.tmp $(DATA_LOCATION)/%(fetch_file)s" % p
		print >>f, "\tmv $@.tmp $@"
	print >>f

	print >>f, "download-premade: download-premade-$(NODENAME)"
	print >>f, "download-premade-all:", " ".join("download-premade-%s" % n for n in config.nodes)
	for n in config.nodes:
		print >>f, "download-premade-%s: $(DOWNLOAD_DIR)/%s" % (n, config.for_node(n).premade_archive_name)
	print >>f
	for n in config.nodes:
		archive = config.for_node(n).premade_archive_name
		print >>f, "$(DOWNLOAD_DIR)/%s: $(DOWNLOAD_DIR)/.dir" % archive
		print >>f, "\t$(FETCH) $@.tmp $(PREMADE_LOCATION)/%s" % archive
		print >>f, "\tmv $@.tmp $@"
	print >>f

	print >>f, "# The downloaded files have a dependency on this file."
	print >>f, "# To avoid redownloading them we set the timestamp far in the past"
	print >>f, "$(DOWNLOAD_DIR)/.dir:"
	print >>f, "\tmkdir -p $(DOWNLOAD_DIR)"
	print >>f, "\ttouch -t 198510260124 $@"
	print >>f

	print >>f, "schema: schema-$(NODENAME)"
	print >>f, "schema-all:", " ".join("schema-%s" % n for n in config.nodes)
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "schema-%s: schema-local-%s schema-remote-%s" % (n, n, n)
	print >>f

	print >>f, "schema-local: schema-$(NODENAME)"
	print >>f, "schema-local-all:", " ".join("schema-local-%s" % n for n in config.nodes)
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "schema-local-%s:" % n
		print >>f, "\t$(MCLIENT_PREFIX)mclient -d %(url)s schema-local-%(node)s.sql " % c
	print >>f

	print >>f, "schema-remote: schema-remote-$(NODENAME)"
	print >>f, "schema-remote-all:", " ".join("schema-remote-%s" % n for n in config.nodes)
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "schema-remote-%s:" % n
		print >>f, "\t$(MCLIENT_PREFIX)mclient -d %(url)s schema-remote-%(node)s.sql " % c
	print >>f

	print >>f, "drop: drop-$(NODENAME)"
	print >>f, "drop-all:"," ".join("drop-%s" % n for n in config.nodes)
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "drop-%s:" % n
		print >>f, "\t$(MCLIENT_PREFIX)mclient -d %(url)s -s 'DROP SCHEMA IF EXISTS atraf CASCADE' " % c
	print >>f

	print >>f, "insert: insert-$(NODENAME)"
	print >>f, "insert-all:"," ".join("insert-%s" % n for n in config.nodes)
	for n in config.nodes:
		c = config.for_node(n)
		print >>f, "insert-%s:" % n
		print >>f, "\t<insert-%(node)s.sql sed -e 's,@DOWNLOAD_DIR@,$(abspath $(DOWNLOAD_DIR)),' | $(MCLIENT_PREFIX)mclient -d %(url)s" % c
	print >>f

	print >>f, "unpack-readymade: unpack-readymade-$(NODENAME)"
	print >>f, "unpack-readymade-all:"," ".join("unpack-readymade-%s" % n for n in config.nodes)
	for n in config.nodes:
		c = config.for_node(n)
		destname = c.url[c.url.rindex('/') + 1:]
		print >>f, "unpack-readymade-%s:" % n
		print >>f, "\ttest -d '$(DBFARM_DIR)' # please set DBFARM_DIR from the command line"
		print >>f, "\tmkdir '$(DBFARM_DIR)/%s' # should not exist yet" % destname
		print >>f, "\t%s -d < '$(DOWNLOAD_DIR)/%s' | tar -x -f- -C '$(DBFARM_DIR)/%s' --strip-components=1" % (
			config.compression, 
			c.premade_archive_name, 
			destname)
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

	plans = [("%s.plan" % q, "sql/plan_%s.sql" % q, q) for q in sorted(config.queries.keys())]
	print >>f
	print >>f, "plan:",
	for p, _, _ in plans:
		print >>f, "\\\n\t\t%s" % p,
	print >>f
	print >>f
	for p, e, q in plans:
		print >>f, "%s: sql/%s.sql" % (e, q)
		print >>f, "\tsed -e '3s/^/PLAN /' <$< >$@.tmp"
		print >>f, "\tmv $@.tmp $@"

	explains = [("%s.explain" % q, "sql/explain_%s.sql" % q, q) for q in sorted(config.queries.keys())]
	print >>f
	print >>f, "explain:",
	for p, _, _ in explains:
		print >>f, "\\\n\t\t%s" % p,
	print >>f
	print >>f
	for p, e, q in explains:
		print >>f, "%s: sql/%s.sql" % (e, q)
		print >>f, "\tsed -e '3s/^/EXPLAIN /' <$< >$@.tmp"
		print >>f, "\tmv $@.tmp $@"

	for p, e, q in plans + explains:
		print >>f, "%s: %s" % (p, e)
		print >>f, "\t$(MCLIENT_PREFIX)mclient -fraw -d $(DB_URL) $< >$@.tmp"
		print >>f, "\tsed -e '/^%/d' <$@.tmp >$@"
		print >>f, "\trm $@.tmp"


def write_schema(writer, conf):
	f = writer.open('schema-local-%(node)s.sql' % conf)
	schema_sql.generate_local_schema(f, conf)
	f = writer.open('schema-remote-%(node)s.sql' % conf)
	schema_sql.generate_remote_schema(f, conf)

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
    src = os.path.join(config.basedir, 'bench.py')
    content = open(src).read()
    writer.open(script, True).write(content)

def main(argv0, args):
	"""Entry point"""

	config = Config()

	config.subset = args.subset
	config.basedir = os.path.abspath(os.path.dirname(argv0))
	config.nodefile = os.path.abspath(args.nodefile)  # not relative to basedir
	config.subsetdir = os.path.abspath(os.path.join(config.basedir, 'subsets', args.subset))
	config.sqldir = os.path.abspath(os.path.join(config.basedir, 'sql'))
	config.outputdir = os.path.abspath(args.outputdir)  # not relative to basedir
	config.compression = args.compression
	config.load_compressed = args.load_compressed
	config.data_location = args.data_location
	config.premade_location = args.premade_location
	config.download_dir = args.download_dir

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

parser = argparse.ArgumentParser(description='Generate airtraffic benchmark files')
parser.add_argument('nodefile', help='Node file, see README')
parser.add_argument('subset', help='data set to use, for example `3mo` or `2yr`')
parser.add_argument('outputdir', help='where to write the generated files')
parser.add_argument('--compression', help='use this to download .gz data instead of .xz',
	choices=('gz', 'xz'),
	default='xz'
)
parser.add_argument('--load-compressed', help='do not decompress downloaded files first',
	action='store_true'
)
parser.add_argument('--data-location', help='http- or rsync location of the data files',
	default='https://s3.eu-central-1.amazonaws.com/atraf/atraf-data'
)
parser.add_argument('--premade-location', help='http- or rsync location of premade database tar files',
	default='https://s3.eu-central-1.amazonaws.com/atraf/atraf-data/linux-amd64/Aug2018-SP2'
)

parser.add_argument('--download-dir', help='directory where downloaded data will be stored, relative to the Makefile',
	default='../atraf-data'
)

if __name__ == "__main__":
	try:
		args = parser.parse_args()
		status = main(sys.argv[0], args)
		sys.exit(status or 0)
	except ErrMsg, e:
		print >>sys.stderr, e.message
		sys.exit(1)
