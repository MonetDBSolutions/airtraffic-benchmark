#!/usr/bin/env python3

import argparse
import glob
import os
import io
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
		sio = io.StringIO()
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
				print(f"Writing {path}")
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
		print("Wrote %d files, left %d unchanged" % (written_files, unchanged_files))


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
		'use_curl',
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
			v = getattr(self, a, None)
			print(f"{a} = {v!r}")

	def for_node(self, node):
		return NodeConfig(self, node)

class NodeConfig(Config):
	__slots__ = Config.__slots__ + [
                'nodenum',
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
		self.nodenum = config.nodes.index(node)
		self.url = config.urls[node]
		self.partition = config.partitions[node][:]
		self.node_suffix = "" if not config.distributed else "_" + self.node
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
		return f"Part(load_file='{self.load_file}')"



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
			raise ErrMsg(f"Duplicate node name {n} in {path}")
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

	print("# Generated file, do not edit", file=f)
	print(file=f)
	print(f"DATA_LOCATION = {config['data_location']}", file=f)
	print(f"PREMADE_LOCATION = {config['premade_location']}", file=f)
	print(f"DOWNLOAD_DIR = {config['download_dir']}", file=f)
	print(file=f)
	print("# Will be invoked as $(FETCH) TARGET_FILE SOURCE_URL", file=f)
	if config.data_location.startswith('rsync'):
		print('FETCH = swapargs() { X="$$1"; shift; Y="$$1"; shift; rsync "$$Y" "$$X" "$$@"; }; swapargs', file=f)
	elif config.use_curl:
		print("FETCH = curl -s -o", file=f)
		print("# Alternative: wget -q -O", file=f)
	else:
		print("FETCH = wget -q -O", file=f)
		print("# Alternative: curl -s -o", file=f)
	print(file=f)
	print("NODENAME := $(shell hostname -s)", file=f)
	print("DB_URL=$(DB_URL_$(NODENAME))", file=f)
	for n in config.nodes:
		c = config.for_node(n)
		print(f"DB_URL_{c['node']}={c['url']}", file=f)

	print(file=f)
	print("default:", file=f)
	print("\t@echo This is node $(NODENAME) with url $(DB_URL)", file=f)
	print(file=f)

	print("ping: ping-$(NODENAME)", file=f)
	print("ping-all:", " ".join("ping-%s" % n for n in config.nodes), file=f)
	for n in config.nodes:
		c = config.for_node(n)
		print(f"ping-{c['node']}:", file=f)
		print(f"\t$(MCLIENT_PREFIX)mclient -d {c['url']} -ftab -s 'select id from sys.tables where false'", file=f)
	print(file=f)

	print("download: download-$(NODENAME)", file=f)
	print("download-all:", " ".join("download-%s" % n for n in config.nodes if config.for_node(n).partition), file=f)
	for n in config.nodes:
		c = config.for_node(n)
		print(f"download-{c['node']}:", end=' ', file=f)
		for p in sorted(set(p.load_file for p in c.partition)):
			print(f"\\\n\t\t$(DOWNLOAD_DIR)/{p}", end=' ', file=f)
		print("", file=f)
	print(file=f)
	seen = set() # instances of Part are unequal even if they're equal
	for p in config.parts:
		if p.fetch_file in seen:
		    continue
		seen.add(p.fetch_file)
		if p.load_file != p.fetch_file:
			print(f"$(DOWNLOAD_DIR)/{p['load_file']}: $(DOWNLOAD_DIR)/{p['fetch_file']}", file=f)
			if p.fetch_file.endswith('.xz'):
				print("\txz -d <$^ >$@.tmp", file=f)
			elif p.fetch_file.endswith('gz'):
				print("\tgzip -d <$^ >$@.tmp", file=f)
			else:
				raise ErrMsg(f"Don't know how to decompress {p.fetch_file}")
			print("\tmv $@.tmp $@", file=f)
		print(f"$(DOWNLOAD_DIR)/{p['fetch_file']}: $(DOWNLOAD_DIR)/.dir", file=f)
		print(f"\t$(FETCH) $@.tmp $(DATA_LOCATION)/{p['fetch_file']}", file=f)
		print("\tmv $@.tmp $@", file=f)
	print(file=f)

	print("download-premade: download-premade-$(NODENAME)", file=f)
	print("download-premade-all:", " ".join("download-premade-%s" % n for n in config.nodes), file=f)
	for n in config.nodes:
		print(f"download-premade-{n}: $(DOWNLOAD_DIR)/{config.for_node(n).premade_archive_name}", file=f)
	print(file=f)
	for n in config.nodes:
		archive = config.for_node(n).premade_archive_name
		print(f"$(DOWNLOAD_DIR)/{archive}: $(DOWNLOAD_DIR)/.dir", file=f)
		print(f"\t$(FETCH) $@.tmp $(PREMADE_LOCATION)/{archive}", file=f)
		print("\tmv $@.tmp $@", file=f)
	print(file=f)

	print("# The downloaded files have a dependency on this file.", file=f)
	print("# To avoid redownloading them we set the timestamp far in the past", file=f)
	print("$(DOWNLOAD_DIR)/.dir:", file=f)
	print("\tmkdir -p $(DOWNLOAD_DIR)", file=f)
	print("\ttouch -t 198510260124 $@", file=f)
	print(file=f)

	print("schema: schema-$(NODENAME)", file=f)
	print("schema-all:", " ".join("schema-%s" % n for n in config.nodes), file=f)
	for n in config.nodes:
		c = config.for_node(n)
		print(f"schema-{n}: schema-local-{n} schema-remote-{n}", file=f)
	print(file=f)

	print("schema-local: schema-local-$(NODENAME)", file=f)
	print("schema-local-all:", " ".join("schema-local-%s" % n for n in config.nodes), file=f)
	for n in config.nodes:
		c = config.for_node(n)
		print(f"schema-local-{n}:", file=f)
		print(f"\t$(MCLIENT_PREFIX)mclient -d {c['url']} schema-local-{c['node']}.sql ", file=f)
	print(file=f)

	print("schema-remote: schema-remote-$(NODENAME)", file=f)
	print("schema-remote-all:", " ".join("schema-remote-%s" % n for n in config.nodes), file=f)
	for n in config.nodes:
		c = config.for_node(n)
		print(f"schema-remote-{n}:", file=f)
		print(f"\t$(MCLIENT_PREFIX)mclient -d {c['url']} schema-remote-{c['node']}.sql ", file=f)
	print(file=f)

	print("drop: drop-$(NODENAME)", file=f)
	print("drop-all:", " ".join("drop-%s" % n for n in config.nodes), file=f)
	for n in config.nodes:
		c = config.for_node(n)
		print(f"drop-{n}:", file=f)
		print(f"\t$(MCLIENT_PREFIX)mclient -d {c['url']} -s 'DROP SCHEMA IF EXISTS atraf CASCADE' ", file=f)
	print(file=f)

	print("insert: insert-$(NODENAME)", file=f)
	print("insert-all:", " ".join("insert-%s" % n for n in config.nodes), file=f)
	for n in config.nodes:
		c = config.for_node(n)
		print(f"insert-{n}:", file=f)
		print(f"\t<insert-{c['node']}.sql sed -e 's,@DOWNLOAD_DIR@,$(abspath $(DOWNLOAD_DIR)),' | $(MCLIENT_PREFIX)mclient -d {c['url']}", file=f)
	print(file=f)

	print("unpack-premade: unpack-premade-$(NODENAME)", file=f)
	print("unpack-premade-all:", " ".join("unpack-premade-%s" % n for n in config.nodes), file=f)
	for n in config.nodes:
		c = config.for_node(n)
		destname = c.url[c.url.rindex('/') + 1:]
		print(f"unpack-premade-{n}:", file=f)
		print("\ttest -d '$(DBFARM_DIR)' # please set DBFARM_DIR from the command line", file=f)
		print(f"\tmkdir '$(DBFARM_DIR)/{destname}' # should not exist yet", file=f)
		print("\t%s -d < '$(DOWNLOAD_DIR)/%s' | tar -x -f- -C '$(DBFARM_DIR)/%s' --strip-components=1" % (
			config.compression, 
			c.premade_archive_name, 
			destname), file=f)
	print(file=f)

	print("validate: validate-rowcount", end=' ', file=f)
	for q in sorted(config.queries.keys()):
		print(f"\\\n\t\tvalidate-{q}", end=' ', file=f)
	print(file=f)

	print(file=f)
	print("validate-rowcount:", file=f)
	print("\tmkdir -p output", file=f)
	print("\t$(MCLIENT_PREFIX)mclient -f csv -d $(DB_URL) -s 'SELECT \"Year\", \"Month\", COUNT(*) AS \"Rows\" FROM atraf.ontime GROUP BY \"Year\", \"Month\" ORDER BY \"Year\", \"Month\"' >output/rowcount.csv", file=f)
	print("\tcmp answers/rowcount.csv output/rowcount.csv", file=f)
	for q in sorted(config.queries.keys()):
		print(f"validate-{q}:", file=f)
		print("\tmkdir -p output", file=f)
		print(f"\t$(MCLIENT_PREFIX)mclient -f csv -d $(DB_URL) sql/{q}.sql >output/{q}.csv", file=f)
		print(f"\tcmp answers/{q}.csv output/{q}.csv", file=f)

	print(file=f)
	print("validated: ", end=' ', file=f)
	for q in sorted(config.queries.keys()):
		print(f"\\\n\t{q}.ok", end=' ', file=f)
	print(file=f)
	print("%.ok:", file=f)
	print("\tmake validate-$(@:.ok=)", file=f)
	print("\ttouch $@", file=f)

	plans = [(f"{q}.plan", f"sql/plan_{q}.sql", q) for q in sorted(config.queries.keys())]
	print(file=f)
	print("plan:", end=' ', file=f)
	for p, _, _ in plans:
		print(f"\\\n\t\t{p}", end=' ', file=f)
	print(file=f)
	print(file=f)
	for p, e, q in plans:
		print(f"{e}: sql/{q}.sql", file=f)
		print("\tsed -e '3s/^/PLAN /' <$< >$@.tmp", file=f)
		print("\tmv $@.tmp $@", file=f)

	explains = [(f"{q}.explain", f"sql/explain_{q}.sql", q) for q in sorted(config.queries.keys())]
	print(file=f)
	print("explain:", end=' ', file=f)
	for p, _, _ in explains:
		print(f"\\\n\t\t{p}", end=' ', file=f)
	print(file=f)
	print(file=f)
	for p, e, q in explains:
		print(f"{e}: sql/{q}.sql", file=f)
		print("\tsed -e '3s/^/EXPLAIN /' <$< >$@.tmp", file=f)
		print("\tmv $@.tmp $@", file=f)

	for p, e, q in plans + explains:
		print(f"{p}: {e}", file=f)
		print("\t$(MCLIENT_PREFIX)mclient -fraw -d $(DB_URL) $< >$@.tmp", file=f)
		print("\tsed -e '/^%/d' <$@.tmp >$@", file=f)
		print("\trm $@.tmp", file=f)


def write_schema(writer, conf):
	f = writer.open(f"schema-local-{conf['node']}.sql")
	schema_sql.generate_local_schema(f, conf)
	f = writer.open(f"schema-remote-{conf['node']}.sql")
	schema_sql.generate_remote_schema(f, conf)

def write_inserts(writer, conf):
	f = writer.open(f"insert-{conf['node']}.sql")
	schema_sql.generate_inserts(f, conf)

def write_sql(writer, config):
	for name, query in sorted(config.queries.items()):
		f = writer.open(os.path.join(f'sql/{name}.sql'))
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
	config.load_compressed = args.load_compressed
	config.compression = args.compression or ('gz' if config.load_compressed else 'xz')
	config.data_location = args.data_location
	config.premade_location = args.premade_location
	config.download_dir = args.download_dir
	config.use_curl = args.use_curl

	if not os.path.isfile(config.nodefile):
		raise ErrMsg(f"Node file {config.nodefile} does not exist")
	if not os.path.isdir(config.subsetdir):
		raise ErrMsg(f"Subset directory {config.subsetdir} does not exist")

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
	choices=('gz', 'xz')
)
parser.add_argument('--decompress-first', help='decompress downloaded files first',
	action='store_false', dest='load_compressed'
)
parser.add_argument('--load-compressed', help='do not decompress downloaded files first',
	action='store_true', dest='load_compressed'
)
parser.set_defaults(load_compressed=True)
parser.add_argument('--data-location', help='http- or rsync location of the data files',
	default='https://s3.eu-central-1.amazonaws.com/atraf/atraf-data'
)
parser.add_argument('--premade-location', help='http- or rsync location of premade database tar files',
	default='https://s3.eu-central-1.amazonaws.com/atraf-premade/linux-amd64/Aug2018-SP2'
)
parser.add_argument('--download-dir', help='directory where downloaded data will be stored, relative to the Makefile',
	default='../atraf-data'
)
parser.add_argument('--use-curl', help='use curl rather than wget to download data',
	action='store_true'
)

if __name__ == "__main__":
	try:
		args = parser.parse_args()
		status = main(sys.argv[0], args)
		sys.exit(status or 0)
	except ErrMsg as e:
		print(e.message, file=sys.stderr)
		sys.exit(1)
