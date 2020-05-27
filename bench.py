#!/usr/bin/env python3

import argparse
import glob
import os
import random
import re
import sys
import subprocess
import time

# for consistency with the Makefile we use string concatenation here,
# not a proper os.path.join
MCLIENT = os.environ.get('MCLIENT_PREFIX', '') + "mclient"

class Issue(Exception):
    def __init__(self, msg):
        self.msg = msg

def writer(outfilename, also_stdout):
    outfile = open(outfilename, 'a')

    def write(fmt, *args):
        line = fmt % args
        if also_stdout:
            print(line)
        print(line, file=outfile)
        outfile.flush()

    return write

def qq(s):
    if '"' in s or '\\' in s:
        raise("Oops")
    return '"' + s + '"'

def main(args):
    config = args.name or args.db
    deadline = time.time() + (args.duration or float("inf"))
    repeat = args.repeat or float("inf")

    outfilename = config + ".csv"
    if args.output:
        if os.path.isdir(args.output):
            outfilename = os.path.join(args.output, outfilename)
        else:
            outfilename = args.output

    show_header = True
    seq = 0

    if os.path.exists(outfilename):
        contents = open(outfilename).readlines()
        show_header = not contents
        if len(contents) > 1:
            last = contents[-1]
            seq = int(last.split(',')[1])
    write = writer(outfilename, not args.silent)

    queries = sorted(glob.glob('sql/q??.sql'))
    if not queries:
	raise Issue("No queries found")

    rnd = random.Random(0)

    if show_header:
        write("config,seqno,query,duration")
    count = 0
    done = False
    while repeat == 0 or count < repeat:
        count += 1
        for q in queries:
            if time.time() >= deadline:
                done = True
                break
            seq += 1
            duration = run(args.db, q)
            name = os.path.splitext(os.path.basename(q))[0]
            write("%s,%d,%s,%f", qq(config), seq, qq(name), duration)
        rnd.shuffle(queries)   # in-place

    return 0

def run(db, queryfile):
    # /usr/bin/env searches the PATH for us
    cmd = ['/usr/bin/env', MCLIENT, '-tperformance', '-fraw', '-d', db, queryfile]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (_, err) = p.communicate()
    except subprocess.CalledProcessError as e:
        print("Query %s triggered an exception:" % queryfile, file=sys.stderr)
        raise e
    
    # Look for something like this: clk:1297.253 ms
    pattern = re.compile(r'^clk:\s*(\d+\.\d+)\s*ms\s*$', re.M)
    m = pattern.search(err)
    if not m:
        max = 100
        snippet = err if len(err) <= max else "..." + err[-max:]
        raise Issue("Query %s failed: %r" % (queryfile, snippet))
    return float(m.group(1))


parser = argparse.ArgumentParser(description='Repeatedly run benchmarks')
parser.add_argument('db',
                    help='Database name')
parser.add_argument('--name', '-n',
                    help='Config name, used to label the results, defaults to database name')
parser.add_argument('--duration', '-d', type=int,
                    help='After this many seconds no new queries are started')
parser.add_argument('--repeat', '-N', type=int,
                    help='Number of time to repeat each query, 0 to repeat indefinitely')
parser.add_argument('--output', '-o',
                    help='File or directory to write output to, defaults to ./<CONFIG>.csv')
parser.add_argument('--silent', action='store_true',
                    help='Do not write the results to stdout')


if __name__ == "__main__":
    try:
        args = parser.parse_args()
        status = main(args)
        sys.exit(status or 0)
    except Issue as e:
        print("An error occurred:", e.msg, file=sys.stderr)
        sys.exit(1)
