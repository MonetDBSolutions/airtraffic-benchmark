#!/usr/bin/env python

import glob
import os
import random
import re
import sys
import subprocess
import time

# for consistency with for example the Makefile
# we use string concatenation here, not a proper os.path.join
MCLIENT = os.environ.get('MCLIENT_PREFIX', '') + "mclient"

class Issue(Exception):
    def __init__(self, msg):
        self.msg = msg

def sayer(outfilename):
    if outfilename:
        outfile = open(outfilename, 'a')
    else:
        outfile = None

    def say(fmt, *args):
        line = fmt % args
        print line
        if outfile:
            print >>outfile, line
            outfile.flush()

    return say

def qq(s):
    if '"' in s or '\\' in s:
        raise("Oops")
    return '"' + s + '"'

def main(argv0, config, db, duration_in_seconds=None):
    deadline = time.time() + int(duration_in_seconds) if duration_in_seconds else float("inf")
    show_header = True
    seq = 0
    say = sayer(None)
    if config:
        outfilename = config + '.csv'
        say = sayer(outfilename)
        if os.path.exists(outfilename):
            contents = open(outfilename).readlines()
	    show_header = not contents
            if len(contents) > 1:
                last = contents[-1]
                seq = int(last.split(',')[1])
    else:
        config = 'adhoc'

    queries = glob.glob('sql/q??.sql')
    if not queries:
	raise Issue("No queries found")

    rnd = random.Random(0)

    if show_header:
        say("config,seqno,query,duration")
    while time.time() < deadline:
        rnd.shuffle(queries)   # in-place, ew!
        for q in queries:
            seq += 1
            duration = run(db, q)
            name = os.path.splitext(os.path.basename(q))[0]
            say("%s,%d,%s,%f", qq(config), seq, qq(name), duration)

def run(db, queryfile):
    # /usr/bin/env searches the PATH for us
    cmd = ['/usr/bin/env', MCLIENT, '-tperformance', '-fraw', '-d', db, queryfile]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out, err) = p.communicate()
    except subprocess.CalledProcessError, e:
        print >>sys.stderr, "Query %s triggered an exception:" % queryfile
        raise e
    
    # Look for something like this: clk:1297.253 ms
    pattern = re.compile(r'^clk:\s*(\d+\.\d+)\s*ms\s*$', re.M)
    m = pattern.search(err)
    if not m:
        max = 100
        snippet = err if len(err) <= max else "..." + err[-max:]
        raise Issue("Query %s failed: %r" % (queryfile, snippet))
    return float(m.group(1))



if __name__ == "__main__":
    try:
        sys.exit(main(*sys.argv))
    except Issue, e:
        print >>sys.stderr, "An error occurred:", e.msg
        sys.exit(1)
