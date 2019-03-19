#!/usr/bin/env python2

# for i in q01 q02 q03 q04 q05 q06 q08 q09 q10 q11 q12 q13 q14 q15 q16 q18; do ./tracetraffic.py 9999 $i make validate-$i ; done

import os
import signal
import socket
import subprocess
import struct
import sys

import SocketServer


BPF_PROGRAM = """
/* Based on tcptop */

#include <uapi/linux/ptrace.h>
#include <net/sock.h>
#include <bcc/proto.h>

struct ipv4_key_t {
        u32 daddr;
        u16 dport;
        u16 dummy; /* not sure why */
};
BPF_HASH(ipv4_recv_bytes, struct ipv4_key_t);

int kprobe__tcp_cleanup_rbuf(struct pt_regs *ctx, struct sock *sk, int copied)
{
        if (copied <= 0)
                return 0;

        if (sk->__sk_common.skc_family != AF_INET)
                return 0;

        u16 raw_dport = sk->__sk_common.skc_dport;
        u16 dport = ntohs(raw_dport);
        struct ipv4_key_t ipv4_key = {
                .daddr = sk->__sk_common.skc_daddr,
                .dport = dport,
        };
        u64 zero = 0;
        u64* value = ipv4_recv_bytes.lookup_or_init(&ipv4_key, &zero);

        (*value) += copied;

        return 0;
}
"""

class TrafficTracer(object):
    def __init__(self):
        import bcc
        self._bpf = bcc.BPF(text=BPF_PROGRAM)
        self._recv_bytes = self._bpf["ipv4_recv_bytes"]

    def read(self):
        result = {}
        for k, v in self._recv_bytes.items():
            ip = socket.inet_ntop(socket.AF_INET, struct.pack("I", k.daddr))
            port = k.dport
            bytes = v.value
            result[(ip, port)] = bytes
        return result

    def clear(self):
        self._recv_bytes.clear()

TRACER = None # will be initialized to a TrafficTracer later on

class TTHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        global TRACER
        TRACER.clear()
        while 1:
            try:
                msg = self.request.recv(1024)
                if not msg:
                    break
                elif msg == 'kill\r\n':
                    os.kill(os.getpid(),signal.SIGHUP) # send myself sighup
            except IOError, e:
                print >>sys.stderr, "Error while reading:", e
                return
            data = TRACER.read()
            try:
                f = self.request.makefile()
                for (host, port), bytes in data.items():
                    print >>f, "%s\t%d\t%d" % (host, port, bytes)
                #self.request.sendall(repr(data))
            except IOError, e:
                print >>sys.stderr, "Error while writing:", e
                return
            break

def server(portno):
    global TRACER

    server = SocketServer.TCPServer(('localhost', portno), TTHandler)
    server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))

    TRACER = TrafficTracer()
    server.serve_forever()

def client(port, tag, cmd_args):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', port))

    PIPE=subprocess.PIPE
    proc = subprocess.Popen(cmd_args, stdout=PIPE, stderr=subprocess.STDOUT, stdin=PIPE)
    (out, err) = proc.communicate('')

    if proc.returncode == 0:
        s.sendall('ping\r\n')
        for line in s.makefile():
            host, port, bytes = line.strip().split('\t')
            port = int(port)
            bytes = int(bytes)
            if port != 50000 or host == '127.0.0.1':
                continue
            print "%s,%s,%d,recv,%d" % (tag, host, port, bytes)
    else:
        print >>sys.stderr, "Error while running the command"
        sys.stdout.write(out or '')
        sys.stderr.write(err or '')
        print >>sys.stderr, ""

    return proc.returncode

def send_kill(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', port))
    s.sendall('kill\r\n')
    return 0


if __name__ == "__main__":
    argv0 = sys.argv[0]
    args = sys.argv[1:]
    nargs = len(args)

    USAGE = "Usage: %s PORTNO TAG COMMAND ARG...\nOr:    sudo %s -p PORTNO\nOr:    %s -kill PORTNO" % (argv0, argv0, argv0)

    if nargs < 1:
        print >>sys.stderr, USAGE
        sys.exit(42)
    if args[0] == '-p' and nargs == 2:
        portno = int(args[1])
        sys.exit(server(portno) or 0)
    if args[0] == '-kill':
        portno = int(args[1])
        sys.exit(send_kill(portno) or 0)
    if len(args) >= 3:
        portno = int(args[0])
        sys.exit(client(portno, args[1], args[2:]) or 0)

    print >>sys.stderr, USAGE
    sys.exit(42)
