# expect: succeeded
#
# Tries to reach the network several ways. Every attempt must fail; the
# program exits non-zero if any succeeds.
import socket
import sys

escaped = []

def attempt(name, fn):
    try:
        fn()
        escaped.append(name)
    except OSError:
        pass

attempt("tcp 1.1.1.1:443", lambda: socket.create_connection(("1.1.1.1", 443), timeout=2))
attempt("tcp 8.8.8.8:53", lambda: socket.create_connection(("8.8.8.8", 53), timeout=2))
attempt("dns lookup", lambda: socket.getaddrinfo("example.com", 80))
attempt("tcp host gateway 172.17.0.1:2375", lambda: socket.create_connection(("172.17.0.1", 2375), timeout=2))

ifaces = [name for _, name in socket.if_nameindex()]
if ifaces != ["lo"]:
    escaped.append(f"network interfaces present: {ifaces}")

if escaped:
    print("ESCAPED:", escaped)
    sys.exit(1)
print("CONTAINED: no network access")
