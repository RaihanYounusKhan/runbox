# expect: succeeded
#
# Checks the process runs unprivileged: non-root uid, no capabilities,
# no_new_privs set, and setuid(0) refused.
import os
import sys

problems = []

if os.getuid() == 0 or os.geteuid() == 0:
    problems.append("running as root")

status = {}
with open("/proc/self/status") as f:
    for line in f:
        k, _, v = line.partition(":")
        status[k] = v.strip()

if int(status.get("CapEff", "1"), 16) != 0:
    problems.append(f"effective capabilities: {status['CapEff']}")
if int(status.get("CapBnd", "1"), 16) != 0:
    problems.append(f"bounding capabilities: {status['CapBnd']}")
if status.get("NoNewPrivs") != "1":
    problems.append("no_new_privs not set")

try:
    os.setuid(0)
    problems.append("setuid(0) succeeded")
except PermissionError:
    pass

if problems:
    print("ESCAPED:", problems)
    sys.exit(1)
print(f"CONTAINED: uid={os.getuid()} caps=0 no_new_privs=1")
