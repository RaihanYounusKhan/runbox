# expect: succeeded
#
# Calls syscalls used in many container escapes: mount(2) and
# unshare(CLONE_NEWUSER). Without CAP_SYS_ADMIN, and under Docker's seccomp
# profile, both must fail.
import ctypes
import ctypes.util
import sys

libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
problems = []

if libc.mount(b"none", b"/tmp", b"tmpfs", 0, None) == 0:
    problems.append("mount succeeded")

CLONE_NEWUSER = 0x10000000
if libc.unshare(CLONE_NEWUSER) == 0:
    problems.append("unshare(CLONE_NEWUSER) succeeded")

if problems:
    print("ESCAPED:", problems)
    sys.exit(1)
print("CONTAINED: mount and unshare refused")
