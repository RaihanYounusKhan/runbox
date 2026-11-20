# expect: timed_out
# limits: {"timeout_ms": 2000, "pids": 32}
#
# Classic fork bomb. The pids cgroup caps the container at 32 processes, so
# fork() starts failing almost immediately and the host is unaffected. The
# job is then stopped by the wall-clock limit.
import os

while True:
    try:
        os.fork()
    except OSError:
        pass
