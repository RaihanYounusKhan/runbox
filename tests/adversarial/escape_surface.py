# expect: succeeded
#
# Looks for common container-escape footholds: the Docker socket, host
# devices, and kernel interfaces that should be masked.
import os
import sys

problems = []
for path in ["/var/run/docker.sock", "/run/docker.sock", "/dev/sda", "/dev/mem", "/dev/kmsg"]:
    if os.path.exists(path):
        problems.append(f"{path} is present")

# Docker masks these; reading them must fail or return nothing useful.
for path in ["/proc/kcore", "/proc/sysrq-trigger"]:
    try:
        with open(path, "rb") as f:
            if f.read(1):
                problems.append(f"{path} is readable")
    except OSError:
        pass

try:
    with open("/sys/kernel/uevent_helper", "w") as f:
        f.write("/tmp/x")
    problems.append("/sys is writable")
except OSError:
    pass

if problems:
    print("ESCAPED:", problems)
    sys.exit(1)
print("CONTAINED: no docker socket, host devices or writable kernel interfaces")
