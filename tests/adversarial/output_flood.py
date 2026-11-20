# expect: output_limit_exceeded
# limits: {"output_bytes": 4096, "timeout_ms": 10000}
#
# Writes output forever. The worker counts bytes as they stream in and kills
# the container as soon as the limit is crossed, rather than buffering
# gigabytes or waiting for the timeout. Nothing is written to Docker's log
# files (LogConfig: none), so this can't fill the host disk either.
import sys

line = "A" * 1023 + "\n"
while True:
    sys.stdout.write(line)
