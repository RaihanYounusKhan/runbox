# expect: succeeded
# limits: {"memory_mb": 256}
#
# Tries to fill the writable /tmp. It's a size-capped tmpfs (and an fsize
# ulimit applies), so writes must start failing well before 1 GiB.
import sys

written = 0
block = b"\0" * (1024 * 1024)
try:
    with open("/tmp/fill", "wb") as f:
        while written < 1024 * 1024 * 1024:
            f.write(block)
            f.flush()
            written += len(block)
except OSError as e:
    print(f"CONTAINED: write failed after {written // (1024 * 1024)} MiB ({e.strerror})")
    sys.exit(0)

print(f"ESCAPED: wrote {written // (1024 * 1024)} MiB to /tmp")
sys.exit(1)
