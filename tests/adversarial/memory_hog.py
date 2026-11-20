# expect: memory_limit_exceeded
# limits: {"memory_mb": 64, "timeout_ms": 10000}
#
# Allocates (and touches) memory until the cgroup OOM killer steps in.
# Swap is disabled (MemorySwap == Memory), so this can't page to disk.
chunks = []
while True:
    chunks.append(bytearray(8 * 1024 * 1024))
