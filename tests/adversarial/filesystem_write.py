# expect: succeeded
#
# Tries to write outside /tmp: the root filesystem and the workspace mount
# are read-only. Exits non-zero if any write succeeds.
import sys

escaped = []
for path in ["/etc/pwned", "/usr/bin/pwned", "/workspace/pwned", "/workspace/main.py", "/pwned"]:
    try:
        with open(path, "w") as f:
            f.write("x")
        escaped.append(path)
    except OSError:
        pass

# /tmp is writable (that's where compiled binaries go).
with open("/tmp/ok", "w") as f:
    f.write("fine")

if escaped:
    print("ESCAPED:", escaped)
    sys.exit(1)
print("CONTAINED: rootfs and workspace are read-only")
