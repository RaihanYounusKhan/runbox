# Threat model

## Assets

1. **The host**: the worker machine, its kernel, the Docker daemon, and
   other jobs running on it.
2. **Other tenants' data**: their source code, inputs and outputs.
3. **Service availability** for well-behaved tenants.
4. **Integrity of the record** of who ran what, and what came out.

## Adversaries

- **A malicious job author**: submits arbitrary code. This is the main
  adversary, and is assumed to be skilled.
- **A malicious or compromised tenant** using the API: tries to read or
  cancel other tenants' jobs, or exhaust shared capacity.
- **An insider with database access**: tries to rewrite history in the
  audit log.

Out of scope: an attacker with root on the worker host, compromise of the
Docker daemon, and supply-chain attacks on base images (mitigated by pinning
and rebuilding images, not by runbox).

## Trust boundaries

```
 tenant ──HTTP──▶ API ──SQL──▶ Postgres ◀──SQL── worker ──unix socket──▶ dockerd ──▶ [ untrusted code ]
         (auth)                                (root-equivalent)          ▲ the critical boundary
```

The critical boundary is between untrusted code and the host kernel. The
worker holds the Docker socket, which is **root-equivalent on the host**:
anything that can talk to dockerd can start a privileged container.

## Controls inside the sandbox

Every control is set in `containerConfig` (`internal/sandbox/runner.go`) and
pinned by `TestContainerConfigIsLockedDown`.

| Control | Setting | Stops |
|---|---|---|
| Unprivileged user | `User: 65534:65534` | an escape landing as root; writes to root-owned files |
| No capabilities | `CapDrop: [ALL]` | mount, raw sockets, ptrace of other processes, chown, … |
| No privilege gain | `no-new-privileges` | setuid binaries regaining privileges |
| Seccomp | Docker's default allowlist (≈300 syscalls), or `SECCOMP_PROFILE` | rarely-needed syscalls with a history of kernel bugs (`keyctl`, `bpf`, `userfaultfd`, `unshare` without CAP_SYS_ADMIN, …) |
| No network | `NetworkMode: none` | exfiltration, attacks on internal services, crypto-mining pools, reaching the host's dockerd over TCP |
| Read-only rootfs | `ReadonlyRootfs: true` | tampering with the image; persistence |
| Read-only workspace | bind `:ro` | modifying the staged source |
| Bounded scratch space | `/tmp` tmpfs, `size=64m,nosuid,nodev`; `fsize` ulimit | disk exhaustion (tmpfs pages also count against the memory limit) |
| Memory | `Memory == MemorySwap` | host memory pressure; swapping to disk |
| CPU | `NanoCpus` quota | starving co-located jobs |
| Processes | `PidsLimit` | fork bombs exhausting the host PID table |
| File descriptors | `nofile=256` | fd exhaustion |
| Core dumps | `core=0` | leaking memory contents to disk |
| Wall clock | timer + `SIGKILL` | sleeping or blocked programs holding slots |
| Output | streamed with a byte cap; `LogConfig: none` | log-driven disk exhaustion, memory blow-up in the worker |
| Init | tini as PID 1 | zombie accumulation; signals ignored by PID 1 |
| IPC | `IpcMode: private` | shared-memory side channels with other containers |

Docker's defaults add masked `/proc` paths (`/proc/kcore`, `/proc/keys`, …),
a read-only `/sys`, a minimal `/dev`, and an AppArmor profile where
available.

### The remaining risk

All of the above share **one kernel with the host**. A kernel privilege
escalation reachable through allowed syscalls breaks the boundary. The
mitigations, strongest first:

1. **gVisor (`CONTAINER_RUNTIME=runsc`)**: the untrusted code's syscalls are
   handled by a user-space kernel (the Sentry), and only a small, filtered set
   reaches the host kernel. It's a one-line configuration change. Startup
   and syscall-heavy workloads get slower.
2. **MicroVMs (Firecracker / Kata)**: a separate guest kernel per job. Much
   stronger isolation, but more operational work, and it needs KVM. This is
   what multi-tenant serverless platforms use.
3. **Dedicated worker hosts**: run workers on machines that hold nothing else
   of value, with no cloud credentials (block the instance metadata
   endpoint), on a separate network segment from Postgres replicas and
   internal services.

### Deployment guidance for the worker

- Run workers on dedicated hosts or VMs. Never co-locate them with the API or
  the database.
- Prefer **rootless Docker** or gVisor on worker hosts.
- Give the worker its own Postgres role, with only the privileges it needs on
  `jobs`, `audit_log` and `audit_head`.
- Keep sandbox images minimal and rebuild them regularly. The Node image has
  npm removed, for example.

## API-level controls

| Threat | Control |
|---|---|
| Key theft from the database | only SHA-256 hashes of 256-bit random keys are stored |
| Timing attack on the admin token | constant-time comparison |
| Cross-tenant reads, cancels and ID probing | every query is scoped by `api_key_id`; foreign IDs return `404`, not `403` |
| Path traversal through job IDs or language config | job IDs must be canonical UUIDs; language names and file names are checked against strict patterns |
| Oversized requests | `MaxBytesReader`, source and stdin size caps, strict JSON (unknown fields rejected) |
| Resource-exhaustion tenant | per-key rate limit, per-key queue cap, per-key concurrency cap, global backpressure |
| Slowloris | `ReadHeaderTimeout`, `ReadTimeout`, `MaxHeaderBytes` |

Run the API behind TLS termination (a load balancer or ingress). If
`TRUST_PROXY=true`, the client IP recorded in audit entries comes from
`X-Forwarded-For`, so only enable it behind a proxy you control.

## Audit integrity

See [DESIGN.md §5](DESIGN.md#5-audit-log). In summary: triggers stop casual
modification, and the hash chain detects modification by anyone who can
bypass the triggers, unless they rewrite the whole chain. Closing that last
gap requires anchoring the head hash outside the database.

## Verification

`tests/adversarial/` contains one program per threat above, each with its
expected outcome in a header comment. They run in CI against real Docker on
every push:

```
dangerous_syscalls.py  succeeded              CONTAINED: mount and unshare refused
escape_surface.py      succeeded              CONTAINED: no docker socket, host devices or writable kernel interfaces
filesystem_write.py    succeeded              CONTAINED: rootfs and workspace are read-only
fill_tmp.py            succeeded              CONTAINED: write failed after 64 MiB (File too large)
fork_bomb.py           timed_out
infinite_loop.py       timed_out
memory_hog.py          memory_limit_exceeded
network_egress.py      succeeded              CONTAINED: no network access
output_flood.py        output_limit_exceeded
privileges.py          succeeded              CONTAINED: uid=65534 caps=0 no_new_privs=1
sleep_forever.sh       timed_out
```
