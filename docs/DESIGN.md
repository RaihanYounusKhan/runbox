# Design notes

This document explains the main decisions in runbox: what each part does,
and the alternatives considered.

## 1. Postgres as the queue

**Decision:** the `jobs` table *is* the queue. Workers claim jobs with

```sql
SELECT ... FROM jobs WHERE status = 'queued' ... ORDER BY created_at
LIMIT 1 FOR UPDATE SKIP LOCKED
```

inside an `UPDATE ... RETURNING`.

**Why:**
- A job's state, its result and its audit record change in **one
  transaction**. With a separate broker (Redis, RabbitMQ) you have to
  coordinate two systems. You either accept windows where the queue and the
  database disagree, or you build an outbox.
- `SKIP LOCKED` lets many workers dequeue concurrently without blocking each
  other or double-claiming a row.
- It's one less piece of infrastructure to run.

**Trade-off:** Postgres won't match a dedicated broker at very high
throughput. Here each job costs hundreds of milliseconds of container work,
so the queue is far from the bottleneck: a claim takes about a millisecond.

**Wake-ups:** `CreateJob` runs `pg_notify('runbox_jobs', id)` inside the
insert transaction, so the notification is only delivered once the row is
visible. Idle worker slots wait on a channel fed by a `LISTEN` connection,
and also poll with jitter as a fallback, because notifications sent while the
listener is reconnecting are lost.

## 2. Leases, heartbeats, fencing: at-least-once execution

A claimed job gets `lease_owner = <worker id>` and `lease_expires_at = now() +
LEASE_DURATION`. While the job runs, the worker extends the lease every
`LEASE_DURATION/3`.

- **Worker crash or partition:** the lease expires. The reaper, which runs in
  every worker and is safe to run concurrently thanks to `SKIP LOCKED`, puts
  the job back in the queue with exponential backoff, or marks it `error`
  once it reaches `max_attempts`.
- **Fencing:** every write after the claim includes
  `WHERE lease_owner = $me AND status = 'running'`. Consider a worker that
  was paused (say, a long GC pause or a network partition) and wakes up after
  its job was reaped and rerun elsewhere. It finds the update matches zero
  rows, and its stale result is discarded. The heartbeat notices the lost
  lease too, and kills the container early.
- **Semantics:** this gives **at-least-once** execution. A program can run
  twice if its worker disappears mid-run. Exactly-once execution of arbitrary
  code is impossible without the code's cooperation. The audit log records
  every attempt (`job.started`, `job.requeued`, ...), so duplicate runs are
  visible.

**Infrastructure errors vs program errors.** `Runner.Run` returns an `error`
only when *we* failed: the daemon was unreachable or the image was missing.
Those are retried. Anything the program did, including crashing or
exceeding a limit, is a `Result` and is never retried.

**Graceful shutdown** has two phases. On SIGTERM, slots stop claiming new
jobs, and in-flight jobs get `SHUTDOWN_GRACE` to finish. After that their
containers are killed and the jobs are *released* back to the queue with the
attempt refunded, because a deploy isn't the job's fault. Compose's
`stop_grace_period` is set longer than the grace period for this reason.

## 3. Fairness and overload

- **Per-key concurrency cap** (`api_keys.max_concurrent`): the claim query
  skips jobs whose key already has that many running jobs. One tenant
  flooding the queue can occupy at most its cap's worth of slots, so other
  tenants' jobs still get picked up. The check is a subquery inside the claim,
  which makes it a *soft* limit: two simultaneous claims can overshoot by one.
  A per-key advisory lock would make it strict at the cost of serializing
  claims per key.
- **Backpressure:** submissions are refused once the global queue
  (`503 queue_full`) or the key's queue (`429 too_many_queued`) is too deep.
  Letting the queue grow without bound only turns overload into unbounded
  latency for everyone.
- **Rate limiting:** an in-memory token bucket per key. It's per replica,
  which is a documented limitation.

## 4. The sandbox

### Talking to Docker

`internal/sandbox/docker.go` is a ~300-line client for the Engine API over
the unix socket. It negotiates the API version at startup. Why not the SDK?
The SDK pulls in a large dependency tree. More importantly, writing out the
`HostConfig` by hand puts every isolation knob in one reviewable function
(`containerConfig`), and a unit test (`TestContainerConfigIsLockedDown`)
fails if anyone loosens it.

### Lifecycle of one job

1. Write `main.<ext>` and `stdin` to `JOBS_DIR/<job id>/`. The ID is
   validated as a UUID before it touches the filesystem.
2. `create` the container. The job directory is bind-mounted read-only at
   `/workspace`.
3. **`attach` before `start`**, so no early output is lost. Docker's
   multiplexed stream is decoded by `demux`, which keeps at most
   `output_bytes` per stream and signals as soon as either overflows.
4. `start`, then `select` over: container exit, timeout, overflow, and
   cancellation (the job context, which covers user cancel, lost lease, and
   shutdown). Every outcome except a natural exit sends `SIGKILL`.
5. Inspect for `OOMKilled`, classify the result, and remove the container in
   the background. Removal takes about 100 ms and the result doesn't depend
   on it. Containers left behind by a crash are swept by `CleanupOrphans` at
   startup, using container labels.

### Classification order

`canceled` > `timed_out` > `output_limit_exceeded` > `memory_limit_exceeded`
> exit code. What *we* did to the container wins over how it happened to
exit, since every kill we send also shows up as exit code 137. With tini as
PID 1 under cgroup v2, an OOM kill of a child process doesn't always set
Docker's `OOMKilled` flag. So a 137 exit that we didn't cause is treated as
an OOM.

### Why output is streamed, not logged

With the default `json-file` log driver, a program printing in a loop writes
gigabytes to the host's disk before the timeout. With `LogConfig: none` plus
attach, output is never written to disk, and we kill the container at the
byte limit. The `output_flood.py` test is cut off within milliseconds.

## 5. Audit log

### Format

```
hash_n = SHA256("runbox-audit-v1\n" ‖ hash_{n-1} ‖ seq_n ‖ len‖ts ‖ len‖actor ‖ len‖action ‖ len‖job_id ‖ len‖details)
```

- **Length prefixes** prevent ambiguity at field boundaries. Without them,
  (`ab`,`c`) and (`a`,`bc`) would hash the same.
- **`details` is stored in a `json` column, not `jsonb`.** `jsonb`
  normalizes key order and whitespace, which would change the bytes that were
  hashed. `json` stores them verbatim.
- **Timestamps** are truncated to microseconds before hashing, because that's
  Postgres's precision. They're formatted in UTC, so a client's time zone
  can't change the hash.

### Concurrency

Appenders `SELECT ... FOR UPDATE` the single `audit_head` row. This
serializes appends, so `seq` has no gaps and each entry links to its true
predecessor. The lock is held until the surrounding business transaction
commits, which makes audit throughput the system's serialization point.
That's acceptable at thousands of entries per second. Beyond that, the options
are batching, or partitioning into several chains that are periodically
anchored together.

### Threats it addresses

| Attack | Detected by |
|---|---|
| `UPDATE`/`DELETE`/`TRUNCATE` by the app or an ordinary role | triggers reject it |
| Superuser disables triggers and edits a row | hash mismatch at that seq |
| …and recomputes that row's hash | `prev_hash` mismatch at the next seq |
| Deletes rows in the middle | seq gap |
| Deletes rows from the end | head (`audit_head`) doesn't match the last row |
| Rewrites the entire chain and head | **not detected** without an external anchor (future work) |

Atomicity matters as much as tamper-evidence: since the audit entry is
written in the same transaction as the change, an audited action that didn't
happen, or a change with no audit entry, can't exist.

## 6. Latency work

The first benchmark showed single-client end-to-end p50 of **356 ms**, while
sandbox execution was only about 66 ms. Timing each phase of `Run` showed:

| Phase | Time |
|---|---|
| create container | ~20 ms |
| attach + start | ~100 ms |
| program runs | ~66 ms |
| collect + inspect | <5 ms |

That accounted for about 190 ms. The remaining ~170 ms came from the long-poll
handler, which re-read the job with backoff intervals up to 500 ms. A job
finishing just after a check waited for the next one.

**Fix:** completions now `pg_notify('runbox_job_done', id)` in the completion
transaction. The API keeps one `LISTEN` connection and a hub that maps job IDs
to waiting requests. A waiter subscribes *before* its first read, so it can't
miss a notification that arrives between the read and the wait. If the
listener is down, waiters fall back to frequent re-checks.

**Result:** p50 dropped from 356 ms to **183 ms**. Moving container removal
off the critical path was tried first and made no measurable difference;
it's kept because it costs nothing.

The remaining overhead is mostly runc's container start. The next step would
be a warm pool of pre-created containers.

## 7. Things deliberately left out

- **A container warm pool.** It complicates the isolation story: pooled
  containers must never be reused across jobs, so the gain is only creation
  time.
- **Kubernetes.** A `Runner` implementation that creates Pods (with gVisor's
  `RuntimeClass`) would slot in behind the same interface. Docker keeps the
  project runnable on a laptop.
- **Streaming output to clients.** The API returns output once the job
  finishes. Server-Sent Events from the worker, via Postgres or a pub/sub
  system, would be the next feature.
