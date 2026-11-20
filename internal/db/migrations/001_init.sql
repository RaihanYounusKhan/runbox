-- API keys. Only a SHA-256 of the key is stored; the plaintext is shown once.
CREATE TABLE api_keys (
    id              uuid PRIMARY KEY,
    name            text        NOT NULL,
    key_hash        bytea       NOT NULL UNIQUE,
    key_prefix      text        NOT NULL,          -- first chars, for identification in UIs/logs
    max_concurrent  int         NOT NULL DEFAULT 4 CHECK (max_concurrent > 0),
    rate_rps        double precision,              -- NULL = server default
    rate_burst      int,
    created_at      timestamptz NOT NULL DEFAULT now(),
    revoked_at      timestamptz
);

-- Jobs double as the work queue (claimed with FOR UPDATE SKIP LOCKED).
CREATE TABLE jobs (
    id                uuid PRIMARY KEY,
    api_key_id        uuid        NOT NULL REFERENCES api_keys(id),
    language          text        NOT NULL,
    source            text        NOT NULL,
    source_sha256     text        NOT NULL,
    stdin             bytea       NOT NULL DEFAULT ''::bytea,
    limits            jsonb       NOT NULL,
    status            text        NOT NULL CHECK (status IN (
                          'queued','running','succeeded','failed','timed_out',
                          'memory_limit_exceeded','output_limit_exceeded','canceled','error')),
    attempts          int         NOT NULL DEFAULT 0,
    max_attempts      int         NOT NULL,
    cancel_requested  boolean     NOT NULL DEFAULT false,
    run_after         timestamptz NOT NULL DEFAULT now(),
    lease_owner       text,
    lease_expires_at  timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now(),
    started_at        timestamptz,
    finished_at       timestamptz,
    exit_code         int,
    stdout            bytea,
    stderr            bytea,
    stdout_truncated  boolean     NOT NULL DEFAULT false,
    stderr_truncated  boolean     NOT NULL DEFAULT false,
    duration_ms       bigint,
    error             text
);

-- Hot path for the dequeue query: only queued rows, in FIFO order.
CREATE INDEX jobs_queued_idx  ON jobs (created_at) WHERE status = 'queued';
-- Used by the per-key concurrency check and the lease reaper.
CREATE INDEX jobs_running_idx ON jobs (api_key_id) WHERE status = 'running';
CREATE INDEX jobs_lease_idx   ON jobs (lease_expires_at) WHERE status = 'running';
CREATE INDEX jobs_key_created_idx ON jobs (api_key_id, created_at DESC);

-- Tamper-evident audit log. Each row's hash covers the previous row's hash,
-- so editing or deleting any row breaks every hash after it.
CREATE TABLE audit_log (
    seq        bigint      PRIMARY KEY,
    ts         timestamptz NOT NULL,
    actor      text        NOT NULL,
    action     text        NOT NULL,
    job_id     uuid,
    details    json        NOT NULL,   -- json (not jsonb) preserves the exact hashed bytes
    prev_hash  bytea       NOT NULL,
    hash       bytea       NOT NULL
);
CREATE INDEX audit_log_job_idx ON audit_log (job_id) WHERE job_id IS NOT NULL;

-- Single-row table holding the chain head. Appenders lock this row, which
-- serialises appends and makes seq gap-free.
CREATE TABLE audit_head (
    id    int    PRIMARY KEY CHECK (id = 1),
    seq   bigint NOT NULL,
    hash  bytea  NOT NULL
);
INSERT INTO audit_head (id, seq, hash) VALUES (1, 0, decode(repeat('00', 32), 'hex'));

-- First line of defence: the database refuses UPDATE/DELETE/TRUNCATE.
-- (A superuser can disable triggers; the hash chain is the second line.)
CREATE FUNCTION audit_log_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_log_no_update_delete
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION audit_log_immutable();

CREATE TRIGGER audit_log_no_truncate
    BEFORE TRUNCATE ON audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION audit_log_immutable();
