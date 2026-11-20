// Package audit implements a tamper-evident, hash-chained audit log.
//
// Every entry's hash is SHA-256 over a canonical encoding of the entry plus
// the previous entry's hash. Changing, inserting or deleting any historical
// entry therefore changes every subsequent hash, which Verify detects.
//
// Entries are appended inside the caller's transaction, so an audit record
// exists if and only if the state change it describes was committed.
package audit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Actions recorded in the log.
const (
	ActionKeyCreated    = "key.created"
	ActionKeyRevoked    = "key.revoked"
	ActionJobSubmitted  = "job.submitted"
	ActionJobStarted    = "job.started"
	ActionJobFinished   = "job.finished"
	ActionJobCancelReq  = "job.cancel_requested"
	ActionJobCanceled   = "job.canceled"
	ActionJobRetried    = "job.retried"
	ActionJobRequeued   = "job.requeued" // lease expired, will run again
	ActionJobLost       = "job.lost"     // lease expired, out of attempts
	ActionJobReleased   = "job.released" // worker shut down mid-run
	ActionAuditVerified = "audit.verified"
	hashDomain          = "runbox-audit-v1\n"
	hashSize            = sha256.Size
)

// GenesisHash is the prev_hash of the first entry.
var GenesisHash = make([]byte, hashSize)

// Entry is one row of the audit log.
type Entry struct {
	Seq      int64           `json:"seq"`
	Time     time.Time       `json:"ts"`
	Actor    string          `json:"actor"`
	Action   string          `json:"action"`
	JobID    string          `json:"job_id,omitempty"`
	Details  json.RawMessage `json:"details"`
	PrevHash []byte          `json:"-"`
	Hash     []byte          `json:"-"`
}

// MarshalJSON renders hashes as hex.
func (e Entry) MarshalJSON() ([]byte, error) {
	type alias Entry
	return json.Marshal(struct {
		alias
		PrevHash string `json:"prev_hash"`
		Hash     string `json:"hash"`
	}{alias(e), hex.EncodeToString(e.PrevHash), hex.EncodeToString(e.Hash)})
}

// ComputeHash returns the chain hash for an entry. Fields are length-prefixed
// so that no two distinct entries share an encoding (e.g. actor="ab",
// action="c" vs actor="a", action="bc").
func ComputeHash(prev []byte, seq int64, ts time.Time, actor, action, jobID string, details []byte) []byte {
	h := sha256.New()
	h.Write([]byte(hashDomain))
	h.Write(prev)
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(seq))
	h.Write(n[:])
	for _, f := range [][]byte{
		[]byte(ts.UTC().Format(time.RFC3339Nano)),
		[]byte(actor),
		[]byte(action),
		[]byte(jobID),
		details,
	} {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], uint32(len(f)))
		h.Write(l[:])
		h.Write(f)
	}
	return h.Sum(nil)
}

// Append adds an entry inside tx. It locks the chain head row, which
// serialises concurrent appenders; the lock is held until tx commits.
func Append(ctx context.Context, tx pgx.Tx, actor, action, jobID string, details any) (Entry, error) {
	if details == nil {
		details = map[string]any{}
	}
	// encoding/json sorts map keys and emits struct fields in declaration
	// order, so the encoding is deterministic. The exact bytes are stored
	// in a json (not jsonb) column, which preserves them verbatim.
	det, err := json.Marshal(details)
	if err != nil {
		return Entry{}, fmt.Errorf("audit: marshal details: %w", err)
	}

	var headSeq int64
	var headHash []byte
	if err := tx.QueryRow(ctx, `SELECT seq, hash FROM audit_head WHERE id = 1 FOR UPDATE`).Scan(&headSeq, &headHash); err != nil {
		return Entry{}, fmt.Errorf("audit: lock head: %w", err)
	}

	e := Entry{
		Seq:      headSeq + 1,
		Time:     time.Now().UTC().Truncate(time.Microsecond), // Postgres precision
		Actor:    actor,
		Action:   action,
		JobID:    jobID,
		Details:  det,
		PrevHash: headHash,
	}
	e.Hash = ComputeHash(e.PrevHash, e.Seq, e.Time, e.Actor, e.Action, e.JobID, e.Details)

	var jobArg any
	if jobID != "" {
		jobArg = jobID
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_log (seq, ts, actor, action, job_id, details, prev_hash, hash)
		 VALUES ($1, $2, $3, $4, $5, $6::json, $7, $8)`,
		e.Seq, e.Time, e.Actor, e.Action, jobArg, string(e.Details), e.PrevHash, e.Hash); err != nil {
		return Entry{}, fmt.Errorf("audit: insert: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE audit_head SET seq = $1, hash = $2 WHERE id = 1`, e.Seq, e.Hash); err != nil {
		return Entry{}, fmt.Errorf("audit: update head: %w", err)
	}
	return e, nil
}

// Report is the outcome of Verify.
type Report struct {
	OK          bool   `json:"ok"`
	Entries     int64  `json:"entries"`
	HeadSeq     int64  `json:"head_seq"`
	HeadHash    string `json:"head_hash"`
	FirstBadSeq int64  `json:"first_bad_seq,omitempty"`
	Problem     string `json:"problem,omitempty"`
}

// Verifier checks a stream of entries in sequence order. It is independent
// of the database so it can be unit-tested and reused for exported logs.
type Verifier struct {
	expectSeq int64
	prev      []byte
	count     int64
	bad       *Report
}

// NewVerifier starts a verification from the genesis entry.
func NewVerifier() *Verifier { return &Verifier{expectSeq: 1, prev: GenesisHash} }

// Add checks the next entry. It returns false once a problem is found.
func (v *Verifier) Add(e Entry) bool {
	if v.bad != nil {
		return false
	}
	fail := func(msg string) bool {
		v.bad = &Report{FirstBadSeq: e.Seq, Problem: msg}
		return false
	}
	if e.Seq != v.expectSeq {
		return fail(fmt.Sprintf("expected seq %d, found %d (entry missing or inserted)", v.expectSeq, e.Seq))
	}
	if !bytes.Equal(e.PrevHash, v.prev) {
		return fail("prev_hash does not match previous entry's hash")
	}
	want := ComputeHash(e.PrevHash, e.Seq, e.Time, e.Actor, e.Action, e.JobID, e.Details)
	if !bytes.Equal(want, e.Hash) {
		return fail("hash mismatch: entry contents were modified")
	}
	v.prev = e.Hash
	v.expectSeq++
	v.count++
	return true
}

// Finish compares the end of the chain with the recorded head and returns
// the report. A head that doesn't match means entries were truncated from
// the end (or the head itself was tampered with).
func (v *Verifier) Finish(headSeq int64, headHash []byte) Report {
	if v.bad != nil {
		r := *v.bad
		r.Entries = v.count
		r.HeadSeq = headSeq
		r.HeadHash = hex.EncodeToString(headHash)
		return r
	}
	r := Report{OK: true, Entries: v.count, HeadSeq: headSeq, HeadHash: hex.EncodeToString(headHash)}
	if headSeq != v.expectSeq-1 || !bytes.Equal(headHash, v.prev) {
		r.OK = false
		r.FirstBadSeq = v.expectSeq
		r.Problem = fmt.Sprintf("chain ends at seq %d but head records seq %d (entries truncated or head modified)", v.expectSeq-1, headSeq)
	}
	return r
}

// Verify walks the whole log in a REPEATABLE READ snapshot so concurrent
// appends don't produce a false mismatch between the rows and the head.
func Verify(ctx context.Context, pool *pgxpool.Pool) (Report, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Report{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var headSeq int64
	var headHash []byte
	if err := tx.QueryRow(ctx, `SELECT seq, hash FROM audit_head WHERE id = 1`).Scan(&headSeq, &headHash); err != nil {
		return Report{}, err
	}

	rows, err := tx.Query(ctx, `SELECT seq, ts, actor, action, COALESCE(job_id::text, ''), details::text, prev_hash, hash
		FROM audit_log ORDER BY seq`)
	if err != nil {
		return Report{}, err
	}
	defer rows.Close()

	v := NewVerifier()
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return Report{}, err
		}
		if !v.Add(e) {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return Report{}, err
	}
	rows.Close()
	return v.Finish(headSeq, headHash), nil
}

// ListFilter selects entries for List.
type ListFilter struct {
	AfterSeq int64
	JobID    string
	Limit    int
}

// List returns entries in sequence order.
func List(ctx context.Context, pool *pgxpool.Pool, f ListFilter) ([]Entry, error) {
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 100
	}
	var jobArg any
	if f.JobID != "" {
		jobArg = f.JobID
	}
	rows, err := pool.Query(ctx, `SELECT seq, ts, actor, action, COALESCE(job_id::text, ''), details::text, prev_hash, hash
		FROM audit_log
		WHERE seq > $1 AND ($2::uuid IS NULL OR job_id = $2::uuid)
		ORDER BY seq LIMIT $3`, f.AfterSeq, jobArg, f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func scanEntry(rows pgx.Rows) (Entry, error) {
	var e Entry
	var det string
	if err := rows.Scan(&e.Seq, &e.Time, &e.Actor, &e.Action, &e.JobID, &det, &e.PrevHash, &e.Hash); err != nil {
		return e, err
	}
	e.Time = e.Time.UTC()
	e.Details = json.RawMessage(det)
	return e, nil
}
