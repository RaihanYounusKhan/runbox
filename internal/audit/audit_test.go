package audit

import (
	"encoding/json"
	"testing"
	"time"
)

// buildChain constructs a valid in-memory chain of n entries.
func buildChain(n int) []Entry {
	prev := GenesisHash
	base := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	out := make([]Entry, n)
	for i := 0; i < n; i++ {
		e := Entry{
			Seq:      int64(i + 1),
			Time:     base.Add(time.Duration(i) * time.Second),
			Actor:    "worker:w1",
			Action:   ActionJobFinished,
			JobID:    "6f1c2d3e-0000-4000-8000-000000000001",
			Details:  json.RawMessage(`{"exit_code":0,"status":"succeeded"}`),
			PrevHash: prev,
		}
		e.Hash = ComputeHash(e.PrevHash, e.Seq, e.Time, e.Actor, e.Action, e.JobID, e.Details)
		prev = e.Hash
		out[i] = e
	}
	return out
}

func verify(chain []Entry) Report {
	v := NewVerifier()
	for _, e := range chain {
		if !v.Add(e) {
			break
		}
	}
	var headSeq int64
	head := GenesisHash
	if len(chain) > 0 {
		headSeq = chain[len(chain)-1].Seq
		head = chain[len(chain)-1].Hash
	}
	return v.Finish(headSeq, head)
}

func TestValidChain(t *testing.T) {
	if r := verify(buildChain(50)); !r.OK || r.Entries != 50 {
		t.Fatalf("valid chain rejected: %+v", r)
	}
	if r := verify(nil); !r.OK {
		t.Fatalf("empty chain rejected: %+v", r)
	}
}

func TestHashIsDeterministicAndFieldSensitive(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	a := ComputeHash(GenesisHash, 1, ts, "ab", "c", "", []byte("{}"))
	b := ComputeHash(GenesisHash, 1, ts, "ab", "c", "", []byte("{}"))
	if string(a) != string(b) {
		t.Fatal("hash not deterministic")
	}
	// Length prefixing: shifting bytes between fields must change the hash.
	c := ComputeHash(GenesisHash, 1, ts, "a", "bc", "", []byte("{}"))
	if string(a) == string(c) {
		t.Fatal("field boundary ambiguity: (ab,c) and (a,bc) hash equal")
	}
	// Time zone of the input must not matter.
	d := ComputeHash(GenesisHash, 1, ts.In(time.FixedZone("X", 3600)), "ab", "c", "", []byte("{}"))
	if string(a) != string(d) {
		t.Fatal("hash depends on time zone")
	}
}

func TestTamperDetection(t *testing.T) {
	cases := map[string]struct {
		mutate  func([]Entry) []Entry
		badSeq  int64
		checkOK bool
	}{
		"modified details": {
			mutate: func(c []Entry) []Entry {
				c[9].Details = json.RawMessage(`{"exit_code":1,"status":"failed"}`)
				return c
			},
			badSeq: 10,
		},
		"modified actor": {
			mutate: func(c []Entry) []Entry { c[3].Actor = "worker:evil"; return c },
			badSeq: 4,
		},
		"modified timestamp": {
			mutate: func(c []Entry) []Entry { c[0].Time = c[0].Time.Add(time.Microsecond); return c },
			badSeq: 1,
		},
		"deleted middle entry": {
			mutate: func(c []Entry) []Entry { return append(c[:5:5], c[6:]...) },
			badSeq: 7, // seq 6 is missing; seq 7 shows up where 6 was expected
		},
		"re-hashed modified entry": {
			// An attacker who edits a row and recomputes its hash still
			// breaks the link to the next entry.
			mutate: func(c []Entry) []Entry {
				e := &c[4]
				e.Actor = "worker:evil"
				e.Hash = ComputeHash(e.PrevHash, e.Seq, e.Time, e.Actor, e.Action, e.JobID, e.Details)
				return c
			},
			badSeq: 6,
		},
		"reordered entries": {
			mutate: func(c []Entry) []Entry { c[2], c[3] = c[3], c[2]; return c },
			badSeq: 4,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			chain := tc.mutate(buildChain(20))
			r := verify(chain)
			if r.OK {
				t.Fatalf("tampering not detected: %+v", r)
			}
			if r.FirstBadSeq != tc.badSeq {
				t.Fatalf("first bad seq = %d, want %d (%s)", r.FirstBadSeq, tc.badSeq, r.Problem)
			}
		})
	}
}

func TestTruncationDetectedViaHead(t *testing.T) {
	chain := buildChain(20)
	head := chain[len(chain)-1]
	v := NewVerifier()
	for _, e := range chain[:15] { // last five rows deleted
		v.Add(e)
	}
	r := v.Finish(head.Seq, head.Hash)
	if r.OK {
		t.Fatal("truncation not detected")
	}
	if r.FirstBadSeq != 16 {
		t.Fatalf("first bad seq = %d, want 16", r.FirstBadSeq)
	}
}
