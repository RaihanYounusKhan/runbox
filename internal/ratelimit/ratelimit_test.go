package ratelimit

import (
	"testing"
	"time"
)

func TestTokenBucket(t *testing.T) {
	now := time.Unix(0, 0)
	l := New()
	l.now = func() time.Time { return now }

	// Burst of 5 is allowed immediately.
	for i := 0; i < 5; i++ {
		if ok, _ := l.Allow("k", 2, 5); !ok {
			t.Fatalf("request %d within burst rejected", i)
		}
	}
	ok, wait := l.Allow("k", 2, 5)
	if ok {
		t.Fatal("request beyond burst allowed")
	}
	if wait <= 0 || wait > 500*time.Millisecond {
		t.Fatalf("wait = %v, want (0, 500ms]", wait)
	}

	// 2 rps: after 500ms exactly one more token.
	now = now.Add(500 * time.Millisecond)
	if ok, _ := l.Allow("k", 2, 5); !ok {
		t.Fatal("refilled token rejected")
	}
	if ok, _ := l.Allow("k", 2, 5); ok {
		t.Fatal("second token allowed before refill")
	}

	// Refill never exceeds burst.
	now = now.Add(time.Hour)
	for i := 0; i < 5; i++ {
		l.Allow("k", 2, 5)
	}
	if ok, _ := l.Allow("k", 2, 5); ok {
		t.Fatal("bucket overfilled past burst")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l := New()
	for i := 0; i < 3; i++ {
		l.Allow("a", 1, 3)
	}
	if ok, _ := l.Allow("a", 1, 3); ok {
		t.Fatal("key a should be exhausted")
	}
	if ok, _ := l.Allow("b", 1, 3); !ok {
		t.Fatal("key b affected by key a")
	}
}

func TestDisabledAndSweep(t *testing.T) {
	now := time.Unix(0, 0)
	l := New()
	l.now = func() time.Time { return now }
	if ok, _ := l.Allow("x", 0, 0); !ok {
		t.Fatal("zero rate should disable limiting")
	}
	l.Allow("y", 1, 1)
	now = now.Add(time.Hour)
	if n := l.Sweep(time.Minute); n != 1 {
		t.Fatalf("swept %d buckets, want 1", n)
	}
}
