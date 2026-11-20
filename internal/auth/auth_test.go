package auth

import (
	"strings"
	"testing"

	"runbox/internal/ids"
)

func TestKeys(t *testing.T) {
	a, b := NewKey(), NewKey()
	if a == b {
		t.Fatal("keys collide")
	}
	if !strings.HasPrefix(a, Prefix) || len(a) != len(Prefix)+43 {
		t.Fatalf("unexpected key format %q", a)
	}
	if string(Hash(a)) == string(Hash(b)) || len(Hash(a)) != 32 {
		t.Fatal("bad hash")
	}
	if DisplayPrefix(a) != a[:10] {
		t.Fatalf("display prefix = %q", DisplayPrefix(a))
	}
}

func TestBearerToken(t *testing.T) {
	for in, want := range map[string]string{
		"Bearer abc":   "abc",
		"bearer  abc ": "abc",
		"Basic abc":    "",
		"Bearer":       "",
		"":             "",
	} {
		got, ok := BearerToken(in)
		if got != want || ok != (want != "") {
			t.Errorf("BearerToken(%q) = %q,%v", in, got, ok)
		}
	}
}

func TestUUID(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		u := ids.NewUUID()
		if !ids.ValidUUID(u) || u[14] != '4' {
			t.Fatalf("bad uuid %q", u)
		}
		if seen[u] {
			t.Fatal("duplicate uuid")
		}
		seen[u] = true
	}
	for _, bad := range []string{"", "../../etc", "6F1C2D3E-0000-4000-8000-000000000001", "6f1c2d3e00004000800000000000000001"} {
		if ids.ValidUUID(bad) {
			t.Errorf("accepted %q", bad)
		}
	}
}
