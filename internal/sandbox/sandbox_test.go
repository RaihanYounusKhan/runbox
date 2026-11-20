package sandbox

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func frame(stream byte, payload string) []byte {
	h := make([]byte, 8)
	h[0] = stream
	binary.BigEndian.PutUint32(h[4:], uint32(len(payload)))
	return append(h, payload...)
}

func TestDemuxSeparatesStreams(t *testing.T) {
	var in bytes.Buffer
	in.Write(frame(1, "hello "))
	in.Write(frame(2, "oops\n"))
	in.Write(frame(1, "world\n"))
	out, err := demux(&in, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.stdout.buf.String(); got != "hello world\n" {
		t.Errorf("stdout = %q", got)
	}
	if got := out.stderr.buf.String(); got != "oops\n" {
		t.Errorf("stderr = %q", got)
	}
	if out.stdout.truncated || out.stderr.truncated {
		t.Error("unexpected truncation")
	}
}

func TestDemuxCapsOutputAndSignalsOnce(t *testing.T) {
	var in bytes.Buffer
	for i := 0; i < 10; i++ {
		in.Write(frame(1, strings.Repeat("x", 100)))
	}
	calls := 0
	out, err := demux(&in, 250, func() { calls++ })
	if err != nil {
		t.Fatal(err)
	}
	if out.stdout.buf.Len() != 250 || !out.stdout.truncated {
		t.Errorf("len=%d truncated=%v, want 250/true", out.stdout.buf.Len(), out.stdout.truncated)
	}
	if calls != 1 {
		t.Errorf("overflow callback called %d times, want 1", calls)
	}
}

func TestDemuxToleratesTruncatedStream(t *testing.T) {
	b := frame(1, "complete")
	b = append(b, frame(1, "partial-frame")[:12]...) // cut mid-payload
	out, err := demux(bytes.NewReader(b), 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.stdout.buf.String() != "complete" {
		t.Errorf("stdout = %q", out.stdout.buf.String())
	}
}

func TestDemuxRejectsGarbage(t *testing.T) {
	if _, err := demux(bytes.NewReader(frame(9, "x")), 1024, nil); err == nil {
		t.Fatal("expected error for unknown stream id")
	}
}

func TestLimits(t *testing.T) {
	l := Limits{}.WithDefaults()
	if l != DefaultLimits {
		t.Fatalf("WithDefaults = %+v", l)
	}
	if err := l.Validate(); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
	if err := MaxLimits.Validate(); err != nil {
		t.Fatalf("max limits invalid: %v", err)
	}
	bad := DefaultLimits
	bad.MemoryMB = MaxLimits.MemoryMB + 1
	bad.TimeoutMs = 1
	err := bad.Validate()
	if err == nil || !strings.Contains(err.Error(), "memory_mb") || !strings.Contains(err.Error(), "timeout_ms") {
		t.Fatalf("expected both fields reported, got %v", err)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		reason    Status
		code      int
		oom, trun bool
		want      Status
	}{
		{"", 0, false, false, StatusSucceeded},
		{"", 1, false, false, StatusFailed},
		{"", 137, false, false, StatusMemoryExceeded},
		{"", 1, true, false, StatusMemoryExceeded},
		{"", 0, false, true, StatusOutputExceeded},
		{StatusOutputExceeded, 137, false, true, StatusOutputExceeded},
		{StatusTimedOut, 137, false, false, StatusTimedOut},
		{StatusTimedOut, 137, false, true, StatusTimedOut},
		{StatusCanceled, 137, true, true, StatusCanceled},
	}
	for _, c := range cases {
		if got := classify(c.reason, c.code, c.oom, c.trun); got != c.want {
			t.Errorf("classify(%q, %d, oom=%v, trunc=%v) = %s, want %s", c.reason, c.code, c.oom, c.trun, got, c.want)
		}
	}
}

func TestContainerConfigIsLockedDown(t *testing.T) {
	r := NewDockerRunner(nil, DockerRunnerConfig{WorkerID: "w1", JobsDir: "/jobs"})
	lang := DefaultLanguages()[0]
	cfg := r.containerConfig(Spec{JobID: "x", Language: lang, Limits: DefaultLimits}, "/host/jobs/x")
	hc := cfg.HostConfig
	switch {
	case cfg.User != sandboxUser:
		t.Error("must not run as root")
	case !cfg.NetworkDisabled || hc.NetworkMode != "none":
		t.Error("network must be disabled")
	case !hc.ReadonlyRootfs:
		t.Error("rootfs must be read-only")
	case len(hc.CapDrop) != 1 || hc.CapDrop[0] != "ALL":
		t.Error("all capabilities must be dropped")
	case hc.Memory != hc.MemorySwap || hc.Memory != int64(DefaultLimits.MemoryMB)<<20:
		t.Error("memory limit wrong or swap allowed")
	case hc.PidsLimit == nil || *hc.PidsLimit != int64(DefaultLimits.Pids):
		t.Error("pids limit missing")
	case hc.LogConfig.Type != "none":
		t.Error("output must not be logged to disk")
	case len(hc.Binds) != 1 || !strings.HasSuffix(hc.Binds[0], ":/workspace:ro"):
		t.Errorf("workspace must be mounted read-only, got %v", hc.Binds)
	}
	found := false
	for _, o := range hc.SecurityOpt {
		found = found || o == "no-new-privileges:true"
	}
	if !found {
		t.Error("no-new-privileges missing")
	}
	if !strings.Contains(hc.Tmpfs["/tmp"], "nosuid") || !strings.Contains(hc.Tmpfs["/tmp"], "size=") {
		t.Errorf("tmpfs options too permissive: %q", hc.Tmpfs["/tmp"])
	}
}

func TestLoadLanguages(t *testing.T) {
	m, err := LoadLanguages("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m["python"]; !ok {
		t.Fatal("python missing from defaults")
	}

	dir := t.TempDir()
	write := func(s string) string {
		p := filepath.Join(dir, "langs.json")
		if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if _, err := LoadLanguages(write(`[{"name":"py","image":"img","file_name":"m.py","command":"python3 m.py"}]`)); err != nil {
		t.Fatalf("valid file rejected: %v", err)
	}
	for _, bad := range []string{
		`[{"name":"../evil","image":"img","file_name":"m.py","command":"x"}]`,
		`[{"name":"py","image":"img","file_name":"../../etc/passwd","command":"x"}]`,
		`[{"name":"py","image":"img","file_name":"stdin","command":"x"}]`,
		`[{"name":"py","image":"","file_name":"m.py","command":"x"}]`,
		`[]`,
	} {
		if _, err := LoadLanguages(write(bad)); err == nil {
			t.Errorf("accepted invalid config %s", bad)
		}
	}
}
