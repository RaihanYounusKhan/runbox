package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("jobs_total", "Jobs.", "status")
	c.Inc("ok")
	c.Add(2, "ok")
	c.Inc(`we"ird`)
	g := r.NewGauge("busy", "Busy slots.")
	g.Set(3)
	h := r.NewHistogram("lat_seconds", "Latency.", []float64{0.1, 1})
	h.Observe(0.05)
	h.Observe(0.5)
	h.Observe(5)
	r.NewGaugeFunc("depth", "Queue depth.", func() float64 { return 7 })

	var buf bytes.Buffer
	r.Write(&buf)
	out := buf.String()

	for _, want := range []string{
		"# TYPE jobs_total counter",
		`jobs_total{status="ok"} 3`,
		`jobs_total{status="we\"ird"} 1`,
		"busy 3",
		"depth 7",
		`lat_seconds_bucket{le="0.1"} 1`,
		`lat_seconds_bucket{le="1"} 2`,
		`lat_seconds_bucket{le="+Inf"} 3`,
		"lat_seconds_sum 5.55",
		"lat_seconds_count 3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// Sorted by metric name.
	if strings.Index(out, "busy") > strings.Index(out, "jobs_total") {
		t.Error("metrics not sorted by name")
	}
}

func TestLabelCountMismatchPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	NewRegistry().NewCounter("x", "x", "a", "b").Inc("only-one")
}
