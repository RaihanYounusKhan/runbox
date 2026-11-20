// Package metrics is a small, dependency-free implementation of the
// Prometheus text exposition format: counters, gauges, histograms and
// callback gauges, each with optional labels.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type collector interface {
	write(w io.Writer)
	name() string
}

// Registry holds metrics and serves them over HTTP.
type Registry struct {
	mu         sync.Mutex
	collectors []collector
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) register(c collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectors = append(r.collectors, c)
}

// Handler serves the registry in Prometheus text format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
}

// Write writes every metric, sorted by name.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	cs := append([]collector(nil), r.collectors...)
	r.mu.Unlock()
	sort.Slice(cs, func(i, j int) bool { return cs[i].name() < cs[j].name() })
	for _, c := range cs {
		c.write(w)
	}
}

// ---- shared label handling ----

type vec struct {
	metricName string
	help       string
	labels     []string
}

func (v vec) name() string { return v.metricName }

func (v vec) key(lvs []string) string {
	if len(lvs) != len(v.labels) {
		panic(fmt.Sprintf("metric %s: got %d label values, want %d", v.metricName, len(lvs), len(v.labels)))
	}
	return strings.Join(lvs, "\xff")
}

func (v vec) labelString(key string, extra ...string) string {
	var parts []string
	if len(v.labels) > 0 {
		vals := strings.Split(key, "\xff")
		for i, l := range v.labels {
			parts = append(parts, fmt.Sprintf(`%s="%s"`, l, escape(vals[i])))
		}
	}
	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, extra[i], escape(extra[i+1])))
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (v vec) header(w io.Writer, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", v.metricName, v.help, v.metricName, typ)
}

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return strings.ReplaceAll(s, `"`, `\"`)
}

func formatFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func sortedKeys[T any](m map[string]T) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// ---- Counter / Gauge ----

// Vec is a counter or gauge with labels.
type Vec struct {
	vec
	typ  string
	mu   sync.Mutex
	vals map[string]float64
}

// NewCounter registers a monotonically increasing counter.
func (r *Registry) NewCounter(name, help string, labels ...string) *Vec {
	c := &Vec{vec: vec{name, help, labels}, typ: "counter", vals: map[string]float64{}}
	r.register(c)
	return c
}

// NewGauge registers a gauge.
func (r *Registry) NewGauge(name, help string, labels ...string) *Vec {
	g := &Vec{vec: vec{name, help, labels}, typ: "gauge", vals: map[string]float64{}}
	r.register(g)
	return g
}

// Add adds delta to the series identified by label values.
func (c *Vec) Add(delta float64, lvs ...string) {
	k := c.key(lvs)
	c.mu.Lock()
	c.vals[k] += delta
	c.mu.Unlock()
}

// Inc adds one.
func (c *Vec) Inc(lvs ...string) { c.Add(1, lvs...) }

// Dec subtracts one (gauges only).
func (c *Vec) Dec(lvs ...string) { c.Add(-1, lvs...) }

// Set sets the value (gauges only).
func (c *Vec) Set(v float64, lvs ...string) {
	k := c.key(lvs)
	c.mu.Lock()
	c.vals[k] = v
	c.mu.Unlock()
}

// Value returns the current value (useful in tests).
func (c *Vec) Value(lvs ...string) float64 {
	k := c.key(lvs)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.vals[k]
}

func (c *Vec) write(w io.Writer) {
	c.header(w, c.typ)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.labels) == 0 && len(c.vals) == 0 {
		fmt.Fprintf(w, "%s 0\n", c.metricName)
	}
	for _, k := range sortedKeys(c.vals) {
		fmt.Fprintf(w, "%s%s %s\n", c.metricName, c.labelString(k), formatFloat(c.vals[k]))
	}
}

// ---- GaugeFunc ----

type gaugeFunc struct {
	vec
	fn func() float64
}

// NewGaugeFunc registers a gauge whose value is computed at scrape time.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.register(&gaugeFunc{vec: vec{metricName: name, help: help}, fn: fn})
}

func (g *gaugeFunc) write(w io.Writer) {
	g.header(w, "gauge")
	fmt.Fprintf(w, "%s %s\n", g.metricName, formatFloat(g.fn()))
}

// ---- Histogram ----

// DefBuckets suit latencies from milliseconds to tens of seconds.
var DefBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

type histSeries struct {
	counts []uint64 // non-cumulative per bucket
	sum    float64
	count  uint64
}

// Histogram tracks a distribution with labels.
type Histogram struct {
	vec
	buckets []float64
	mu      sync.Mutex
	series  map[string]*histSeries
}

// NewHistogram registers a histogram. buckets must be sorted ascending.
func (r *Registry) NewHistogram(name, help string, buckets []float64, labels ...string) *Histogram {
	if buckets == nil {
		buckets = DefBuckets
	}
	h := &Histogram{vec: vec{name, help, labels}, buckets: buckets, series: map[string]*histSeries{}}
	r.register(h)
	return h
}

// Observe records one value.
func (h *Histogram) Observe(v float64, lvs ...string) {
	k := h.key(lvs)
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[k]
	if !ok {
		s = &histSeries{counts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	i := sort.SearchFloat64s(h.buckets, v) // first bucket with bound >= v
	if i < len(h.buckets) {
		s.counts[i]++
	}
	s.sum += v
	s.count++
}

func (h *Histogram) write(w io.Writer) {
	h.header(w, "histogram")
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range sortedKeys(h.series) {
		s := h.series[k]
		var cum uint64
		for i, b := range h.buckets {
			cum += s.counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.metricName, h.labelString(k, "le", formatFloat(b)), cum)
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.metricName, h.labelString(k, "le", "+Inf"), s.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.metricName, h.labelString(k), formatFloat(s.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.metricName, h.labelString(k), s.count)
	}
}
