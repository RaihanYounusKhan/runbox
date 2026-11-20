// Command runbox-bench is a load generator. It submits N jobs with C
// concurrent clients, waits for each to finish, and reports throughput and
// latency percentiles (end-to-end, queue wait, and execution time).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type jobResp struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	QueueWaitMs *int64 `json:"queue_wait_ms"`
	Result      *struct {
		DurationMs *int64 `json:"duration_ms"`
	} `json:"result"`
}

type sample struct {
	status    string
	endToEnd  time.Duration
	queueWait time.Duration
	exec      time.Duration
	err       error
}

func main() {
	url := flag.String("url", "http://localhost:8080", "API base URL")
	key := flag.String("key", os.Getenv("RUNBOX_API_KEY"), "API key (or RUNBOX_API_KEY)")
	n := flag.Int("n", 200, "total jobs")
	c := flag.Int("c", 20, "concurrent clients")
	lang := flag.String("lang", "python", "language")
	code := flag.String("code", "print(sum(range(100000)))", "source code to run")
	asJSON := flag.Bool("json", false, "print results as JSON")
	flag.Parse()
	if *key == "" {
		fmt.Fprintln(os.Stderr, "an API key is required (-key or RUNBOX_API_KEY)")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	body, _ := json.Marshal(map[string]any{"language": *lang, "source": *code})

	do := func(method, path string, payload []byte) (int, []byte, error) {
		req, _ := http.NewRequest(method, *url+path, bytes.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+*key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return resp.StatusCode, b, err
	}

	runOne := func() sample {
		start := time.Now()
		var code int
		var b []byte
		var err error
		for attempt := 0; ; attempt++ { // honour backpressure with simple retries
			code, b, err = do(http.MethodPost, "/v1/jobs", body)
			if err != nil {
				return sample{err: err}
			}
			if (code == 429 || code == 503) && attempt < 50 {
				time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
				continue
			}
			break
		}
		if code != http.StatusAccepted {
			return sample{err: fmt.Errorf("submit: HTTP %d: %s", code, b)}
		}
		var j jobResp
		_ = json.Unmarshal(b, &j)
		for {
			code, b, err = do(http.MethodGet, "/v1/jobs/"+j.ID+"?wait=25s", nil)
			if err != nil {
				return sample{err: err}
			}
			if code != 200 {
				return sample{err: fmt.Errorf("get: HTTP %d: %s", code, b)}
			}
			_ = json.Unmarshal(b, &j)
			if j.Status != "queued" && j.Status != "running" {
				break
			}
		}
		s := sample{status: j.Status, endToEnd: time.Since(start)}
		if j.QueueWaitMs != nil {
			s.queueWait = time.Duration(*j.QueueWaitMs) * time.Millisecond
		}
		if j.Result != nil && j.Result.DurationMs != nil {
			s.exec = time.Duration(*j.Result.DurationMs) * time.Millisecond
		}
		return s
	}

	jobs := make(chan struct{}, *n)
	for i := 0; i < *n; i++ {
		jobs <- struct{}{}
	}
	close(jobs)

	var mu sync.Mutex
	var samples []sample
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				s := runOne()
				mu.Lock()
				samples = append(samples, s)
				done := len(samples)
				mu.Unlock()
				if !*asJSON && done%max(1, *n/10) == 0 {
					fmt.Fprintf(os.Stderr, "  %d/%d\n", done, *n)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	statuses := map[string]int{}
	var e2e, qw, ex []time.Duration
	errs := 0
	var firstErr error
	for _, s := range samples {
		if s.err != nil {
			errs++
			if firstErr == nil {
				firstErr = s.err
			}
			continue
		}
		statuses[s.status]++
		e2e = append(e2e, s.endToEnd)
		qw = append(qw, s.queueWait)
		ex = append(ex, s.exec)
	}

	report := map[string]any{
		"jobs": *n, "clients": *c, "language": *lang,
		"elapsed_s":      elapsed.Seconds(),
		"throughput_jps": float64(len(e2e)) / elapsed.Seconds(),
		"statuses":       statuses,
		"client_errors":  errs,
		"end_to_end_ms":  pct(e2e),
		"queue_wait_ms":  pct(qw),
		"execution_ms":   pct(ex),
	}
	if *asJSON {
		b, _ := json.MarshalIndent(report, "", "  ")
		fmt.Println(string(b))
		return
	}
	fmt.Printf("\n%d jobs, %d clients, %s in %.1fs  →  %.1f jobs/s\n", *n, *c, *lang, elapsed.Seconds(), report["throughput_jps"])
	fmt.Printf("statuses: %v   client errors: %d\n\n", statuses, errs)
	fmt.Printf("%-14s %8s %8s %8s %8s\n", "latency (ms)", "p50", "p90", "p99", "max")
	for _, row := range []struct {
		name string
		d    []time.Duration
	}{{"end-to-end", e2e}, {"queue wait", qw}, {"execution", ex}} {
		p := pct(row.d)
		fmt.Printf("%-14s %8.0f %8.0f %8.0f %8.0f\n", row.name, p["p50"], p["p90"], p["p99"], p["max"])
	}
	if firstErr != nil {
		fmt.Printf("\nfirst error: %v\n", firstErr)
	}
}

func pct(ds []time.Duration) map[string]float64 {
	out := map[string]float64{"p50": 0, "p90": 0, "p99": 0, "max": 0}
	if len(ds) == 0 {
		return out
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(p float64) float64 {
		i := int(float64(len(s)-1) * p)
		return float64(s[i].Microseconds()) / 1000
	}
	out["p50"], out["p90"], out["p99"] = at(0.50), at(0.90), at(0.99)
	out["max"] = float64(s[len(s)-1].Microseconds()) / 1000
	return out
}
