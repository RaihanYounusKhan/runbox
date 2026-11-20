//go:build integration

// Package integration runs the real API, worker, Postgres and Docker
// together. Run with:
//
//	RUNBOX_TEST_DATABASE_URL=postgres://... go test -tags integration ./internal/integration/
//
// Each test gets its own freshly migrated database.
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"runbox/internal/api"
	"runbox/internal/audit"
	"runbox/internal/config"
	"runbox/internal/db"
	"runbox/internal/metrics"
	"runbox/internal/sandbox"
	"runbox/internal/store"
	"runbox/internal/worker"
)

const adminToken = "test-admin-token"

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var (
	baseDBURL   = envOr("RUNBOX_TEST_DATABASE_URL", "postgres://runbox:runbox@127.0.0.1:5432/runbox?sslmode=disable")
	pythonImage = envOr("RUNBOX_TEST_PYTHON_IMAGE", "runbox/python:3.13")
	dockerSock  = envOr("DOCKER_SOCKET", "/var/run/docker.sock")
)

func testLanguages() map[string]sandbox.Language {
	return map[string]sandbox.Language{
		"python": {Name: "python", Image: pythonImage, FileName: "main.py", Command: "python3 -I -B /workspace/main.py"},
		"bash":   {Name: "bash", Image: pythonImage, FileName: "main.sh", Command: "bash /workspace/main.sh"},
	}
}

// ---- test environment ----

type env struct {
	t     *testing.T
	pool  *pgxpool.Pool
	st    *store.Store
	srv   *httptest.Server
	key   string // default API key
	langs map[string]sandbox.Language
}

func newEnv(t *testing.T, mod func(*config.API)) *env {
	t.Helper()
	ctx := context.Background()

	docker := sandbox.NewDockerClient(dockerSock)
	if err := docker.Negotiate(ctx); err != nil {
		t.Skipf("docker unavailable: %v", err)
	}

	// Fresh database per test.
	admin, err := pgxpool.New(ctx, baseDBURL)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	if err := admin.Ping(ctx); err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	dbName := "runbox_it_" + hex.EncodeToString(b)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create database: %v", err)
	}
	u, _ := url.Parse(baseDBURL)
	u.Path = "/" + dbName
	pool, err := db.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP DATABASE "+dbName+" WITH (FORCE)")
		admin.Close()
	})
	if _, err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	cfg := config.API{
		AdminToken: adminToken, MaxQueueDepth: 1000, MaxQueuedPerKey: 500,
		RateLimitRPS: 1000, RateLimitBurst: 1000, MaxSourceBytes: 256 << 10, MaxStdinBytes: 1 << 20, MaxAttempts: 3,
	}
	if mod != nil {
		mod(&cfg)
	}
	e := &env{t: t, pool: pool, st: store.New(pool), langs: testLanguages()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e.srv = httptest.NewServer(api.New(cfg, e.st, e.langs, metrics.NewRegistry(), logger).Handler())
	t.Cleanup(e.srv.Close)
	e.key = e.newKey(16)
	return e
}

func (e *env) newKey(maxConcurrent int) string {
	e.t.Helper()
	code, body := e.request("POST", "/v1/admin/keys", adminToken, map[string]any{"name": "test", "max_concurrent": maxConcurrent})
	if code != http.StatusCreated {
		e.t.Fatalf("create key: %d %s", code, body)
	}
	var out struct {
		APIKey string `json:"api_key"`
	}
	_ = json.Unmarshal(body, &out)
	return out.APIKey
}

type workerOpts struct {
	concurrency int
	grace       time.Duration
	id          string
}

// startWorker runs an in-process worker; the returned func stops it and
// waits for shutdown to complete.
func (e *env) startWorker(o workerOpts) (stop func()) {
	e.t.Helper()
	if o.concurrency == 0 {
		o.concurrency = 4
	}
	if o.grace == 0 {
		o.grace = 10 * time.Second
	}
	if o.id == "" {
		o.id = "it-worker-" + e.t.Name()
	}
	jobsDir, err := os.MkdirTemp("", "runbox-jobs-")
	if err != nil {
		e.t.Fatal(err)
	}
	_ = os.Chmod(jobsDir, 0o755) // the sandbox user must be able to read job files
	docker := sandbox.NewDockerClient(dockerSock)
	if err := docker.Negotiate(context.Background()); err != nil {
		e.t.Fatal(err)
	}
	runner := sandbox.NewDockerRunner(docker, sandbox.DockerRunnerConfig{WorkerID: o.id, JobsDir: jobsDir})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("RUNBOX_TEST_VERBOSE") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	w := worker.New(worker.Config{
		WorkerID: o.id, Concurrency: o.concurrency,
		LeaseDuration: 3 * time.Second, PollInterval: 200 * time.Millisecond,
		ShutdownGrace: o.grace, ReapInterval: 500 * time.Millisecond,
	}, e.st, runner, e.langs, worker.NewMetrics(metrics.NewRegistry()), logger)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
			runner.Close()
			os.RemoveAll(jobsDir)
		})
	}
	e.t.Cleanup(stop)
	return stop
}

func (e *env) request(method, path, token string, body any) (int, []byte) {
	e.t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, rdr)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

type jobView struct {
	ID       string         `json:"id"`
	Status   sandbox.Status `json:"status"`
	Attempts int            `json:"attempts"`
	Error    *string        `json:"error"`
	Result   *struct {
		ExitCode        *int   `json:"exit_code"`
		Stdout          string `json:"stdout"`
		Stderr          string `json:"stderr"`
		StdoutTruncated bool   `json:"stdout_truncated"`
		DurationMs      *int64 `json:"duration_ms"`
	} `json:"result"`
}

func (e *env) submit(key, lang, src string, limits map[string]any) jobView {
	e.t.Helper()
	body := map[string]any{"language": lang, "source": src}
	if limits != nil {
		body["limits"] = limits
	}
	code, b := e.request("POST", "/v1/jobs", key, body)
	if code != http.StatusAccepted {
		e.t.Fatalf("submit: %d %s", code, b)
	}
	var j jobView
	_ = json.Unmarshal(b, &j)
	return j
}

func (e *env) get(key, id string) jobView {
	e.t.Helper()
	code, b := e.request("GET", "/v1/jobs/"+id, key, nil)
	if code != http.StatusOK {
		e.t.Fatalf("get: %d %s", code, b)
	}
	var j jobView
	_ = json.Unmarshal(b, &j)
	return j
}

// waitFor long-polls until the job is terminal.
func (e *env) waitFor(key, id string, max time.Duration) jobView {
	e.t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		code, b := e.request("GET", "/v1/jobs/"+id+"?wait=5s", key, nil)
		if code != http.StatusOK {
			e.t.Fatalf("get: %d %s", code, b)
		}
		var j jobView
		_ = json.Unmarshal(b, &j)
		if j.Status.Terminal() {
			return j
		}
	}
	e.t.Fatalf("job %s not finished after %s (status %s)", id, max, e.get(key, id).Status)
	return jobView{}
}

func (e *env) waitStatus(key, id string, want sandbox.Status, max time.Duration) {
	e.t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		if e.get(key, id).Status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("job %s never reached %s (now %s)", id, want, e.get(key, id).Status)
}

func (e *env) verifyAudit() audit.Report {
	e.t.Helper()
	rep, err := audit.Verify(context.Background(), e.pool)
	if err != nil {
		e.t.Fatal(err)
	}
	return rep
}

// ---- tests ----

func TestHelloWorldAndStdin(t *testing.T) {
	e := newEnv(t, nil)
	e.startWorker(workerOpts{})

	code, b := e.request("POST", "/v1/jobs", e.key, map[string]any{
		"language": "python",
		"source":   "import sys\nname = sys.stdin.read().strip()\nprint(f'hello, {name}')\nprint('warn', file=sys.stderr)\nsys.exit(3)",
		"stdin":    "runbox\n",
	})
	if code != http.StatusAccepted {
		t.Fatalf("submit: %d %s", code, b)
	}
	var j jobView
	_ = json.Unmarshal(b, &j)
	j = e.waitFor(e.key, j.ID, 30*time.Second)

	if j.Status != sandbox.StatusFailed || j.Result == nil || *j.Result.ExitCode != 3 {
		t.Fatalf("unexpected result: %+v %+v", j, j.Result)
	}
	if j.Result.Stdout != "hello, runbox\n" || j.Result.Stderr != "warn\n" {
		t.Fatalf("stdout=%q stderr=%q", j.Result.Stdout, j.Result.Stderr)
	}

	ok := e.waitFor(e.key, e.submit(e.key, "python", "print(6*7)", nil).ID, 30*time.Second)
	if ok.Status != sandbox.StatusSucceeded || ok.Result.Stdout != "42\n" {
		t.Fatalf("unexpected: %+v", ok)
	}
}

// TestAdversarial runs every program in tests/adversarial and checks each
// was contained with the expected outcome.
func TestAdversarial(t *testing.T) {
	e := newEnv(t, nil)
	e.startWorker(workerOpts{concurrency: 6})

	files, _ := filepath.Glob("../../tests/adversarial/*")
	if len(files) == 0 {
		t.Fatal("no adversarial programs found")
	}
	expectRe := regexp.MustCompile(`(?m)^# expect: (\S+)`)
	limitsRe := regexp.MustCompile(`(?m)^# limits: (\{.*\})`)

	type tc struct {
		name, id string
		want     sandbox.Status
	}
	var cases []tc
	for _, f := range files {
		src, _ := os.ReadFile(f)
		m := expectRe.FindSubmatch(src)
		if m == nil {
			t.Fatalf("%s: missing '# expect:' header", f)
		}
		var limits map[string]any
		if lm := limitsRe.FindSubmatch(src); lm != nil {
			if err := json.Unmarshal(lm[1], &limits); err != nil {
				t.Fatalf("%s: bad limits: %v", f, err)
			}
		}
		lang := "python"
		if strings.HasSuffix(f, ".sh") {
			lang = "bash"
		}
		j := e.submit(e.key, lang, string(src), limits)
		cases = append(cases, tc{filepath.Base(f), j.ID, sandbox.Status(m[1])})
	}

	start := time.Now()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := e.waitFor(e.key, c.id, 60*time.Second)
			out := ""
			if j.Result != nil {
				out = j.Result.Stdout + j.Result.Stderr
				if len(out) > 400 {
					out = out[:400] + "..."
				}
			}
			if j.Status != c.want {
				t.Fatalf("status = %s, want %s\noutput: %s\nerror: %v", j.Status, c.want, out, deref(j.Error))
			}
			line := strings.TrimSpace(firstLine(out))
			if len(line) > 70 {
				line = line[:70] + "..."
			}
			t.Logf("%-22s %-22s %s", c.name, j.Status, line)
		})
	}
	t.Logf("%d adversarial programs contained in %s", len(cases), time.Since(start).Round(time.Millisecond))

	if rep := e.verifyAudit(); !rep.OK {
		t.Fatalf("audit chain broken: %+v", rep)
	}
}

func TestConcurrentJobs(t *testing.T) {
	e := newEnv(t, nil)
	e.startWorker(workerOpts{concurrency: 8})

	const n = 24
	ids := make([]string, n)
	for i := range ids {
		ids[i] = e.submit(e.key, "python", fmt.Sprintf("import time\ntime.sleep(0.5)\nprint(%d)", i), nil).ID
	}
	start := time.Now()
	for i, id := range ids {
		j := e.waitFor(e.key, id, 60*time.Second)
		if j.Status != sandbox.StatusSucceeded || strings.TrimSpace(j.Result.Stdout) != fmt.Sprint(i) {
			t.Fatalf("job %d: %+v", i, j)
		}
	}
	elapsed := time.Since(start)
	t.Logf("%d jobs (0.5s each) on 8 slots in %s", n, elapsed.Round(time.Millisecond))
	// Serially this would take >= 12s; with 8 slots it should be far less.
	if elapsed > 10*time.Second {
		t.Errorf("jobs do not appear to run concurrently (%s)", elapsed)
	}
}

func TestCancelRunningJob(t *testing.T) {
	e := newEnv(t, nil)
	e.startWorker(workerOpts{})

	j := e.submit(e.key, "python", "import time\ntime.sleep(25)", map[string]any{"timeout_ms": 30000})
	e.waitStatus(e.key, j.ID, sandbox.StatusRunning, 20*time.Second)

	start := time.Now()
	if code, b := e.request("POST", "/v1/jobs/"+j.ID+"/cancel", e.key, nil); code != http.StatusAccepted {
		t.Fatalf("cancel: %d %s", code, b)
	}
	got := e.waitFor(e.key, j.ID, 15*time.Second)
	if got.Status != sandbox.StatusCanceled {
		t.Fatalf("status = %s, want canceled", got.Status)
	}
	t.Logf("running job canceled in %s", time.Since(start).Round(time.Millisecond))

	if code, _ := e.request("POST", "/v1/jobs/"+j.ID+"/cancel", e.key, nil); code != http.StatusConflict {
		t.Fatalf("second cancel: got %d, want 409", code)
	}
}

func TestPerKeyConcurrencyAndQueuedCancel(t *testing.T) {
	e := newEnv(t, nil)
	e.startWorker(workerOpts{concurrency: 4})
	key := e.newKey(1) // this key may only run one job at a time

	first := e.submit(key, "python", "import time\ntime.sleep(20)", map[string]any{"timeout_ms": 30000})
	e.waitStatus(key, first.ID, sandbox.StatusRunning, 20*time.Second)
	second := e.submit(key, "python", "print('should never run')", nil)

	// Free slots exist, but the key is at its cap, so the job must stay queued.
	time.Sleep(1500 * time.Millisecond)
	if st := e.get(key, second.ID).Status; st != sandbox.StatusQueued {
		t.Fatalf("second job status = %s, want queued (per-key cap ignored)", st)
	}
	// Other keys are unaffected.
	other := e.waitFor(e.key, e.submit(e.key, "python", "print(1)", nil).ID, 30*time.Second)
	if other.Status != sandbox.StatusSucceeded {
		t.Fatalf("other key's job: %s", other.Status)
	}

	if code, b := e.request("POST", "/v1/jobs/"+second.ID+"/cancel", key, nil); code != http.StatusAccepted {
		t.Fatalf("cancel queued: %d %s", code, b)
	}
	if st := e.get(key, second.ID).Status; st != sandbox.StatusCanceled {
		t.Fatalf("queued job status after cancel = %s", st)
	}
	e.request("POST", "/v1/jobs/"+first.ID+"/cancel", key, nil)
	e.waitFor(key, first.ID, 15*time.Second)
}

func TestTenantIsolationAndAuth(t *testing.T) {
	e := newEnv(t, nil)
	other := e.newKey(4)
	j := e.submit(e.key, "python", "print('secret')", nil)

	if code, _ := e.request("GET", "/v1/jobs/"+j.ID, other, nil); code != http.StatusNotFound {
		t.Fatalf("other tenant reading job: got %d, want 404", code)
	}
	if code, _ := e.request("POST", "/v1/jobs/"+j.ID+"/cancel", other, nil); code != http.StatusNotFound {
		t.Fatalf("other tenant canceling job: got %d, want 404", code)
	}
	if code, _ := e.request("GET", "/v1/jobs/"+j.ID, "", nil); code != http.StatusUnauthorized {
		t.Fatalf("no key: got %d, want 401", code)
	}
	if code, _ := e.request("GET", "/v1/jobs/"+j.ID, "rbx_notarealkey", nil); code != http.StatusUnauthorized {
		t.Fatalf("bad key: got %d, want 401", code)
	}
	if code, _ := e.request("GET", "/v1/admin/keys", e.key, nil); code != http.StatusUnauthorized {
		t.Fatalf("API key on admin endpoint: got %d, want 401", code)
	}

	// Revoked keys stop working immediately.
	var keys struct {
		Keys []store.Key `json:"keys"`
	}
	_, b := e.request("GET", "/v1/admin/keys", adminToken, nil)
	_ = json.Unmarshal(b, &keys)
	for _, k := range keys.Keys {
		if strings.HasPrefix(other, k.Prefix) {
			if code, b := e.request("POST", "/v1/admin/keys/"+k.ID+"/revoke", adminToken, nil); code != 200 {
				t.Fatalf("revoke: %d %s", code, b)
			}
		}
	}
	if code, _ := e.request("GET", "/v1/languages", other, nil); code != http.StatusUnauthorized {
		t.Fatalf("revoked key: got %d, want 401", code)
	}
}

func TestValidation(t *testing.T) {
	e := newEnv(t, nil)
	for name, body := range map[string]map[string]any{
		"unknown language": {"language": "cobol", "source": "x"},
		"empty source":     {"language": "python", "source": ""},
		"limit too high":   {"language": "python", "source": "x", "limits": map[string]any{"memory_mb": 100000}},
		"negative limit":   {"language": "python", "source": "x", "limits": map[string]any{"timeout_ms": -5}},
		"unknown field":    {"language": "python", "source": "x", "evil": true},
	} {
		if code, b := e.request("POST", "/v1/jobs", e.key, body); code != http.StatusBadRequest {
			t.Errorf("%s: got %d %s, want 400", name, code, b)
		}
	}
	big := strings.Repeat("x", 300<<10)
	if code, _ := e.request("POST", "/v1/jobs", e.key, map[string]any{"language": "python", "source": big}); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized source: got %d, want 413", code)
	}
}

func TestBackpressureAndRateLimit(t *testing.T) {
	e := newEnv(t, func(c *config.API) { c.MaxQueuedPerKey = 2 })
	// No worker: jobs stay queued.
	e.submit(e.key, "python", "print(1)", nil)
	e.submit(e.key, "python", "print(2)", nil)
	if code, _ := e.request("POST", "/v1/jobs", e.key, map[string]any{"language": "python", "source": "print(3)"}); code != http.StatusTooManyRequests {
		t.Fatalf("third queued job: got %d, want 429", code)
	}

	e2 := newEnv(t, func(c *config.API) { c.RateLimitRPS = 1; c.RateLimitBurst = 3 })
	codes := []int{}
	for i := 0; i < 5; i++ {
		code, _ := e2.request("GET", "/v1/languages", e2.key, nil)
		codes = append(codes, code)
	}
	if codes[2] != 200 || codes[3] != http.StatusTooManyRequests {
		t.Fatalf("rate limit codes = %v, want 3x200 then 429", codes)
	}
}

// TestCrashedWorkerLeaseExpires simulates a worker that claims a job and
// dies: the lease expires, the reaper requeues it, and a healthy worker
// finishes it on the second attempt.
func TestCrashedWorkerLeaseExpires(t *testing.T) {
	e := newEnv(t, nil)
	j := e.submit(e.key, "python", "print('recovered')", nil)

	ghost, err := e.st.ClaimJob(context.Background(), "ghost-worker", time.Second)
	if err != nil || ghost == nil || ghost.ID != j.ID {
		t.Fatalf("ghost claim: %v %+v", err, ghost)
	}
	time.Sleep(1500 * time.Millisecond) // lease expires; ghost never heartbeats

	e.startWorker(workerOpts{})
	got := e.waitFor(e.key, j.ID, 30*time.Second)
	if got.Status != sandbox.StatusSucceeded || got.Attempts != 2 || got.Result.Stdout != "recovered\n" {
		t.Fatalf("unexpected: status=%s attempts=%d %+v", got.Status, got.Attempts, got.Result)
	}

	// A late write from the ghost must be rejected by the lease fence.
	ok, err := e.st.CompleteJob(context.Background(), j.ID, "ghost-worker", sandbox.Result{Status: sandbox.StatusFailed}, "")
	if err != nil || ok {
		t.Fatalf("stale worker overwrote result: ok=%v err=%v", ok, err)
	}

	entries, _ := audit.List(context.Background(), e.pool, audit.ListFilter{JobID: j.ID})
	var actions []string
	for _, en := range entries {
		actions = append(actions, en.Action)
	}
	want := []string{audit.ActionJobSubmitted, audit.ActionJobStarted, audit.ActionJobRequeued, audit.ActionJobStarted, audit.ActionJobFinished}
	if strings.Join(actions, ",") != strings.Join(want, ",") {
		t.Fatalf("audit trail = %v, want %v", actions, want)
	}
}

// TestGracefulShutdownReleasesJobs: a worker stopped mid-job past its grace
// period kills the container and hands the job back, without spending an attempt.
func TestGracefulShutdownReleasesJobs(t *testing.T) {
	e := newEnv(t, nil)
	stop := e.startWorker(workerOpts{grace: 500 * time.Millisecond, id: "shutdown-worker"})
	j := e.submit(e.key, "python", "import time\ntime.sleep(20)", map[string]any{"timeout_ms": 30000})
	e.waitStatus(e.key, j.ID, sandbox.StatusRunning, 20*time.Second)

	start := time.Now()
	stop()
	t.Logf("worker stopped in %s", time.Since(start).Round(time.Millisecond))
	got := e.get(e.key, j.ID)
	if got.Status != sandbox.StatusQueued || got.Attempts != 0 {
		t.Fatalf("after shutdown: status=%s attempts=%d, want queued/0", got.Status, got.Attempts)
	}
}

func TestAuditChainDetectsTampering(t *testing.T) {
	e := newEnv(t, nil)
	e.startWorker(workerOpts{})
	for i := 0; i < 3; i++ {
		e.waitFor(e.key, e.submit(e.key, "python", fmt.Sprintf("print(%d)", i), nil).ID, 30*time.Second)
	}
	rep := e.verifyAudit()
	if !rep.OK || rep.Entries < 10 {
		t.Fatalf("clean chain: %+v", rep)
	}

	ctx := context.Background()
	// Layer 1: the database refuses edits.
	if _, err := e.pool.Exec(ctx, `UPDATE audit_log SET actor = 'x' WHERE seq = 3`); err == nil {
		t.Fatal("UPDATE on audit_log was allowed")
	}
	if _, err := e.pool.Exec(ctx, `DELETE FROM audit_log WHERE seq = 3`); err == nil {
		t.Fatal("DELETE on audit_log was allowed")
	}

	// Layer 2: an attacker with superuser disables the trigger and edits a
	// row. The hash chain catches it.
	if _, err := e.pool.Exec(ctx, `ALTER TABLE audit_log DISABLE TRIGGER audit_log_no_update_delete`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE audit_log SET details = '{"rewritten":true}' WHERE seq = 5`); err != nil {
		t.Fatal(err)
	}
	rep = e.verifyAudit()
	if rep.OK || rep.FirstBadSeq != 5 {
		t.Fatalf("tampering not detected: %+v", rep)
	}
	t.Logf("tampering detected: seq %d: %s", rep.FirstBadSeq, rep.Problem)

	// The API reports it too.
	if code, _ := e.request("GET", "/v1/admin/audit/verify", adminToken, nil); code != http.StatusConflict {
		t.Fatalf("verify endpoint: got %d, want 409", code)
	}
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
