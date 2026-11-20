package sandbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"runbox/internal/ids"
)

// DockerRunnerConfig configures how containers are created.
type DockerRunnerConfig struct {
	WorkerID       string
	JobsDir        string // where this process writes job files
	HostJobsDir    string // the same directory as seen by the Docker daemon
	Runtime        string // "" for the daemon default (runc), "runsc" for gVisor
	SeccompProfile []byte // optional custom profile; nil keeps Docker's default
	TmpfsMB        int    // size of the writable /tmp, counted against the memory limit
}

// DockerRunner runs each job in a fresh, locked-down container.
type DockerRunner struct {
	cli      *DockerClient
	cfg      DockerRunnerConfig
	removals sync.WaitGroup // in-flight background container removals
}

// Close waits for background container removals to finish.
func (r *DockerRunner) Close() { r.removals.Wait() }

// NewDockerRunner returns a Runner backed by the given Docker client.
func NewDockerRunner(cli *DockerClient, cfg DockerRunnerConfig) *DockerRunner {
	if cfg.TmpfsMB <= 0 {
		cfg.TmpfsMB = 64
	}
	if cfg.HostJobsDir == "" {
		cfg.HostJobsDir = cfg.JobsDir
	}
	return &DockerRunner{cli: cli, cfg: cfg}
}

// Labels applied to every container we create, used for orphan cleanup.
const (
	labelManaged = "runbox.managed"
	labelWorker  = "runbox.worker"
	labelJob     = "runbox.job"
)

// sandboxUser is "nobody:nogroup". Running as a non-root uid means a
// container escape lands as an unprivileged user on the host.
const sandboxUser = "65534:65534"

// containerConfig builds the full isolation policy for one job. Every line
// here is a deliberate restriction; see docs/THREAT_MODEL.md.
func (r *DockerRunner) containerConfig(spec Spec, hostDir string) containerConfig {
	lim := spec.Limits
	pids := int64(lim.Pids)
	initProc := true
	tmpfsBytes := int64(r.cfg.TmpfsMB) << 20

	secOpt := []string{"no-new-privileges:true"}
	if len(r.cfg.SeccompProfile) > 0 {
		// The Engine API takes the profile JSON itself, not a path.
		secOpt = append(secOpt, "seccomp="+string(r.cfg.SeccompProfile))
	}

	env := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME=/tmp",
		"LANG=C.UTF-8",
	}
	env = append(env, spec.Language.Env...)

	return containerConfig{
		Image: spec.Language.Image,
		// Redirect stdin for the whole shell so multi-step commands
		// (compile && run) all see the job's stdin file.
		Cmd:             []string{"/bin/sh", "-c", "exec 0</workspace/stdin; " + spec.Language.Command},
		Env:             env,
		User:            sandboxUser,
		WorkingDir:      "/tmp",
		Hostname:        "sandbox",
		NetworkDisabled: true,
		AttachStdout:    true,
		AttachStderr:    true,
		Labels: map[string]string{
			labelManaged: "true",
			labelWorker:  r.cfg.WorkerID,
			labelJob:     spec.JobID,
		},
		HostConfig: hostConfig{
			Binds:          []string{hostDir + ":/workspace:ro"},
			NetworkMode:    "none",
			ReadonlyRootfs: true,
			CapDrop:        []string{"ALL"},
			SecurityOpt:    secOpt,
			Tmpfs: map[string]string{
				"/tmp": fmt.Sprintf("rw,exec,nosuid,nodev,size=%d,mode=1777", tmpfsBytes),
			},
			Memory:     int64(lim.MemoryMB) << 20,
			MemorySwap: int64(lim.MemoryMB) << 20, // equal to Memory => no swap
			NanoCpus:   int64(lim.CPUs * 1e9),
			PidsLimit:  &pids,
			Ulimits: []ulimit{
				{Name: "nofile", Soft: 256, Hard: 256},
				{Name: "fsize", Soft: tmpfsBytes, Hard: tmpfsBytes},
				{Name: "core", Soft: 0, Hard: 0},
			},
			Init:      &initProc, // tini as PID 1: reaps zombies, forwards signals
			Runtime:   r.cfg.Runtime,
			IpcMode:   "private",
			LogConfig: logConfig{Type: "none"}, // output is streamed via attach, never written to disk
		},
	}
}

type waitResult struct {
	code int
	err  error
}

type demuxResult struct {
	out *demuxOutput
	err error
}

// Run executes spec in a new container. See the Runner interface for the
// error contract.
func (r *DockerRunner) Run(ctx context.Context, spec Spec) (Result, error) {
	if !ids.ValidUUID(spec.JobID) {
		return Result{}, fmt.Errorf("invalid job id %q", spec.JobID)
	}
	lim := spec.Limits

	// 1. Stage the source and stdin on disk; mounted read-only into the container.
	dir := filepath.Join(r.cfg.JobsDir, spec.JobID)
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Result{}, fmt.Errorf("create job dir: %w", err)
	}
	defer os.RemoveAll(dir)
	if err := os.WriteFile(filepath.Join(dir, spec.Language.FileName), spec.Source, 0o644); err != nil {
		return Result{}, fmt.Errorf("write source: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stdin"), spec.Stdin, 0o644); err != nil {
		return Result{}, fmt.Errorf("write stdin: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Result{Status: StatusCanceled}, nil
	}

	// 2. Create the container. A random suffix avoids name clashes if a
	// retried job lands on a host where a previous attempt's container lingers.
	cfg := r.containerConfig(spec, filepath.Join(r.cfg.HostJobsDir, spec.JobID))
	name := "runbox-" + spec.JobID + "-" + randHex(3)
	createCtx, cancelCreate := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	id, err := r.cli.CreateContainer(createCtx, name, cfg)
	cancelCreate()
	if err != nil {
		if IsNotFound(err) {
			return Result{}, fmt.Errorf("image %q not found on worker: %w", spec.Language.Image, err)
		}
		return Result{}, fmt.Errorf("create container: %w", err)
	}
	// Removal takes a few hundred ms and the result doesn't depend on it, so
	// it happens off the critical path. If the process dies first, the
	// container is caught by CleanupOrphans on the next start.
	defer func() {
		r.removals.Add(1)
		go func() {
			defer r.removals.Done()
			rmCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = r.cli.RemoveContainer(rmCtx, id)
		}()
	}()

	// 3. Attach before start so no output is missed.
	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 10*time.Second)
	conn, br, err := r.cli.Attach(attachCtx, id)
	cancelAttach()
	if err != nil {
		return Result{}, fmt.Errorf("attach: %w", err)
	}
	defer conn.Close()

	overflow := make(chan struct{})
	outCh := make(chan demuxResult, 1)
	go func() {
		o, err := demux(br, lim.OutputBytes, func() { close(overflow) })
		outCh <- demuxResult{o, err}
	}()

	// 4. Start and race: natural exit vs timeout vs output overflow vs cancel.
	startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	err = r.cli.StartContainer(startCtx, id)
	cancelStart()
	if err != nil {
		return Result{}, fmt.Errorf("start container: %w", err)
	}
	started := time.Now()

	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	waitCh := make(chan waitResult, 1)
	go func() {
		code, err := r.cli.WaitContainer(waitCtx, id)
		waitCh <- waitResult{code, err}
	}()

	timer := time.NewTimer(lim.Timeout())
	defer timer.Stop()

	var reason Status
	var wr waitResult
	select {
	case wr = <-waitCh:
	case <-timer.C:
		reason = StatusTimedOut
	case <-overflow:
		reason = StatusOutputExceeded
	case <-ctx.Done():
		reason = StatusCanceled
	}
	if reason != "" {
		killCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		killErr := r.cli.KillContainer(killCtx, id)
		cancel()
		select {
		case wr = <-waitCh:
		case <-time.After(15 * time.Second):
			return Result{}, fmt.Errorf("container did not stop after SIGKILL (kill error: %v)", killErr)
		}
	}
	duration := time.Since(started)
	if wr.err != nil {
		return Result{}, fmt.Errorf("wait container: %w", wr.err)
	}

	// 5. Collect output. The attach stream ends when the container exits;
	// closing the connection is a backstop against a stuck stream.
	var dr demuxResult
	select {
	case dr = <-outCh:
	case <-time.After(5 * time.Second):
		conn.Close()
		dr = <-outCh
	}
	if dr.err != nil && !errors.Is(dr.err, os.ErrDeadlineExceeded) {
		// A broken stream is our problem, not the program's.
		return Result{}, fmt.Errorf("read output: %w", dr.err)
	}

	inspCtx, cancelInsp := context.WithTimeout(context.Background(), 10*time.Second)
	state, inspErr := r.cli.InspectState(inspCtx, id)
	cancelInsp()

	code := wr.code
	res := Result{
		ExitCode:        &code,
		Stdout:          dr.out.stdout.buf.Bytes(),
		Stderr:          dr.out.stderr.buf.Bytes(),
		StdoutTruncated: dr.out.stdout.truncated,
		StderrTruncated: dr.out.stderr.truncated,
		Duration:        duration,
	}
	res.Status = classify(reason, code, inspErr == nil && state.OOMKilled, res.StdoutTruncated || res.StderrTruncated)
	return res, nil
}

// classify decides the job status. Order matters: what *we* did to the
// container (cancel, timeout) takes priority over how it happened to exit.
func classify(reason Status, exitCode int, oomKilled, truncated bool) Status {
	switch {
	case reason == StatusCanceled:
		return StatusCanceled
	case reason == StatusTimedOut:
		return StatusTimedOut
	case reason == StatusOutputExceeded || truncated:
		return StatusOutputExceeded
	// With tini as PID 1, an OOM kill of a child surfaces as exit 137
	// (128+SIGKILL) even when Docker's OOMKilled flag isn't set (cgroup v2).
	// We didn't send that SIGKILL, so the kernel's OOM killer did.
	case oomKilled || exitCode == 137:
		return StatusMemoryExceeded
	case exitCode == 0:
		return StatusSucceeded
	default:
		return StatusFailed
	}
}

// CleanupOrphans removes containers left behind by a previous run of this
// worker (e.g. after a crash). Call once at startup.
func (r *DockerRunner) CleanupOrphans(ctx context.Context) (int, error) {
	idsList, err := r.cli.ListContainerIDs(ctx, map[string]string{labelManaged: "true", labelWorker: r.cfg.WorkerID})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range idsList {
		if err := r.cli.RemoveContainer(ctx, id); err == nil {
			n++
		}
	}
	return n, nil
}

// CheckImages returns the images referenced by langs that are missing locally.
func (r *DockerRunner) CheckImages(ctx context.Context, langs map[string]Language) ([]string, error) {
	seen := map[string]bool{}
	var missing []string
	for _, l := range langs {
		if seen[l.Image] {
			continue
		}
		seen[l.Image] = true
		ok, err := r.cli.ImageExists(ctx, l.Image)
		if err != nil {
			return nil, err
		}
		if !ok {
			missing = append(missing, l.Image)
		}
	}
	return missing, nil
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
