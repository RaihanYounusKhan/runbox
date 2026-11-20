package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DockerClient is a deliberately small client for the Docker Engine API,
// spoken directly over the daemon's unix socket. Implementing the handful of
// endpoints we need keeps the dependency tree tiny and makes every isolation
// setting explicit in this repo rather than hidden in an SDK.
type DockerClient struct {
	socket     string
	http       *http.Client
	apiVersion string // e.g. "v1.45", set by Negotiate
}

// maxAPIVersion is the newest API version this client was written against.
const maxAPIVersion = 1.47

// minAPIVersion is the oldest daemon API version we accept.
const minAPIVersion = 1.41

// NewDockerClient returns a client for the daemon listening on socket.
func NewDockerClient(socket string) *DockerClient {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		MaxIdleConns:    64,
		IdleConnTimeout: 30 * time.Second,
	}
	return &DockerClient{socket: socket, http: &http.Client{Transport: tr}, apiVersion: "v1.44"}
}

// Negotiate picks min(daemon API version, maxAPIVersion).
func (c *DockerClient) Negotiate(ctx context.Context) error {
	var v struct {
		APIVersion string `json:"ApiVersion"`
		Version    string `json:"Version"`
	}
	if err := c.doRaw(ctx, http.MethodGet, "/version", nil, nil, &v); err != nil {
		return fmt.Errorf("docker: %w", err)
	}
	server, err := strconv.ParseFloat(v.APIVersion, 64)
	if err != nil {
		return fmt.Errorf("docker: unparseable API version %q", v.APIVersion)
	}
	if server < minAPIVersion {
		return fmt.Errorf("docker: daemon API %s is older than required %.2f", v.APIVersion, minAPIVersion)
	}
	if server > maxAPIVersion {
		c.apiVersion = fmt.Sprintf("v%.2f", maxAPIVersion)
	} else {
		c.apiVersion = "v" + v.APIVersion
	}
	return nil
}

// APIError is a non-2xx response from the daemon.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string { return fmt.Sprintf("docker API %d: %s", e.Status, e.Message) }

// IsNotFound reports whether err is a 404 from the daemon.
func IsNotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Status == http.StatusNotFound
}

func (c *DockerClient) do(ctx context.Context, method, path string, q url.Values, body, out any) error {
	return c.doRaw(ctx, method, "/"+c.apiVersion+path, q, body, out)
}

func (c *DockerClient) doRaw(ctx context.Context, method, path string, q url.Values, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	u := "http://docker" + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		var m struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(b, &m) != nil || m.Message == "" {
			m.Message = strings.TrimSpace(string(b))
		}
		return &APIError{Status: resp.StatusCode, Message: m.Message}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// ---- Container create payload (subset of the Engine API schema) ----

type containerConfig struct {
	Image           string            `json:"Image"`
	Cmd             []string          `json:"Cmd"`
	Env             []string          `json:"Env"`
	User            string            `json:"User"`
	WorkingDir      string            `json:"WorkingDir"`
	Hostname        string            `json:"Hostname"`
	Labels          map[string]string `json:"Labels"`
	NetworkDisabled bool              `json:"NetworkDisabled"`
	AttachStdout    bool              `json:"AttachStdout"`
	AttachStderr    bool              `json:"AttachStderr"`
	AttachStdin     bool              `json:"AttachStdin"`
	OpenStdin       bool              `json:"OpenStdin"`
	Tty             bool              `json:"Tty"`
	StopSignal      string            `json:"StopSignal,omitempty"`
	HostConfig      hostConfig        `json:"HostConfig"`
}

type hostConfig struct {
	Binds          []string          `json:"Binds"`
	NetworkMode    string            `json:"NetworkMode"`
	ReadonlyRootfs bool              `json:"ReadonlyRootfs"`
	CapDrop        []string          `json:"CapDrop"`
	SecurityOpt    []string          `json:"SecurityOpt"`
	Tmpfs          map[string]string `json:"Tmpfs"`
	Memory         int64             `json:"Memory"`
	MemorySwap     int64             `json:"MemorySwap"`
	NanoCpus       int64             `json:"NanoCpus"`
	PidsLimit      *int64            `json:"PidsLimit"`
	Ulimits        []ulimit          `json:"Ulimits"`
	Init           *bool             `json:"Init,omitempty"`
	Runtime        string            `json:"Runtime,omitempty"`
	IpcMode        string            `json:"IpcMode,omitempty"`
	LogConfig      logConfig         `json:"LogConfig"`
	AutoRemove     bool              `json:"AutoRemove"`
}

type ulimit struct {
	Name string `json:"Name"`
	Soft int64  `json:"Soft"`
	Hard int64  `json:"Hard"`
}

type logConfig struct {
	Type   string            `json:"Type"`
	Config map[string]string `json:"Config,omitempty"`
}

type containerState struct {
	Running   bool   `json:"Running"`
	OOMKilled bool   `json:"OOMKilled"`
	ExitCode  int    `json:"ExitCode"`
	Error     string `json:"Error"`
}

// CreateContainer creates (but does not start) a container.
func (c *DockerClient) CreateContainer(ctx context.Context, name string, cfg containerConfig) (string, error) {
	var out struct {
		ID string `json:"Id"`
	}
	q := url.Values{"name": {name}}
	if err := c.do(ctx, http.MethodPost, "/containers/create", q, cfg, &out); err != nil {
		return "", err
	}
	return out.ID, nil
}

// StartContainer starts a created container.
func (c *DockerClient) StartContainer(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/containers/"+id+"/start", nil, nil, nil)
}

// KillContainer sends SIGKILL. A container that already exited is not an error.
func (c *DockerClient) KillContainer(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodPost, "/containers/"+id+"/kill", url.Values{"signal": {"KILL"}}, nil, nil)
	var ae *APIError
	if errors.As(err, &ae) && (ae.Status == http.StatusConflict || ae.Status == http.StatusNotFound) {
		return nil
	}
	return err
}

// RemoveContainer force-removes a container and its anonymous volumes.
func (c *DockerClient) RemoveContainer(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodDelete, "/containers/"+id, url.Values{"force": {"1"}, "v": {"1"}}, nil, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

// WaitContainer blocks until the container stops and returns its exit code.
func (c *DockerClient) WaitContainer(ctx context.Context, id string) (int, error) {
	var out struct {
		StatusCode int `json:"StatusCode"`
		Error      *struct {
			Message string `json:"Message"`
		} `json:"Error"`
	}
	if err := c.do(ctx, http.MethodPost, "/containers/"+id+"/wait", url.Values{"condition": {"not-running"}}, nil, &out); err != nil {
		return 0, err
	}
	if out.Error != nil && out.Error.Message != "" {
		return out.StatusCode, errors.New(out.Error.Message)
	}
	return out.StatusCode, nil
}

// InspectState returns the container's State block.
func (c *DockerClient) InspectState(ctx context.Context, id string) (containerState, error) {
	var out struct {
		State containerState `json:"State"`
	}
	err := c.do(ctx, http.MethodGet, "/containers/"+id+"/json", nil, nil, &out)
	return out.State, err
}

// ImageExists reports whether image is present locally.
func (c *DockerClient) ImageExists(ctx context.Context, image string) (bool, error) {
	err := c.do(ctx, http.MethodGet, "/images/"+image+"/json", nil, nil, nil)
	if IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// ListContainerIDs returns IDs of all containers (running or not) carrying
// every label in labels.
func (c *DockerClient) ListContainerIDs(ctx context.Context, labels map[string]string) ([]string, error) {
	var filt []string
	for k, v := range labels {
		filt = append(filt, k+"="+v)
	}
	f, _ := json.Marshal(map[string][]string{"label": filt})
	var out []struct {
		ID string `json:"Id"`
	}
	if err := c.do(ctx, http.MethodGet, "/containers/json", url.Values{"all": {"1"}, "filters": {string(f)}}, nil, &out); err != nil {
		return nil, err
	}
	ids := make([]string, len(out))
	for i, o := range out {
		ids[i] = o.ID
	}
	return ids, nil
}

// Attach opens a hijacked connection streaming the container's stdout and
// stderr. Call it before StartContainer so no early output is lost. The
// returned reader yields Docker's multiplexed stream format (see demux.go).
func (c *DockerClient) Attach(ctx context.Context, id string) (net.Conn, *bufio.Reader, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", c.socket)
	if err != nil {
		return nil, nil, err
	}
	u := "http://docker/" + c.apiVersion + "/containers/" + id + "/attach?stream=1&stdout=1&stderr=1"
	req, err := http.NewRequest(http.MethodPost, u, nil)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "tcp")
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols && resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		conn.Close()
		return nil, nil, &APIError{Status: resp.StatusCode, Message: strings.TrimSpace(string(b))}
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, br, nil
}
