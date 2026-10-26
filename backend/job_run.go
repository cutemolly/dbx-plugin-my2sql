package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// EventPublisher decouples the job/exec managers from the concrete SDK emitter.
type EventPublisher func(method string, params any)

func (m *JobManager) Start(opts AnalysisOptions, spec *ConnectionSpec, publish EventPublisher) (string, error) {
	id := m.nextID()
	outDir, err := jobOutputDir(id)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(context.Background())
	j := &job{id: id, opts: opts, status: StatusRunning, created: time.Now(), cancel: cancel, done: make(chan struct{})}

	m.mu.Lock()
	m.jobs[id] = j
	m.mu.Unlock()

	go m.run(ctx, j, spec, outDir, publish)
	return id, nil
}

func (m *JobManager) run(ctx context.Context, j *job, spec *ConnectionSpec, outDir string, publish EventPublisher) {
	started := time.Now()
	withID := func(ev ProgressEvent) ProgressEvent { ev.JobID = j.id; return ev }
	progress := func(ev ProgressEvent) {
		if publish != nil {
			publish("my2sql/job/progress", withID(ev))
		}
	}

	progress(ProgressEvent{Stage: "starting", ParsedEvents: 0, Rows: 0, SQLFiles: 0})

	result, err := m.engine.Run(ctx, spec, j.opts, outDir, progress)
	elapsed := time.Since(started).Milliseconds()
	defer close(j.done)

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.result != nil {
		// The stop watchdog already published a terminal result; the engine
		// goroutine only landed later. Do not emit a second done event.
		return
	}
	if err != nil {
		status := StatusFailed
		if ctx.Err() == context.Canceled {
			status = StatusStopped
		}
		j.status = status
		j.result = &JobResult{JobID: j.id, Status: status, OutDir: filepath.Base(outDir), Error: err.Error(), ElapsedMs: elapsed}
	} else {
		result.JobID = j.id
		result.Status = j.statusIfNotStopped()
		result.ElapsedMs = elapsed
		if result.OutDir == "" {
			result.OutDir = filepath.Base(outDir)
		}
		j.status = result.Status
		j.result = &result
	}
	if publish != nil {
		publish("my2sql/job/done", j.result)
	}
}

func (j *job) statusIfNotStopped() JobStatus {
	if j.status == StatusStopped {
		return StatusStopped
	}
	return StatusDone
}

// stopGrace is how long a cancelled engine may take to unwind before the
// watchdog declares the job stopped. Cancel reaches the stream loop via
// context, but a driver call or a wedged socket read can lag behind; the UI
// must not sit in "stopping" forever because of that.
const stopGrace = 3 * time.Second

// forceStopped marks a still-running job stopped and returns its terminal
// result, but only if run() has not already finalized it. The caller publishes
// the done event outside the lock.
func (j *job) forceStopped() (JobResult, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.result != nil {
		return JobResult{}, false
	}
	j.status = StatusStopped
	j.result = &JobResult{JobID: j.id, Status: StatusStopped, Error: "stopped (engine did not unwind within grace period)"}
	return *j.result, true
}

func (m *JobManager) Stop(id string, publish EventPublisher) error {
	m.mu.RLock()
	j, ok := m.jobs[id]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown job: %s", id)
	}
	j.mu.Lock()
	if j.status == StatusRunning {
		j.status = StatusStopped
	}
	j.mu.Unlock()
	if j.cancel != nil {
		j.cancel()
	}
	// Watchdog: if the engine goroutine never lands, terminate the job for it
	// so the UI always receives a final my2sql/job/done.
	go func() {
		timer := time.NewTimer(stopGrace)
		defer timer.Stop()
		select {
		case <-j.done:
			return
		case <-timer.C:
		}
		if res, changed := j.forceStopped(); changed && publish != nil {
			publish("my2sql/job/done", res)
		}
	}()
	return nil
}

func (m *JobManager) Snapshot(id string) (map[string]any, error) {
	m.mu.RLock()
	j, ok := m.jobs[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown job: %s", id)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	out := map[string]any{"jobId": j.id, "status": j.status, "createdAt": j.created.UTC().Format(time.RFC3339)}
	if j.result != nil {
		out["result"] = j.result
	}
	return out, nil
}

// validJobID mirrors exactly what nextID() generates (UTC timestamp + serial).
// The job id doubles as a directory name under <data>/jobs/, so anything with
// dots or path separators must never reach the filesystem layer — a hostile
// UI or script must not be able to traverse out of jobs/ with ".." segments.
var validJobID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// jobOutputDir creates DBX_PLUGIN_DATA_DIR/jobs/<id>. Bulk output stays on the
// sidecar filesystem; the UI only ever sees file names and bounded previews.
// The environment variable is the host contract itself; reading it directly
// keeps the plugin buildable against any published SDK version.
func jobOutputDir(id string) (string, error) {
	base := os.Getenv("DBX_PLUGIN_DATA_DIR")
	if base == "" {
		return "", errors.New("plugin data directory is not set; running outside a DBX host")
	}
	if !validJobID.MatchString(id) {
		return "", fmt.Errorf("invalid jobId: must match [A-Za-z0-9_-]{1,64}")
	}
	root := filepath.Join(base, "jobs")
	dir := filepath.Join(root, id)
	// Belt and braces: the id must land as a direct child of jobs/ even if the
	// pattern above is ever widened.
	if filepath.Dir(dir) != root || id == "." || id == ".." {
		return "", fmt.Errorf("invalid jobId")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return dir, nil
}

// ListOutputs enumerates generated files for a completed job.
func (m *JobManager) ListOutputs(id string) ([]map[string]any, error) {
	dir, err := jobOutputDir(id)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, map[string]any{
			"name": e.Name(),
			"path": filepath.Join(dir, e.Name()),
			"size": info.Size(),
			"kind": classifyOutput(e.Name()),
		})
	}
	return out, nil
}

// Preview reads a bounded slice of one output file (line-based) for the UI.
func (m *JobManager) Preview(id, name string, offset, limit int) (map[string]any, error) {
	dir, err := jobOutputDir(id)
	if err != nil {
		return nil, err
	}
	// Reject traversal: name must be a base name of an existing entry.
	clean := filepath.Base(name)
	if clean != name {
		return nil, fmt.Errorf("invalid output name")
	}
	data, err := os.ReadFile(filepath.Join(dir, clean))
	if err != nil {
		return nil, err
	}
	lines := splitLines(string(data))
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	end := offset + limit
	if end > len(lines) {
		end = len(lines)
	}
	shown := []string{}
	if offset < len(lines) {
		shown = lines[offset:end]
	}
	return map[string]any{
		"name":      clean,
		"total":     len(lines),
		"offset":    offset,
		"lines":     shown,
		"truncated": end < len(lines),
	}, nil
}

func classifyOutput(name string) string {
	switch {
	case filepath.Ext(name) != ".sql":
		return "stats"
	case contains(name, "rollback"):
		return "rollback"
	default:
		return "sql"
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}

func splitLines(s string) []string {
	out := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// decodeOptions is a small helper shared by job/start to keep main.go readable.
func decodeOptions(params json.RawMessage) (AnalysisOptions, error) {
	var opts AnalysisOptions
	if len(params) == 0 {
		return opts, fmt.Errorf("missing params")
	}
	if err := json.Unmarshal(params, &opts); err != nil {
		return opts, err
	}
	if opts.Threads <= 0 {
		opts.Threads = 8
	}
	if opts.Mode == "" {
		opts.Mode = "repl"
	}
	if opts.WorkType == "" {
		opts.WorkType = "rollback"
	}
	return opts, nil
}
