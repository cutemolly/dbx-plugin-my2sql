package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
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
	j := &job{id: id, opts: opts, conn: connRefOf(spec), status: StatusRunning, created: time.Now(), outDir: outDir, cancel: cancel, done: make(chan struct{})}

	m.mu.Lock()
	m.jobs[id] = j
	m.mu.Unlock()

	// Persist the parameters immediately: a job that dies with the process still
	// has to show up in the history view with its window and filters intact.
	j.mu.Lock()
	j.writeMetaLocked()
	j.mu.Unlock()

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

	result, err := m.runEngine(ctx, j, spec, outDir, progress)
	elapsed := time.Since(started).Milliseconds()
	defer close(j.done)

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.result != nil {
		// The stop watchdog already published a terminal result; the engine only
		// landed now. It knows what was actually parsed, so fold that into the
		// summary and refresh job.json — otherwise a stopped job keeps showing an
		// empty report even though the SQL is on disk.
		existing := *j.result
		if result.JobID == "" {
			result.JobID = j.id
		}
		if result.OutDir == "" {
			result.OutDir = filepath.Base(outDir)
		}
		if len(result.Files) == 0 {
			result.Files = outputFilesOf(outDir)
		}
		if result.Stats == nil {
			result.Stats = existing.Stats
		}
		if result.Error == "" {
			result.Error = existing.Error
		}
		if result.ElapsedMs == 0 {
			result.ElapsedMs = elapsed
		}
		// The watchdog already told the UI this job was stopped; don't upgrade it
		// to done just because the engine eventually unwound cleanly.
		result.Status = StatusStopped
		// A second event is only worth sending when the engine really knows more
		// (stats, or files the watchdog had not seen); otherwise the UI would
		// re-render an identical report.
		improved := len(result.Stats) > len(existing.Stats) || len(result.Files) > len(existing.Files)
		j.result = &result
		j.writeMetaLocked()
		if improved && publish != nil {
			publish("my2sql/job/done", j.result)
		}
		return
	}
	if err != nil {
		status := StatusFailed
		if ctx.Err() == context.Canceled {
			status = StatusStopped
		}
		// Keep whatever the engine already flushed. It writes SQL as it parses, so a
		// manual stop really has output; rebuilding the result from scratch here is
		// what made a stopped job show nothing at all.
		if result.JobID == "" {
			result.JobID = j.id
		}
		if result.OutDir == "" {
			result.OutDir = filepath.Base(outDir)
		}
		if len(result.Files) == 0 {
			result.Files = outputFilesOf(outDir)
		}
		result.Status = status
		result.Error = err.Error()
		if result.ElapsedMs == 0 {
			result.ElapsedMs = elapsed
		}
		j.status = status
		j.result = &result
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
	// The summary is written before the event: anything that learns the job
	// finished must also be able to read the finished summary.
	j.writeMetaLocked()
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

// runEngine isolates the parser goroutine from the process. A panic inside the
// binlog kernel would otherwise kill the whole sidecar: the host restarts it
// with backoff and every running job, open session and unconsumed confirm token
// is lost, which the workbench can only observe as "unknown job". Turning it
// into this job's terminal error keeps the other tabs and jobs alive, and the
// stack still reaches the host log via stderr.
func (m *JobManager) runEngine(ctx context.Context, j *job, spec *ConnectionSpec, outDir string, progress func(ProgressEvent)) (result JobResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			debug.PrintStack()
			log.Printf("panic inside the analysis engine: %v", rec)
			result = JobResult{}
			// Never the panic value itself: a kernel panic can carry SQL text or
			// connection data, and this string is stored in job.json and shown in the
			// workbench. The detail is in the plugin log.
			err = errors.New("engine panic (details in the plugin log)")
		}
	}()
	return m.engine.Run(ctx, spec, j.opts, outDir, progress)
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
	// Even on this path the engine may have flushed SQL before wedging; a result
	// without the file list is what made a manual stop look like it produced
	// nothing. Stats stay unknown here — only the engine can supply them.
	res := JobResult{
		JobID:     j.id,
		Status:    StatusStopped,
		Error:     "stopped (engine did not unwind within grace period)",
		OutDir:    filepath.Base(j.outDir),
		ElapsedMs: time.Since(j.created).Milliseconds(),
	}
	res.Files = outputFilesOf(j.outDir)
	j.result = &res
	j.writeMetaLocked()
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

// jobOutputPath validates an id and returns the directory it maps to without
// creating anything. Read paths (listing, preview, delete) must use this: a
// job that was just deleted would otherwise be resurrected as an empty ghost
// directory by the very next listing, and reappear in the history view.
func jobOutputPath(id string) (string, error) {
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
	return dir, nil
}

// jobOutputDir is the write-side resolver: same validation, plus the directory
// is created because an engine is about to write into it.
func jobOutputDir(id string) (string, error) {
	dir, err := jobOutputPath(id)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return dir, nil
}

// jobMetaName is the summary file written into each job directory. The
// in-memory job map disappears with the process, so this file is what the
// workbench history view reads to show past analyses.
const jobMetaName = "job.json"

// jobMetaTmpName is the staging file used for the atomic rewrite above. It is
// excluded from output listings so a crash mid-write cannot leave a phantom
// "generated file".
const jobMetaTmpName = jobMetaName + ".tmp"

// jobMeta is the on-disk record of one analysis. It carries analysis parameters,
// the connection identity and the outcome — credentials live in ConnectionSpec
// and are never written out.
type jobMeta struct {
	JobID      string           `json:"jobId"`
	CreatedAt  string           `json:"createdAt"`
	FinishedAt string           `json:"finishedAt,omitempty"`
	Status     JobStatus        `json:"status"`
	Error      string           `json:"error,omitempty"`
	ElapsedMs  int64            `json:"elapsedMs,omitempty"`
	Stats      map[string]any   `json:"stats,omitempty"`
	Options    AnalysisOptions  `json:"options"`
	Connection JobConnectionRef `json:"connection"`
}

// connRefOf copies the connection identity a job ran against, never its
// credentials.
func connRefOf(spec *ConnectionSpec) JobConnectionRef {
	if spec == nil {
		return JobConnectionRef{}
	}
	return JobConnectionRef{ID: spec.ID, Name: spec.Name, Host: spec.Host, Port: spec.Port}
}

// maxMetaStatsBytes bounds the per-table breakdown kept on disk: a wide-schema
// analysis can touch thousands of tables and the history list only needs the
// summary numbers.
const maxMetaStatsBytes = 32 << 10

// writeMetaLocked persists the job summary. Callers must hold j.mu.
func (j *job) writeMetaLocked() {
	if j.outDir == "" {
		return
	}
	meta := jobMeta{
		JobID:      j.id,
		CreatedAt:  j.created.UTC().Format(time.RFC3339),
		Status:     j.status,
		Options:    j.opts,
		Connection: j.conn,
	}
	if j.result != nil {
		meta.FinishedAt = time.Now().UTC().Format(time.RFC3339)
		meta.Status = j.result.Status
		meta.Error = j.result.Error
		meta.ElapsedMs = j.result.ElapsedMs
		if b, err := json.Marshal(j.result.Stats); err == nil && len(b) <= maxMetaStatsBytes {
			meta.Stats = j.result.Stats
		}
	}
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return
	}
	// 0600: the file names databases, tables and the analysed window. Written via
	// a temp file and renamed, because os.WriteFile truncates first and a history
	// read racing a rewrite would otherwise see a half-written (unparseable) file.
	tmp := filepath.Join(j.outDir, jobMetaTmpName)
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, filepath.Join(j.outDir, jobMetaName))
}

// HistoryEntry is one row of the workbench history list.
type HistoryEntry struct {
	JobID      string           `json:"jobId"`
	CreatedAt  string           `json:"createdAt"`
	FinishedAt string           `json:"finishedAt,omitempty"`
	Status     JobStatus        `json:"status"`
	Error      string           `json:"error,omitempty"`
	ElapsedMs  int64            `json:"elapsedMs,omitempty"`
	Files      int              `json:"files"`
	Bytes      int64            `json:"bytes"`
	Stats      map[string]any   `json:"stats,omitempty"`
	Options    AnalysisOptions  `json:"options"`
	Connection JobConnectionRef `json:"connection"`
}

// historyDefaultRows and historyMaxRows bound one listing. The jobs directory
// grows until the (documented, deferred) output cleanup lands, so the list must
// not scale with it.
const (
	historyDefaultRows = 50
	historyMaxRows     = 200
)

// History lists analyses that left output on disk, newest first. It reads the
// filesystem instead of the job map on purpose: the map is gone after the host
// restarts the sidecar, which is exactly when a history view matters most.
func (m *JobManager) History(limit int) ([]HistoryEntry, error) {
	base := os.Getenv("DBX_PLUGIN_DATA_DIR")
	if base == "" {
		return nil, errors.New("plugin data directory is not set; running outside a DBX host")
	}
	root := filepath.Join(base, "jobs")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return []HistoryEntry{}, nil
		}
		return nil, err
	}
	if limit <= 0 {
		limit = historyDefaultRows
	}
	if limit > historyMaxRows {
		limit = historyMaxRows
	}
	out := make([]HistoryEntry, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() || !validJobID.MatchString(e.Name()) {
			continue // a directory name must never be trusted as a path element
		}
		id := e.Name()
		dir := filepath.Join(root, id)
		if filepath.Dir(dir) != root {
			continue
		}
		he := HistoryEntry{JobID: id, Status: StatusDone}
		if info, err := e.Info(); err == nil {
			he.CreatedAt = info.ModTime().UTC().Format(time.RFC3339)
		}
		// The id is itself a UTC timestamp; prefer it over the directory mtime,
		// which moves when output is written.
		if t, err := time.Parse("20060102T150405", strings.SplitN(id, "-", 2)[0]); err == nil {
			he.CreatedAt = t.UTC().Format(time.RFC3339)
		}
		if b, err := os.ReadFile(filepath.Join(dir, jobMetaName)); err == nil {
			var meta jobMeta
			if json.Unmarshal(b, &meta) == nil {
				he.Options = meta.Options
				he.Connection = meta.Connection
				he.Status = meta.Status
				he.Error = meta.Error
				he.ElapsedMs = meta.ElapsedMs
				he.Stats = meta.Stats
				he.FinishedAt = meta.FinishedAt
				if meta.CreatedAt != "" {
					he.CreatedAt = meta.CreatedAt
				}
			}
		}
		files, bytes, err := countOutputs(dir)
		if err != nil {
			continue
		}
		he.Files, he.Bytes = files, bytes
		// A job still running in this process must read as running even though its
		// on-disk summary was written at start time.
		m.mu.RLock()
		if live, ok := m.jobs[id]; ok {
			live.mu.Lock()
			he.Status = live.status
			live.mu.Unlock()
		}
		m.mu.RUnlock()
		out = append(out, he)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func countOutputs(dir string) (int, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, err
	}
	var n int
	var total int64
	for _, e := range entries {
		if e.IsDir() || e.Name() == jobMetaName || e.Name() == jobMetaTmpName {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		n++
		total += info.Size()
	}
	return n, total, nil
}

// outputFilesOf enumerates the generated files of a job straight from disk. Used
// when the engine could not report them itself (it errored or panicked) but did
// write output before that.
func outputFilesOf(dir string) []OutputFile {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []OutputFile
	for _, e := range entries {
		if e.IsDir() || e.Name() == jobMetaName || e.Name() == jobMetaTmpName {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, OutputFile{Name: e.Name(), Path: filepath.Join(dir, e.Name()), Size: info.Size(), Kind: classifyOutput(e.Name())})
	}
	return out
}

// Delete removes one analysis and everything it produced. The id goes through
// jobOutputDir, so a stale or hostile value can never turn into a path
// traversal outside jobs/. A running job is refused rather than cancelled: its
// engine would keep writing into a directory that no longer exists, and the
// caller can stop it first and delete it once it has landed.
func (m *JobManager) Delete(id string) error {
	dir, err := jobOutputPath(id)
	if err != nil {
		return err
	}
	m.mu.RLock()
	j, live := m.jobs[id]
	m.mu.RUnlock()
	if live {
		j.mu.Lock()
		running := j.status == StatusRunning
		j.mu.Unlock()
		if running {
			return fmt.Errorf("job %s is still running; stop it before deleting", id)
		}
		m.mu.Lock()
		delete(m.jobs, id)
		m.mu.Unlock()
	}
	return os.RemoveAll(dir)
}

// ListOutputs enumerates generated files for a completed job.
func (m *JobManager) ListOutputs(id string) ([]map[string]any, error) {
	dir, err := jobOutputPath(id)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, e := range entries {
		if e.IsDir() || e.Name() == jobMetaName || e.Name() == jobMetaTmpName {
			continue // the summary is history metadata, not generated SQL
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
	dir, err := jobOutputPath(id)
	if err != nil {
		return nil, err
	}
	// Reject traversal: name must be a base name of an existing entry.
	clean := filepath.Base(name)
	if clean != name || clean == jobMetaName || clean == jobMetaTmpName {
		return nil, fmt.Errorf("invalid output name")
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	lines, total, complete, err := scanLineWindow(filepath.Join(dir, clean), offset, limit)
	if err != nil {
		return nil, err
	}
	if lines == nil {
		lines = []string{}
	}
	// total is only exact when the scan reached the end of the file; a window
	// taken from a huge file reports -1 ("more lines follow, uncounted") rather
	// than pretending.
	if !complete {
		total = -1
	}
	return map[string]any{
		"name":      clean,
		"total":     total,
		"offset":    offset,
		"lines":     lines,
		"truncated": !complete || offset+len(lines) < total,
	}, nil
}

// maxPreviewLineBytes caps one displayed line. The preview is a display path, so
// a corrupt file that holds gigabytes on a single line must not be able to make
// the sidecar allocate it.
const maxPreviewLineBytes = 64 << 10

// scanLineWindow returns up to limit lines starting at offset, without reading
// the whole file. Analysis output routinely reaches the gigabytes and the
// os.ReadFile behind this call is what pushed the sidecar into a fatal
// allocation while an analysis was still running. complete is false when the
// window was filled before the end of the file, so more lines exist.
func scanLineWindow(path string, offset, limit int) (lines []string, total int, complete bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, false, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 256<<10)
	idx := 0
	for {
		line, clipped, found, rerr := nextLine(br, maxPreviewLineBytes)
		if found {
			total = idx + 1
			if clipped {
				line += "…"
			}
			if idx >= offset {
				lines = append(lines, line)
				if len(lines) >= limit {
					// One more read purely to answer "is there anything after this
					// window", so a file that ends exactly at the window is not
					// reported as truncated.
					_, _, more, _ := nextLine(br, 1)
					return lines, total, !more, nil
				}
			}
			idx++
		}
		if rerr != nil {
			return nil, 0, false, rerr
		}
		if !found {
			return lines, total, true, nil
		}
	}
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
