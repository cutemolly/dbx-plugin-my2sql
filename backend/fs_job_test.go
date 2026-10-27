package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- stop watchdog -----------------------------------------------------------

type blockingEngine struct{ release chan struct{} }

func (e *blockingEngine) Run(ctx context.Context, spec *ConnectionSpec, opts AnalysisOptions, outDir string, progress func(ProgressEvent)) (JobResult, error) {
	<-e.release
	return JobResult{Status: StatusDone}, nil
}

func (e *blockingEngine) waitDone(d time.Duration) bool {
	select {
	case <-e.release:
		return true
	case <-time.After(d):
		return false
	}
}

// Regression for "online job cannot be stopped": Stop cancels the engine and a
// watchdog must publish a terminal job/done even if the engine goroutine never
// unwinds (wedged proxy socket read etc.).
func TestStopWatchdogForceTerminates(t *testing.T) {
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	eng := &blockingEngine{release: make(chan struct{})}
	m := NewJobManager(eng)
	// The publisher runs on the watchdog goroutine, so the collected events need
	// their own lock for -race.
	var evMu sync.Mutex
	var events []json.RawMessage
	pub := func(method string, params any) {
		if method != "my2sql/job/done" {
			return
		}
		b, _ := json.Marshal(params)
		evMu.Lock()
		events = append(events, b)
		evMu.Unlock()
	}
	collected := func() []json.RawMessage {
		evMu.Lock()
		defer evMu.Unlock()
		return append([]json.RawMessage(nil), events...)
	}
	id, err := m.Start(AnalysisOptions{Mode: "repl"}, &ConnectionSpec{Host: "127.0.0.1", Port: 3306}, pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(id, pub); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(stopGrace + 2*time.Second)
	for time.Now().Before(deadline) {
		if len(collected()) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := collected()
	if len(got) == 0 {
		t.Fatalf("watchdog never published job/done (grace=%s)", stopGrace)
	}
	var res JobResult
	if err := json.Unmarshal(got[0], &res); err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusStopped {
		t.Fatalf("status=%s want stopped", res.Status)
	}
	// The engine must NOT have been waited for; it lands later silently.
	close(eng.release)
	if !eng.waitDone(time.Second) {
		t.Fatal("release channel not consumed")
	}
	time.Sleep(100 * time.Millisecond)
	if len(collected()) != 1 {
		t.Fatalf("late engine landing emitted extra events: %d", len(collected()))
	}
}

// --- a stopped job must still show its output --------------------------------

// stopThenWriteEngine behaves like the real engine on a manual stop: it flushes
// SQL as it parses, then unwinds with the context error nested inside its own
// message once Stop cancels it.
type stopThenWriteEngine struct{}

func (stopThenWriteEngine) Run(ctx context.Context, spec *ConnectionSpec, opts AnalysisOptions, outDir string, progress func(ProgressEvent)) (JobResult, error) {
	if err := os.WriteFile(filepath.Join(outDir, "my2sql.rollback.sql"), []byte("-- rollback\n"), 0o600); err != nil {
		return JobResult{}, err
	}
	<-ctx.Done()
	return JobResult{Stats: map[string]any{"totalRows": 2}}, fmt.Errorf("read binlog stream: %w", ctx.Err())
}

func newDoneChannel() (chan JobResult, EventPublisher) {
	ch := make(chan JobResult, 4)
	return ch, func(method string, params any) {
		if method != "my2sql/job/done" {
			return
		}
		b, _ := json.Marshal(params)
		var r JobResult
		if json.Unmarshal(b, &r) == nil {
			ch <- r
		}
	}
}

// Regression for "手动停止后结果没展示": the failure branch rebuilt the terminal
// result from scratch, so everything the stop had already produced was dropped
// — and the history row said "0 files" forever afterwards.
func TestStoppedJobStillReportsItsOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DBX_PLUGIN_DATA_DIR", dir)
	m := NewJobManager(stopThenWriteEngine{})
	doneCh, pub := newDoneChannel()
	id, err := m.Start(AnalysisOptions{Mode: "repl", WorkType: "rollback"}, &ConnectionSpec{Host: "127.0.0.1", Port: 3306}, pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := waitFile(filepath.Join(dir, "jobs", id, "my2sql.rollback.sql"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(id, pub); err != nil {
		t.Fatal(err)
	}
	res := collectDone(t, doneCh)
	if res.Status != StatusStopped {
		t.Fatalf("status=%s want stopped", res.Status)
	}
	if len(res.Files) != 1 || res.Files[0].Name != "my2sql.rollback.sql" {
		t.Fatalf("stopped job lost its output: %+v", res.Files)
	}
	if res.Error == "" {
		t.Fatal("stop reason must still travel with the result")
	}
	// The result travels through JSON, so numbers arrive as float64.
	if got, ok := res.Stats["totalRows"].(float64); !ok || got != 2 {
		t.Fatalf("stats dropped: %+v", res.Stats)
	}
	// The persisted summary and the history list must agree with what the UI showed.
	b, err := os.ReadFile(filepath.Join(dir, "jobs", id, jobMetaName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"status": "stopped"`) {
		t.Fatalf("summary does not record the stop: %s", b)
	}
	outs, err := m.ListOutputs(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 1 {
		t.Fatalf("output listing lost the stopped job's file: %+v", outs)
	}
	list, err := m.History(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Files != 1 {
		t.Fatalf("history lost the output: %+v", list)
	}
}

// hangingWriteEngine writes output and then never returns: a wedged socket read
// that the cancellation cannot reach. The watchdog has to terminate the job.
type hangingWriteEngine struct{ release chan struct{} }

func (e *hangingWriteEngine) Run(ctx context.Context, spec *ConnectionSpec, opts AnalysisOptions, outDir string, progress func(ProgressEvent)) (JobResult, error) {
	_ = os.WriteFile(filepath.Join(outDir, "my2sql.forward.sql"), []byte("-- forward\n"), 0o600)
	<-e.release
	return JobResult{}, ctx.Err()
}

// The watchdog result is what the UI sees for a wedged engine, so it must not be
// an empty report: the SQL the engine already flushed is on disk and countable.
func TestWatchdogResultListsOutputOnDisk(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DBX_PLUGIN_DATA_DIR", dir)
	eng := &hangingWriteEngine{release: make(chan struct{})}
	m := NewJobManager(eng)
	doneCh, pub := newDoneChannel()
	id, err := m.Start(AnalysisOptions{Mode: "repl"}, &ConnectionSpec{Host: "127.0.0.1", Port: 3306}, pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := waitFile(filepath.Join(dir, "jobs", id, "my2sql.forward.sql"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(id, pub); err != nil {
		t.Fatal(err)
	}
	res := collectDone(t, doneCh)
	if res.Status != StatusStopped {
		t.Fatalf("status=%s want stopped", res.Status)
	}
	if len(res.Files) != 1 || res.Files[0].Name != "my2sql.forward.sql" {
		t.Fatalf("watchdog reported no output although the file is on disk: %+v", res.Files)
	}
	if res.OutDir == "" {
		t.Fatal("watchdog result must name the output directory so the UI can list it")
	}
	close(eng.release)
	// The engine landing later with nothing new must not emit a second event.
	time.Sleep(200 * time.Millisecond)
	select {
	case extra := <-doneCh:
		t.Fatalf("unexpected second done event: %+v", extra)
	default:
	}
}

func waitFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("%s never appeared", path)
}

// --- panic isolation ---------------------------------------------------------

type panickingEngine struct{}

func (panickingEngine) Run(ctx context.Context, spec *ConnectionSpec, opts AnalysisOptions, outDir string, progress func(ProgressEvent)) (JobResult, error) {
	panic("simulated binlog kernel panic")
}

// Regression for the "unknown job: <id>" report: a panic inside the engine took
// the whole sidecar down, the host restarted it with backoff, and the new
// process had no job map — so stop failed and every other job in the tab was
// lost too. A panic must instead be this job's terminal failure, with the
// manager still serving the job afterwards.
func TestEnginePanicFailsOnlyItsJob(t *testing.T) {
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	m := NewJobManager(panickingEngine{})
	doneCh := make(chan JobResult, 4)
	pub := func(method string, params any) {
		if method != "my2sql/job/done" {
			return
		}
		b, _ := json.Marshal(params)
		var r JobResult
		if json.Unmarshal(b, &r) == nil {
			doneCh <- r
		}
	}
	id, err := m.Start(AnalysisOptions{Mode: "file"}, &ConnectionSpec{Host: "127.0.0.1", Port: 3306}, pub)
	if err != nil {
		t.Fatal(err)
	}
	var res JobResult
	select {
	case res = <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("no job/done after an engine panic — the process died instead of failing the job")
	}
	if res.Status != StatusFailed {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if !strings.Contains(res.Error, "engine panic") {
		t.Fatalf("error=%q want the failure to be reported", res.Error)
	}
	// The panic value itself may carry SQL or connection text, and this string is
	// persisted into job.json and shown in the workbench: only the log gets it.
	if strings.Contains(res.Error, "simulated binlog kernel panic") {
		t.Fatalf("panic value leaked to the client: %q", res.Error)
	}
	// The job is still addressable: stopping a finished job is a no-op, not an error.
	if err := m.Stop(id, pub); err != nil {
		t.Fatalf("stop after panic: %v", err)
	}
	select {
	case extra := <-doneCh:
		t.Fatalf("stopping a finished job emitted a second done event: %+v", extra)
	case <-time.After(stopGrace + 200*time.Millisecond):
	}
}

// --- history -----------------------------------------------------------------

type writingEngine struct{}

func (writingEngine) Run(ctx context.Context, spec *ConnectionSpec, opts AnalysisOptions, outDir string, progress func(ProgressEvent)) (JobResult, error) {
	if err := os.WriteFile(filepath.Join(outDir, "my2sql.rollback.sql"), []byte("-- rollback\n"), 0o600); err != nil {
		return JobResult{}, err
	}
	return JobResult{Stats: map[string]any{"totalRows": 3}}, nil
}

func collectDone(t *testing.T, ch chan JobResult) JobResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("job never reached a terminal state")
		return JobResult{}
	}
}

// The history view exists because the in-memory job map does not survive the
// sidecar: after a restart the workbench must still list what was analysed, with
// its window and filters, straight from the output directory.
func TestHistorySurvivesProcessRestart(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DBX_PLUGIN_DATA_DIR", dir)
	opts := AnalysisOptions{SourceConnectionID: "conn-1", Mode: "repl", WorkType: "rollback", Databases: []string{"db1"}, Tables: []string{"t1"}, StopDT: "2026-10-27 09:00:00"}
	spec := &ConnectionSpec{ID: "conn-1", Name: "prod-orders", Host: "10.0.0.7", Port: 3306, Username: "dba_root", Password: "s3cr3t-pw"}
	m := NewJobManager(writingEngine{})
	doneCh := make(chan JobResult, 2)
	pub := func(method string, params any) {
		if method != "my2sql/job/done" {
			return
		}
		b, _ := json.Marshal(params)
		var r JobResult
		if json.Unmarshal(b, &r) == nil {
			doneCh <- r
		}
	}
	id, err := m.Start(opts, spec, pub)
	if err != nil {
		t.Fatal(err)
	}
	collectDone(t, doneCh)

	rows, err := NewJobManager(writingEngine{}).History(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].JobID != id {
		t.Fatalf("history after restart = %+v", rows)
	}
	got := rows[0]
	if got.Status != StatusDone {
		t.Fatalf("status=%s want done", got.Status)
	}
	if got.Files != 1 {
		t.Fatalf("files=%d want 1 — job.json must not count as generated output", got.Files)
	}
	if len(got.Options.Databases) != 1 || got.Options.Databases[0] != "db1" || got.Options.StopDT != opts.StopDT {
		t.Fatalf("options not persisted: %+v", got.Options)
	}
	if got.Connection.ID != "conn-1" || got.Connection.Name != "prod-orders" || got.Connection.Host != "10.0.0.7" || got.Connection.Port != 3306 {
		t.Fatalf("connection snapshot missing: %+v", got.Connection)
	}
	if _, err := time.Parse(time.RFC3339, got.CreatedAt); err != nil {
		t.Fatalf("createdAt=%q: %v", got.CreatedAt, err)
	}

	files, err := m.ListOutputs(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0]["name"] != "my2sql.rollback.sql" {
		t.Fatalf("ListOutputs leaked the summary file: %+v", files)
	}
	if _, err := m.Preview(id, "job.json", 0, 10); err == nil {
		t.Fatal("job.json must not be previewable as generated output")
	}
	// The summary itself must be readable and must not carry credentials.
	metaFile := filepath.Join(dir, "jobs", id, jobMetaName)
	if fi, err := os.Stat(metaFile); err != nil {
		t.Fatalf("summary not written: %v", err)
	} else if fi.Mode().Perm() != 0o600 {
		t.Fatalf("summary mode=%o want 0600", fi.Mode().Perm())
	}
	if raw, err := os.ReadFile(metaFile); err != nil {
		t.Fatal(err)
	} else {
		text := string(raw)
		if strings.Contains(strings.ToLower(text), "password") {
			t.Fatal("job.json must never contain credential fields")
		}
		// The history row names the connection; it must never carry its account.
		if strings.Contains(text, spec.Password) || strings.Contains(text, spec.Username) {
			t.Fatal("job.json leaked the connection account or password")
		}
		if !strings.Contains(text, "prod-orders") {
			t.Fatal("job.json must record the connection name for the history view")
		}
	}
}

// A history scan walks a directory the user (or an older version) may have
// touched, so anything that is not a well-formed job id is skipped rather than
// turned into a path.
func TestHistoryIgnoresForeignEntries(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DBX_PLUGIN_DATA_DIR", dir)
	if rows, err := NewJobManager(writingEngine{}).History(0); err != nil || len(rows) != 0 {
		t.Fatalf("missing jobs directory: rows=%v err=%v", rows, err)
	}
	root := filepath.Join(dir, "jobs")
	for _, name := range []string{"20261027T000000-1", "with.dot", "bad name", "brackets[]"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "loose.sql"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	rows, err := NewJobManager(writingEngine{}).History(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].JobID != "20261027T000000-1" {
		t.Fatalf("foreign entries leaked into history: %+v", rows)
	}
	if _, err := time.Parse(time.RFC3339, rows[0].CreatedAt); err != nil {
		t.Fatalf("createdAt must fall back to the id timestamp, got %q", rows[0].CreatedAt)
	}
}

// Deleting a history entry has to remove the directory, not just hide the row:
// the point of the button is to reclaim the gigabytes a wide-running analysis
// left behind.
func TestDeleteRemovesJobFromDiskAndHistory(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DBX_PLUGIN_DATA_DIR", dir)
	m := NewJobManager(writingEngine{})
	doneCh, pub := newDoneChannel()
	id, err := m.Start(AnalysisOptions{Mode: "repl"}, &ConnectionSpec{Host: "127.0.0.1", Port: 3306}, pub)
	if err != nil {
		t.Fatal(err)
	}
	collectDone(t, doneCh)
	jobDir := filepath.Join(dir, "jobs", id)
	if err := m.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(jobDir); !os.IsNotExist(err) {
		t.Fatalf("job directory still there: %v", err)
	}
	if rows, err := m.History(0); err != nil || len(rows) != 0 {
		t.Fatalf("deleted job still in history: rows=%+v err=%v", rows, err)
	}
	if _, err := m.ListOutputs(id); err == nil {
		t.Fatal("listing a deleted job must fail, not return an empty set")
	}
}

// A running job owns live goroutines writing into its directory; deleting it
// from under the engine would leave a job that reports success about files that
// no longer exist.
func TestDeleteRefusesRunningJob(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DBX_PLUGIN_DATA_DIR", dir)
	eng := &hangingWriteEngine{release: make(chan struct{})}
	m := NewJobManager(eng)
	id, err := m.Start(AnalysisOptions{Mode: "repl"}, &ConnectionSpec{Host: "127.0.0.1", Port: 3306}, func(string, any) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := waitFile(filepath.Join(dir, "jobs", id, "my2sql.forward.sql"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	err = m.Delete(id)
	if err == nil || !strings.Contains(err.Error(), "still running") {
		t.Fatalf("Delete(running job) = %v, want a 'still running' refusal", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "jobs", id)); statErr != nil {
		t.Fatalf("refused delete must leave the job alone: %v", statErr)
	}
	close(eng.release)
	time.Sleep(200 * time.Millisecond) // let the engine unwind before TempDir cleanup
}

// The id is a directory name supplied by the UI, so a traversal-shaped value
// must be rejected before RemoveAll ever sees it.
func TestDeleteRejectsForeignIDs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DBX_PLUGIN_DATA_DIR", dir)
	m := NewJobManager(writingEngine{})
	for _, id := range []string{"", "..", "../outside", ".", "a/b", "bad name"} {
		if err := m.Delete(id); err == nil {
			t.Fatalf("Delete(%q) succeeded", id)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "keep.txt")); err != nil {
		t.Fatalf("rejected deletes must not touch anything else: %v", err)
	}
}
