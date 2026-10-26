package main

import (
	"context"
	"encoding/json"
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
	var events []json.RawMessage
	pub := func(method string, params any) {
		if method != "my2sql/job/done" {
			return
		}
		b, _ := json.Marshal(params)
		events = append(events, b)
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
		if len(events) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(events) == 0 {
		t.Fatalf("watchdog never published job/done (grace=%s)", stopGrace)
	}
	var res JobResult
	if err := json.Unmarshal(events[0], &res); err != nil {
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
	if len(events) != 1 {
		t.Fatalf("late engine landing emitted extra events: %d", len(events))
	}
}
