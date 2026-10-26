package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ExecStatus is the observable state of one rollback execution.
type ExecStatus string

const (
	ExecRunning ExecStatus = "running"
	ExecDone    ExecStatus = "done"
	ExecFailed  ExecStatus = "failed"
	ExecStopped ExecStatus = "stopped"
)

type execution struct {
	ID       string `json:"id"`
	Status   ExecStatus
	Total    int
	Done     int
	FailedAt int
	Error    string
	cancel   context.CancelFunc
	mu       sync.Mutex
}

// pendingConfirm is the one-shot authorization produced by Prepare. Start only
// runs when it presents this token; the token is bound to the exact
// job/file/target triple and is consumed on the first Start attempt, so a
// replayed or scripted exec/start cannot execute a file without a fresh,
// validated Prepare. This moves the confirmation gate from the UI into the
// sidecar.
type pendingConfirm struct {
	jobID    string
	file     string
	targetID string
	created  time.Time
}

// confirmTTL bounds how long a Prepare result stays startable. Re-running
// Prepare is cheap, so expiry only forces a fresh look at the preview.
const confirmTTL = 10 * time.Minute

// ExecManager runs generated rollback/SQL files against a target connection.
// Execution requires a confirmToken from Prepare (see pendingConfirm); the UI
// confirmation dialog sits on top of that, it is not the gate itself.
type ExecManager struct {
	mu       sync.RWMutex
	exec     map[string]*execution
	pending  map[string]*pendingConfirm
	sessions *SessionStore
	jobs     *JobManager
}

func NewExecManager(sessions *SessionStore, jobs *JobManager) *ExecManager {
	return &ExecManager{exec: make(map[string]*execution), pending: make(map[string]*pendingConfirm), sessions: sessions, jobs: jobs}
}

func newConfirmToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate confirm token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// gcPendingLocked drops expired confirmations. Callers hold m.mu.
func (m *ExecManager) gcPendingLocked() {
	for token, pe := range m.pending {
		if time.Since(pe.created) > confirmTTL {
			delete(m.pending, token)
		}
	}
}

// Prepare counts statements and returns a bounded preview plus a one-shot
// confirmToken so the host can show a confirmation dialog naming the target
// and the statement volume. Only Start with that token may execute the file.
func (m *ExecManager) Prepare(jobID, fileName, targetConnectionID string) (map[string]any, error) {
	if _, err := m.sessions.Require(targetConnectionID); err != nil {
		return nil, err
	}
	dir, err := jobOutputDir(jobID)
	if err != nil {
		return nil, err
	}
	clean := filepath.Base(fileName)
	if clean != fileName {
		return nil, fmt.Errorf("invalid file name")
	}
	data, err := os.ReadFile(filepath.Join(dir, clean))
	if err != nil {
		return nil, err
	}
	stmts := splitStatements(string(data))
	preview := stmts
	if len(preview) > 20 {
		preview = preview[:20]
	}
	token, err := newConfirmToken()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.gcPendingLocked()
	m.pending[token] = &pendingConfirm{jobID: jobID, file: clean, targetID: targetConnectionID, created: time.Now()}
	m.mu.Unlock()
	return map[string]any{
		"jobId":              jobID,
		"file":               clean,
		"statementCount":     len(stmts),
		"targetConnectionId": targetConnectionID,
		"firstStatements":    preview,
		"confirmToken":       token,
	}, nil
}

func (m *ExecManager) Start(jobID, fileName, targetConnectionID, confirmToken string, publish EventPublisher) (string, error) {
	if confirmToken == "" {
		return "", fmt.Errorf("missing confirmToken: run exec/prepare first")
	}
	m.mu.Lock()
	pe, ok := m.pending[confirmToken]
	delete(m.pending, confirmToken) // one-shot, consumed even when validation below fails
	m.gcPendingLocked()
	m.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("unknown or already-used confirmToken: run exec/prepare again")
	}
	if time.Since(pe.created) > confirmTTL {
		return "", fmt.Errorf("confirmToken expired: run exec/prepare again")
	}
	clean := filepath.Base(fileName)
	if clean != fileName || pe.jobID != jobID || pe.file != clean || pe.targetID != targetConnectionID {
		return "", fmt.Errorf("confirmToken does not match this job/file/target: run exec/prepare again")
	}
	spec, err := m.sessions.Require(targetConnectionID)
	if err != nil {
		return "", err
	}
	dir, err := jobOutputDir(jobID)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(dir, clean))
	if err != nil {
		return "", err
	}
	stmts := splitStatements(string(data))

	id := "exec-" + time.Now().UTC().Format("20060102T150405")
	ctx, cancel := context.WithCancel(context.Background())
	ex := &execution{ID: id, Status: ExecRunning, Total: len(stmts), cancel: cancel}
	m.mu.Lock()
	m.exec[id] = ex
	m.mu.Unlock()

	go m.run(ctx, ex, spec, stmts, publish)
	return id, nil
}

func (m *ExecManager) run(ctx context.Context, ex *execution, spec *ConnectionSpec, stmts []string, publish EventPublisher) {
	db, err := openDB(ctx, spec)
	if err != nil {
		ex.mu.Lock()
		ex.Status = ExecFailed
		ex.Error = err.Error()
		ex.mu.Unlock()
		m.emit(ex, publish)
		return
	}
	defer db.Close()

	for i, stmt := range stmts {
		if ctx.Err() == context.Canceled {
			ex.mu.Lock()
			ex.Status = ExecStopped
			ex.mu.Unlock()
			break
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			ex.mu.Lock()
			ex.Status = ExecFailed
			ex.Done = i
			ex.FailedAt = i
			ex.Error = fmt.Sprintf("statement %d failed: %v", i+1, err)
			ex.mu.Unlock()
			break
		}
		ex.mu.Lock()
		ex.Done = i + 1
		ex.mu.Unlock()
		if publish != nil && (i%200 == 0 || i == len(stmts)-1) {
			m.emit(ex, publish)
		}
	}

	ex.mu.Lock()
	if ex.Status == ExecRunning {
		ex.Status = ExecDone
	}
	ex.mu.Unlock()
	m.emit(ex, publish)
}

func (m *ExecManager) emit(ex *execution, publish EventPublisher) {
	if publish == nil {
		return
	}
	ex.mu.Lock()
	payload := map[string]any{"executionId": ex.ID, "status": ex.Status, "total": ex.Total, "done": ex.Done, "error": ex.Error}
	ex.mu.Unlock()
	publish("my2sql/exec/progress", payload)
}

func (m *ExecManager) Stop(id string) error {
	m.mu.RLock()
	ex, ok := m.exec[id]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown execution: %s", id)
	}
	ex.mu.Lock()
	if ex.Status == ExecRunning {
		ex.Status = ExecStopped
	}
	ex.mu.Unlock()
	if ex.cancel != nil {
		ex.cancel()
	}
	return nil
}

func (m *ExecManager) Snapshot(id string) (map[string]any, error) {
	m.mu.RLock()
	ex, ok := m.exec[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown execution: %s", id)
	}
	ex.mu.Lock()
	defer ex.mu.Unlock()
	return map[string]any{"executionId": ex.ID, "status": ex.Status, "total": ex.Total, "done": ex.Done, "failedAt": ex.FailedAt, "error": ex.Error}, nil
}

// splitStatements performs a pragmatic line-based split on top-level semicolons,
// skipping `--`/`#` comment lines and blank lines. my2sql emits one statement
// per line which makes this safe for its own generated files; hand-edited files
// with multi-line statements are out of scope for the skeleton.
func splitStatements(content string) []string {
	out := []string{}
	var buf strings.Builder
	flush := func() {
		s := strings.TrimSpace(buf.String())
		buf.Reset()
		if s != "" {
			out = append(out, s)
		}
	}
	for _, line := range strings.Split(content, "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "--") || strings.HasPrefix(t, "#") || strings.EqualFold(t, "SET NAMES") {
			continue
		}
		buf.WriteString(line)
		if strings.HasSuffix(t, ";") {
			s := strings.TrimSpace(strings.TrimSuffix(buf.String(), ";"))
			buf.Reset()
			if s != "" {
				out = append(out, s)
			}
		} else {
			buf.WriteString(" ")
		}
	}
	flush()
	return out
}
