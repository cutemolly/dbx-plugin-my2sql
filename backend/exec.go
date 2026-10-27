package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
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
	jobID      string
	file       string
	targetID   string
	statements int // counted by Prepare; reused as the progress total
	created    time.Time
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

// ForgetPending drops the confirmations issued for a job whose output has just
// been deleted, so a token can never survive into an execution of a file that is
// gone. The one-shot check in Start would reject it anyway; this keeps the
// pending set honest.
func (m *ExecManager) ForgetPending(jobID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for token, pe := range m.pending {
		if pe.jobID == jobID {
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
	dir, err := jobOutputPath(jobID)
	if err != nil {
		return nil, err
	}
	clean := filepath.Base(fileName)
	if clean != fileName {
		return nil, fmt.Errorf("invalid file name")
	}
	// Count and preview by streaming: the confirm dialog has to name the
	// statement volume of a file that can be gigabytes, and reading it whole is
	// what killed the process.
	count := 0
	preview := []string{}
	if err := streamStatements(filepath.Join(dir, clean), func(stmt string, _ int) error {
		count++
		if len(preview) < 20 {
			preview = append(preview, stmt)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, fmt.Errorf("%s contains no executable statement", clean)
	}
	token, err := newConfirmToken()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.gcPendingLocked()
	m.pending[token] = &pendingConfirm{jobID: jobID, file: clean, targetID: targetConnectionID, statements: count, created: time.Now()}
	m.mu.Unlock()
	return map[string]any{
		"jobId":              jobID,
		"file":               clean,
		"statementCount":     count,
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
	dir, err := jobOutputPath(jobID)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, clean)

	id := "exec-" + time.Now().UTC().Format("20060102T150405")
	ctx, cancel := context.WithCancel(context.Background())
	// The total comes from the counted Prepare rather than a second scan of a
	// file that may be gigabytes; the token is only valid for that exact file.
	ex := &execution{ID: id, Status: ExecRunning, Total: pe.statements, cancel: cancel}
	m.mu.Lock()
	m.exec[id] = ex
	m.mu.Unlock()

	go m.run(ctx, ex, spec, path, publish)
	return id, nil
}

// errScanStopped ends the statement stream without meaning "the execution
// failed": it is what a cancel or a failing statement uses to unwind.
var errScanStopped = errors.New("statement scan stopped")

func (m *ExecManager) run(ctx context.Context, ex *execution, spec *ConnectionSpec, path string, publish EventPublisher) {
	// Same isolation as the analysis engine: a panic while executing statements
	// must fail this execution, not the process that also holds every running
	// job and the pending confirm tokens.
	defer func() {
		if rec := recover(); rec != nil {
			debug.PrintStack()
			log.Printf("panic while executing rollback statements: %v", rec)
			ex.mu.Lock()
			ex.Status = ExecFailed
			ex.Error = "execution panic (details in the plugin log)"
			ex.mu.Unlock()
			m.emit(ex, publish)
		}
	}()
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

	// Stream the file: execute each statement as it is read instead of holding
	// every statement of a multi-gigabyte rollback file in memory.
	scanErr := streamStatements(path, func(stmt string, i int) error {
		if ctx.Err() != nil {
			ex.mu.Lock()
			ex.Status = ExecStopped
			ex.mu.Unlock()
			return errScanStopped
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			ex.mu.Lock()
			ex.Status = ExecFailed
			ex.Done = i
			ex.FailedAt = i
			ex.Error = fmt.Sprintf("statement %d failed: %v", i+1, err)
			ex.mu.Unlock()
			return errScanStopped
		}
		ex.mu.Lock()
		ex.Done = i + 1
		if i+1 > ex.Total {
			ex.Total = i + 1 // the file grew past what Prepare counted
		}
		ex.mu.Unlock()
		if publish != nil && i%200 == 0 {
			m.emit(ex, publish)
		}
		return nil
	})
	if scanErr != nil && !errors.Is(scanErr, errScanStopped) {
		ex.mu.Lock()
		ex.Status = ExecFailed
		if ex.Error == "" {
			ex.Error = scanErr.Error()
		}
		ex.mu.Unlock()
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
	sp := stmtSplitter{emit: func(stmt string) error {
		out = append(out, stmt)
		return nil
	}}
	for _, line := range strings.Split(content, "\n") {
		_ = sp.line(line)
	}
	_ = sp.finish()
	return out
}

// stmtSplitter is the single definition of a statement boundary in generated
// SQL. It is fed one line at a time so the same rules apply whether the caller
// holds the whole file (splitStatements) or is streaming it (streamStatements).
type stmtSplitter struct {
	buf  strings.Builder
	emit func(stmt string) error
}

func (s *stmtSplitter) line(raw string) error {
	t := strings.TrimSpace(raw)
	if t == "" || strings.HasPrefix(t, "--") || strings.HasPrefix(t, "#") || strings.EqualFold(t, "SET NAMES") {
		return nil
	}
	s.buf.WriteString(raw)
	if strings.HasSuffix(t, ";") {
		stmt := strings.TrimSpace(strings.TrimSuffix(s.buf.String(), ";"))
		s.buf.Reset()
		if stmt != "" {
			return s.emit(stmt)
		}
		return nil
	}
	s.buf.WriteString(" ")
	return nil
}

// finish emits a trailing statement that was never closed by a semicolon.
func (s *stmtSplitter) finish() error {
	stmt := strings.TrimSpace(s.buf.String())
	s.buf.Reset()
	if stmt != "" {
		return s.emit(stmt)
	}
	return nil
}

// maxStatementBytes bounds a single statement read from an output file. A
// generated file ends every statement with a newline, so anything past this is
// a corrupt or hand-edited file; failing loudly is better than allocating the
// rest of it or executing a silently truncated statement.
const maxStatementBytes = 64 << 20

// nextLine reads one '\n'-terminated line, keeping at most max bytes of it.
// found is false only when the reader is at end of file with nothing left;
// an empty line is reported as found with an empty string, so callers keep the
// original line numbering. truncated says characters were dropped.
func nextLine(br *bufio.Reader, max int) (line string, truncated bool, found bool, err error) {
	var b strings.Builder
	for {
		chunk, rerr := br.ReadSlice('\n')
		if len(chunk) > 0 {
			found = true
			if b.Len() < max {
				if room := max - b.Len(); len(chunk) > room {
					b.Write(chunk[:room])
					truncated = true
				} else {
					b.Write(chunk)
				}
			} else {
				truncated = true
			}
		}
		if rerr != nil {
			if errors.Is(rerr, bufio.ErrBufferFull) {
				continue // the buffer is full, not the line
			}
			if errors.Is(rerr, io.EOF) {
				return strings.TrimSuffix(b.String(), "\n"), truncated, found, nil
			}
			return "", false, false, rerr
		}
		return strings.TrimSuffix(b.String(), "\n"), truncated, found, nil
	}
}

// streamStatements scans a SQL file and calls fn for every statement in file
// order, keeping at most one statement in memory. Rollback files from a real
// analysis run into the gigabytes, and loading one whole (the previous
// os.ReadFile + splitStatements) is what pushed the sidecar into a fatal
// allocation and took the running jobs down with it. fn's error aborts the
// scan and is returned to the caller.
func streamStatements(path string, fn func(stmt string, index int) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	// ReadString, not Scanner: a generated INSERT can carry a row wide enough to
	// cross the scanner token limit.
	br := bufio.NewReaderSize(f, 256<<10)
	sp := stmtSplitter{}
	i := 0
	sp.emit = func(stmt string) error {
		err := fn(stmt, i)
		i++
		return err
	}
	lineNo := 0
	for {
		line, truncated, found, rerr := nextLine(br, maxStatementBytes)
		lineNo++
		if truncated {
			return fmt.Errorf("line %d of %s is longer than %d bytes; refusing to execute a possibly truncated statement", lineNo, filepath.Base(path), maxStatementBytes)
		}
		if found {
			if lerr := sp.line(line); lerr != nil {
				return lerr
			}
		}
		if rerr != nil {
			return rerr
		}
		if !found {
			break
		}
	}
	return sp.finish()
}
