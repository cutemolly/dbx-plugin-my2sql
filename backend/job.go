package main

import (
	"context"
	"sync"
	"time"
)

// AnalysisOptions mirrors the my2sql parameter set the workbench collects.
// Field names line up with the UI controls documented in the plugin design.
type AnalysisOptions struct {
	SourceConnectionID string `json:"sourceConnectionId"`
	// Mode: "repl" (online, fake replica) or "file" (offline local binlog).
	Mode string `json:"mode"`
	// WorkType: "rollback" | "2sql" | "stats".
	WorkType string `json:"workType"`

	StartFile string `json:"startFile"`
	StopFile  string `json:"stopFile"`
	StartPos  int64  `json:"startPos"`
	StopPos   int64  `json:"stopPos"`
	StartDT   string `json:"startDatetime"`
	StopDT    string `json:"stopDatetime"`

	// Offline mode only: path to the local binlog file (or its directory).
	LocalBinlogFile string `json:"localBinlogFile"`

	Databases []string `json:"databases"`
	Tables    []string `json:"tables"`
	SQLTypes  []string `json:"sqlTypes"` // insert / update / delete subset

	FilePerTable      bool `json:"filePerTable"`
	AddExtraInfo      bool `json:"addExtraInfo"`
	IgnorePKForInsert bool `json:"ignorePrimaryKeyForInsert"`
	FullColumns       bool `json:"fullColumns"`
	UseUniqueKey      bool `json:"useUniqueKey"`
	DoNotPrefixDB     bool `json:"doNotAddPrefixDb"`
	Threads           int  `json:"threads"`
	BigTrxRowLimit    int  `json:"bigTrxRowLimit"`
	LongTrxSeconds    int  `json:"longTrxSeconds"`

	// Optional per-analysis timezone override; falls back to the connection's -tl.
	Timezone string `json:"timezone"`
}

// JobStatus is the observable state of one running/completed analysis.
type JobStatus string

const (
	StatusRunning JobStatus = "running"
	StatusDone    JobStatus = "done"
	StatusFailed  JobStatus = "failed"
	StatusStopped JobStatus = "stopped"
)

// ProgressEvent is emitted on the "my2sql/job/progress" event channel.
type ProgressEvent struct {
	JobID        string `json:"jobId"`
	Stage        string `json:"stage"`
	ParsedEvents int64  `json:"parsedEvents"`
	Rows         int64  `json:"rows"`
	SQLFiles     int    `json:"sqlFiles"`
	Message      string `json:"message,omitempty"`
}

// JobResult is delivered on the "my2sql/job/done" event channel.
type JobResult struct {
	JobID     string         `json:"jobId"`
	Status    JobStatus      `json:"status"`
	OutDir    string         `json:"outDir"`
	Files     []OutputFile   `json:"files"`
	Stats     map[string]any `json:"stats,omitempty"`
	Error     string         `json:"error,omitempty"`
	ElapsedMs int64          `json:"elapsedMs"`
}

type OutputFile struct {
	Name string `json:"name"`
	Path string `json:"-"` // server-side path, never sent to the UI
	Size int64  `json:"size"`
	Kind string `json:"kind"` // "sql" | "rollback" | "stats"
}

type job struct {
	id      string
	opts    AnalysisOptions
	status  JobStatus
	result  *JobResult
	created time.Time
	cancel  context.CancelFunc
	done    chan struct{} // closed by run() when the engine goroutine lands
	mu      sync.Mutex
}

// JobManager owns in-flight and completed analyses. The sidecar is shared per
// plugin process (not per tab), so one manager serves every workbench instance.
type JobManager struct {
	mu     sync.RWMutex
	jobs   map[string]*job
	engine Engine
	seq    int
}

func NewJobManager(engine Engine) *JobManager {
	return &JobManager{jobs: make(map[string]*job), engine: engine}
}

func (m *JobManager) nextID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	return time.Now().UTC().Format("20060102T150405") + "-" + itoa(m.seq)
}
