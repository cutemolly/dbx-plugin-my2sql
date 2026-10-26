package main

import (
	"context"
	"fmt"
)

// Engine is the extension point for the binlog parser. The real parser lives
// in engine_my2sql.go (build-tag `my2sql`) and is an independent Go
// implementation of the my2sql binlog-to-SQL algorithm on top of
// go-mysql-org/go-mysql; no upstream my2sql source is copied or vendored. The
// contract below is stable so the UI, job lifecycle and the rollback executor
// can be built and tested against it.
//
// Implementations must:
//   - honor ctx cancellation (job/stop) and return promptly;
//   - write generated SQL / stats files under outDir and report them in Result;
//   - call progress at a bounded rate (not per row) so event frames stay < 8 MiB.
type Engine interface {
	Run(ctx context.Context, spec *ConnectionSpec, opts AnalysisOptions, outDir string, progress func(ProgressEvent)) (JobResult, error)
}

// pendingEngine is the default-build placeholder used when the sidecar is
// compiled without `-tags my2sql`. The real kernel lives in
// engine_my2sql.go and is linked in tagged builds only.
type pendingEngine struct{}

func (pendingEngine) Run(ctx context.Context, spec *ConnectionSpec, opts AnalysisOptions, outDir string, progress func(ProgressEvent)) (JobResult, error) {
	if progress != nil {
		progress(ProgressEvent{Stage: "preparing", Message: "Validating source binlog settings before parsing."})
	}
	return JobResult{}, fmt.Errorf(
		"binlog kernel is not compiled into this sidecar: rebuild with -tags my2sql "+
			"(e.g. export GOFLAGS=-tags=my2sql before running 'dbx-plugin dev', or set DBX_MY2SQL_TAGS=my2sql for package.mjs). "+
			"Source=%q mode=%q work-type=%q",
		spec.Endpoint(), opts.Mode, opts.WorkType,
	)
}

// engineFor is selected at build time: the default build returns pendingEngine
// (see engine_default.go) so the tree compiles with no external dependencies;
// building with `-tags my2sql` links the real go-mysql kernel (engine_my2sql.go).
