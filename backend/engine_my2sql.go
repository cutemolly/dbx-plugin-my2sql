//go:build my2sql

package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	gm "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	golog "github.com/siddontang/go-log/log"
)

// engineFor links the real binlog kernel when built with `-tags my2sql`.
//
// Build & enable (requires network to fetch go-mysql + a real MySQL to validate
// before production flashback use):
//
//	go get github.com/go-mysql-org/go-mysql@v1.9.1
//	go build -tags my2sql ./...
//
// Statement text is produced entirely by the sqlgen package (fully unit-tested
// offline); this file only decodes binlog events and drives the range/filter
// logic, so the correctness-sensitive SQL generation stays independently
// verifiable from the byte-level parser.
func engineFor() Engine { return newMy2sqlEngine() }

type my2sqlEngine struct{}

func newMy2sqlEngine() Engine { return &my2sqlEngine{} }

// Run implements Engine. It parses either an online stream (fake replica) or
// offline local binlog files, applies the my2sql range + filter + type rules,
// and writes forward SQL, rollback SQL and/or a stats file under outDir.
func (*my2sqlEngine) Run(ctx context.Context, spec *ConnectionSpec, opts AnalysisOptions, outDir string, progress func(ProgressEvent)) (JobResult, error) {
	st, err := newParseState(ctx, spec, opts, outDir, progress)
	if err != nil {
		return JobResult{}, err
	}
	defer st.close()

	if err := st.loadWindow(); err != nil {
		return JobResult{}, err
	}

	switch strings.ToLower(opts.Mode) {
	case "file":
		err = st.runOffline()
	default:
		err = st.runOnline()
	}

	st.flush()

	result := st.result()
	if err != nil && !isStopped(err) {
		result.Status = StatusFailed
		result.Error = err.Error()
		return result, err
	}
	result.Status = StatusDone
	return result, nil
}

// parseState carries everything the event loop mutates.
type parseState struct {
	ctx      context.Context
	spec     *ConnectionSpec
	opts     AnalysisOptions
	outDir   string
	gen      GenOptions
	progress func(ProgressEvent)

	startAt time.Time
	stopAt  time.Time
	hasTime bool

	schemaCache map[string]*TableSchema

	forwardW  map[string]*sinkFile // per-table or single
	rollbackW map[string]*sinkFile

	stats     *Stats
	rows      int64
	events    int64
	sqlFiles  int
	lastEmit  time.Time
	startWall time.Time

	// transaction bracketing for big/long trx stats
	inTrx    bool
	trxRow   int
	trxStart time.Time
}

type sinkFile struct {
	path string
	name string
	kind string
	file *os.File
	buf  *bufio.Writer
}

func newParseState(ctx context.Context, spec *ConnectionSpec, opts AnalysisOptions, outDir string, progress func(ProgressEvent)) (*parseState, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	gen := GenOptions{
		IgnorePKForInsert: opts.IgnorePKForInsert,
		FullColumns:       opts.FullColumns,
		UseUniqueKey:      opts.UseUniqueKey,
		DoNotPrefixDB:     opts.DoNotPrefixDB,
		OnlyTypes:         opts.SQLTypes,
	}
	return &parseState{
		ctx:         ctx,
		spec:        spec,
		opts:        opts,
		outDir:      outDir,
		gen:         gen,
		progress:    progress,
		schemaCache: map[string]*TableSchema{},
		forwardW:    map[string]*sinkFile{},
		rollbackW:   map[string]*sinkFile{},
		stats:       NewStats(),
		startWall:   time.Now(),
	}, nil
}

// analysisLoc resolves the zone all wall-clock times are interpreted in:
// an explicit -timezone wins, otherwise the source connection's timezone
// (the zone the DBA sees on the server). Empty everywhere falls back to UTC.
func (s *parseState) analysisLoc() *time.Location {
	tz := strings.TrimSpace(s.opts.Timezone)
	if tz == "" {
		tz = s.spec.Timezone
	}
	return parseLocation(tz)
}

func (s *parseState) loadWindow() error {
	loc := s.analysisLoc()
	if s.opts.StartDT != "" {
		t, err := parseMySQLTime(s.opts.StartDT, loc)
		if err != nil {
			return fmt.Errorf("invalid -start-datetime: %w", err)
		}
		s.startAt = t
		s.hasTime = true
	}
	if s.opts.StopDT != "" {
		t, err := parseMySQLTime(s.opts.StopDT, loc)
		if err != nil {
			return fmt.Errorf("invalid -stop-datetime: %w", err)
		}
		s.stopAt = t
		s.hasTime = true
	}
	return nil
}

// eventTime converts a header timestamp (unix seconds) into the analysis zone.
func (s *parseState) eventTime(headerSec uint32) time.Time {
	return time.Unix(int64(headerSec), 0).In(s.analysisLoc())
}

func (s *parseState) inTimeWindow(t time.Time) bool {
	if !s.hasTime {
		return true
	}
	if s.startAt.IsZero() == false && t.Before(s.startAt) {
		return false
	}
	if s.stopAt.IsZero() == false && t.After(s.stopAt) {
		return false
	}
	return true
}

// carriesCommitTime reports whether a binlog event's header timestamp reflects
// real log activity at that moment. Control events are excluded: the initial
// fake ROTATE_EVENT sent during the dump handshake and the per-file
// FORMAT_DESCRIPTION / PREVIOUS_GTIDS events carry the server's *current* time
// (or the file's creation time), not a commit inside the requested window.
// Terminating on those would end an online job before it reads any in-window
// data whenever the window sits in the past.
func carriesCommitTime(t replication.EventType) bool {
	switch t {
	case replication.ROTATE_EVENT,
		replication.FORMAT_DESCRIPTION_EVENT,
		replication.PREVIOUS_GTIDS_EVENT,
		replication.HEARTBEAT_EVENT,
		replication.HEARTBEAT_LOG_EVENT_V2,
		replication.START_EVENT_V3,
		replication.STOP_EVENT:
		return false
	}
	return true
}

// handleEvent is shared by both online and offline paths. It returns stop=true
// when the configured range boundary has been passed.
func (s *parseState) handleEvent(ev *replication.BinlogEvent) (stop bool, err error) {
	if err := s.ctx.Err(); err != nil {
		return true, err
	}
	s.events++
	ts := s.eventTime(ev.Header.Timestamp)

	// The binlog stream is time-ordered, so once ANY event's header timestamp
	// passes the stop boundary the whole remaining tail is outside the window,
	// regardless of which table it touches or whether it is a row/query event.
	// Checking it here (before the db/table filter and row-kind guards below)
	// is what lets an online job finish on time even when the matched tables go
	// idle right after the window: otherwise non-matching tables or query/rotate
	// events keep arriving and the per-row stop check below never runs, so the
	// task streams live binlog forever instead of terminating.
	if s.hasTime && !s.stopAt.IsZero() && ts.After(s.stopAt) && carriesCommitTime(ev.Header.EventType) {
		return true, nil
	}

	switch e := ev.Event.(type) {
	case *replication.RowsEvent:
		// Tick liveness even for events filtered out below, otherwise an
		// idle-but-connected stream never emits progress.
		s.maybeProgress()
		kind, ok := rowsKind(ev.Header.EventType)
		if !ok {
			return false, nil
		}
		schema := string(e.Table.Schema)
		table := string(e.Table.Table)
		if !FilterMatches(s.opts.Databases, s.opts.Tables, schema, table) {
			return false, nil
		}
		if s.hasTime {
			if !s.startAt.IsZero() && ts.Before(s.startAt) {
				return false, nil
			}
		}
		if err := s.emitRows(schema, table, kind, e.Rows); err != nil {
			return false, err
		}
	case *replication.QueryEvent:
		if strings.EqualFold(strings.TrimSpace(string(e.Query)), "BEGIN") {
			s.beginTrx(ts)
		}
	case *replication.XIDEvent:
		s.commitTrx(ts)
	case *replication.RotateEvent:
		// Online rotation past the stop file ends the scan.
		if s.opts.StopFile != "" && string(e.NextLogName) > s.opts.StopFile {
			return true, nil
		}
	}
	return false, nil
}

// emitRows turns one decoded row event into forward + rollback statements. In
// stats work-type it only counts, avoiding needless schema lookups.
func (s *parseState) emitRows(schema, table string, kind EventKind, rows [][]interface{}) error {
	step := 1
	if kind == KindUpdate {
		step = 2 // rows alternate before, after
	}
	count := 0
	for i := 0; i+step <= len(rows); i += step {
		count++
	}

	if s.opts.WorkType == "stats" {
		for range make([]struct{}, count) {
			s.stats.AddRow(schema, table, kind)
			if s.inTrx {
				s.trxRow++
				s.stats.RowInTrx()
			}
			s.rows++
		}
		return nil
	}

	sch, err := s.schemaFor(schema, table)
	if err != nil {
		return err
	}
	fwdName, rbName := s.fileKeys(schema, table)

	for i := 0; i+step <= len(rows); i += step {
		var fwd, rb string
		if kind == KindUpdate {
			before := align(sch.Columns, rows[i])
			after := align(sch.Columns, rows[i+1])
			fwd, rb = sch.Generate(KindUpdate, Pair{Before: before, After: after}, s.gen)
		} else {
			img := align(sch.Columns, rows[i])
			pair := Pair{}
			if kind == KindInsert {
				pair.After = img
			} else {
				pair.Before = img
			}
			fwd, rb = sch.Generate(kind, pair, s.gen)
		}

		// 2sql -> forward only; rollback -> flashback only; default -> both.
		if s.opts.WorkType != "rollback" && fwd != "" {
			if err := s.writeSQL(fwdName, schema, table, evLabel(kind), fwd, true); err != nil {
				return err
			}
		}
		if s.opts.WorkType != "2sql" && rb != "" {
			if err := s.writeSQL(rbName, schema, table, evLabel(kind), rb, false); err != nil {
				return err
			}
		}

		s.stats.AddRow(schema, table, kind)
		if s.inTrx {
			s.trxRow++
			s.stats.RowInTrx()
		}
		s.rows++
	}
	return nil
}

func (s *parseState) beginTrx(t time.Time) {
	s.inTrx = true
	s.trxRow = 0
	s.trxStart = t
	s.stats.BeginTrx(t)
}

func (s *parseState) commitTrx(t time.Time) {
	if !s.inTrx {
		return
	}
	s.inTrx = false
	s.stats.EndTrx(t, s.opts.BigTrxRowLimit, s.opts.LongTrxSeconds)
}

// schemaFor reads (and caches) the column layout from the source server. Both
// online and offline modes need a live connection to resolve column names.
func (s *parseState) schemaFor(schema, table string) (*TableSchema, error) {
	key := schema + "." + table
	if sch, ok := s.schemaCache[key]; ok {
		return sch, nil
	}
	sch, err := SchemaColumns(s.ctx, s.spec, schema, table)
	if err != nil {
		return nil, err
	}
	s.schemaCache[key] = sch
	return sch, nil
}

func (s *parseState) fileKeys(schema, table string) (forward, rollback string) {
	if s.opts.FilePerTable {
		base := sanitize(schema) + "." + sanitize(table)
		return base + ".sql", base + ".rollback.sql"
	}
	return "my2sql.sql", "my2sql.rollback.sql"
}

func (s *parseState) writeSQL(name, schema, table, label, stmt string, forward bool) error {
	w, err := s.sink(name, filepath.Join(s.outDir, name), "sql", forward)
	if err != nil {
		return err
	}
	if s.opts.AddExtraInfo {
		fmt.Fprintf(w.buf, "/* my2sql %s at %s table `%s`.`%s` */\n", label, time.Now().Format(time.RFC3339), schema, table)
	}
	fmt.Fprintf(w.buf, "%s\n", stmt)
	return nil
}

func (s *parseState) sink(name, path, kind string, forward bool) (*sinkFile, error) {
	registry := s.rollbackW
	if forward {
		registry = s.forwardW
	}
	if w, ok := registry[name]; ok {
		return w, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := &sinkFile{path: path, name: name, kind: kind, file: f, buf: bufio.NewWriter(f)}
	registry[name] = w
	s.sqlFiles++
	return w, nil
}

// emitProgressNow bypasses maybeProgress' rate limit; the first frames flip the
// host UI out of "starting" as soon as the connection phase begins and again
// when the stream/file is actually open.
func (s *parseState) emitProgressNow(stage string) {
	if s.progress == nil {
		return
	}
	s.lastEmit = time.Now()
	s.progress(ProgressEvent{Stage: stage, ParsedEvents: s.events, Rows: s.rows, SQLFiles: s.sqlFiles})
}

func (s *parseState) maybeProgress() {
	if s.progress == nil {
		return
	}
	if time.Since(s.lastEmit) < time.Second && s.rows < 1000 {
		return
	}
	s.lastEmit = time.Now()
	s.progress(ProgressEvent{Stage: "parsing", ParsedEvents: s.events, Rows: s.rows, SQLFiles: s.sqlFiles})
}

// dumpConnectTimeout bounds the replica handshake (register + COM_BINLOG_DUMP
// first response). A healthy MySQL answers in milliseconds; only a
// non-forwarding proxy stays silent this long.
const dumpConnectTimeout = 30 * time.Second

// syncerLogger redirects go-mysql's chatter to stderr. Its default logger
// prints to os.Stdout, which the JSONL protocol reserves — a stray "[info]
// binlogsyncer..." line desynchronizes the host's scanner.
func syncerLogger() *golog.Logger {
	h, _ := golog.NewStreamHandler(os.Stderr)
	return golog.NewDefault(h)
}

func (s *parseState) runOnline() error {
	host, port := splitEndpoint(s.spec.Endpoint())
	cfg := replication.BinlogSyncerConfig{
		ServerID:   uint32(s.spec.ServerID),
		Flavor:     "mysql",
		Host:       host,
		Port:       uint16(port),
		User:       s.spec.Username,
		Password:   s.spec.Password,
		ParseTime:  true,
		UseDecimal: true,
		Logger:     syncerLogger(),
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	// Every network step below (binlog list probe + dump handshake) belongs to
	// the connection phase; announce it before touching the network so the UI
	// is never stuck on "starting" while a SQL read is in flight.
	s.emitProgressNow("connecting")
	startFile := s.opts.StartFile
	startPos := uint32(s.opts.StartPos)
	var probeErr error
	if startFile == "" {
		// Fall back to the earliest available binlog so datetime-only windows work.
		var perr error
		startFile, startPos, perr = s.earliestBinlog()
		if perr != nil {
			probeErr = perr
			startFile = ""
		}
	}
	if startFile == "" {
		if probeErr != nil {
			return fmt.Errorf("online mode needs a start binlog file, and querying the server for the list failed: %w", probeErr)
		}
		return fmt.Errorf("online mode needs -start-file or an available binlog")
	}
	// StartSync has no context hook in go-mysql; proxies (TDSQL etc.) can
	// swallow COM_REGISTER_SLAVE/COM_BINLOG_DUMP forever. Run it under a hard
	// deadline so the job turns terminal-and-diagnosable instead of hanging in
	// "connecting". A timed-out StartSync dies with the deferred syncer.Close.
	type dumpRes struct {
		streamer *replication.BinlogStreamer
		err      error
	}
	dumpCh := make(chan dumpRes, 1)
	go func() {
		st, err := syncer.StartSync(gm.Position{Name: startFile, Pos: startPos})
		dumpCh <- dumpRes{st, err}
	}()
	var streamer *replication.BinlogStreamer
	var err error
	select {
	case res := <-dumpCh:
		streamer, err = res.streamer, res.err
	case <-time.After(dumpConnectTimeout):
		return fmt.Errorf("connect to binlog dump at %s:%d timed out after %s: the server or proxy accepted the TCP connection but never finished the dump handshake (COM_REGISTER_SLAVE / COM_BINLOG_DUMP). Check that the account has REPLICATION SLAVE, that log_bin is enabled, and that the endpoint is a real MySQL node rather than a proxy that does not forward binlog dumps", startFile, startPos, dumpConnectTimeout)
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("start sync at %s:%d: %w", startFile, startPos, err)
	}
	s.emitProgressNow("parsing")
	// -stop-pos bounds the starting file only; rotating to the next file
	// lifts the bound (binlog2sql semantics).
	posLimit := uint32(0)
	if s.opts.StopPos > 0 {
		if s.opts.StopPos > int64(^uint32(0)) {
			posLimit = ^uint32(0)
		} else {
			posLimit = uint32(s.opts.StopPos)
		}
	}
	for {
		ev, err := s.nextStreamEvent(streamer)
		if err != nil {
			if err == errStreamIdle {
				// Live-idle past the stop window: the scan is complete.
				return nil
			}
			return err
		}
		if ev == nil {
			continue // idle poll before the window closed; keep waiting
		}
		if posLimit > 0 && ev.Header.LogPos > posLimit {
			return nil
		}
		if ev.Header.EventType == replication.ROTATE_EVENT {
			posLimit = 0
		}
		stop, herr := s.handleEvent(ev)
		if herr != nil {
			return herr
		}
		if stop {
			return nil
		}
	}
}

// streamIdlePoll bounds a single GetEvent wait. When it lapses with no event
// the stream is live-idle at the master's head; if the configured stop time has
// already passed in wall-clock terms, no in-range event can still arrive and the
// scan is finished. During replay events arrive back-to-back, so this deadline
// only fires once we are genuinely caught up and idle.
const streamIdlePoll = 3 * time.Second

// errStreamIdle signals the online scan reached a clean end: live-idle past the
// stop window. It is distinct from context.DeadlineExceeded so the job is
// reported as done, not failed.
var errStreamIdle = fmt.Errorf("binlog stream idle past stop time")

// nextStreamEvent fetches one binlog event, or returns (nil, nil) for a benign
// idle poll that has not yet passed the stop window. It returns errStreamIdle
// once the stream is idle AND the wall clock is past stopAt.
func (s *parseState) nextStreamEvent(streamer *replication.BinlogStreamer) (*replication.BinlogEvent, error) {
	if s.stopAt.IsZero() {
		// Unbounded capture (no stop time): wait for the next event or cancel.
		return streamer.GetEvent(s.ctx)
	}
	getCtx, cancel := context.WithTimeout(s.ctx, streamIdlePoll)
	ev, err := streamer.GetEvent(getCtx)
	cancel()
	if err != nil {
		if err == context.DeadlineExceeded && s.ctx.Err() == nil {
			if s.idlePollPastWindow() {
				return nil, errStreamIdle
			}
			return nil, nil
		}
		return nil, err
	}
	return ev, nil
}

// idlePollPastWindow reports whether a GetEvent poll that timed out with the job
// still live should end the scan: it should once the wall clock has passed the
// stop time, because no in-range binlog event can arrive after its own window
// has closed. A zero stopAt means an unbounded capture that never self-ends.
func (s *parseState) idlePollPastWindow() bool {
	return !s.stopAt.IsZero() && time.Now().After(s.stopAt)
}

// earliestBinlog asks the server for the first file in SHOW BINARY LOGS.
func (s *parseState) earliestBinlog() (string, uint32, error) {
	res, err := ListBinlogFiles(s.ctx, s.spec)
	if err != nil {
		return "", 0, err
	}
	files, _ := res["files"].([]map[string]any)
	if len(files) == 0 {
		return "", 0, fmt.Errorf("no binlog files")
	}
	name, _ := files[0]["name"].(string)
	return name, 4, nil
}

func (s *parseState) runOffline() error {
	files, err := s.localFiles()
	if err != nil {
		return err
	}
	s.emitProgressNow("parsing")
	parser := replication.NewBinlogParser()
	parser.SetParseTime(true)
	parser.SetUseDecimal(true)

	for idx, path := range files {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		var offset int64 = 4
		if idx == 0 && s.opts.StartPos > 0 {
			offset = s.opts.StartPos
		}
		done := false
		err := parser.ParseFile(path, offset, func(ev *replication.BinlogEvent) error {
			// -stop-pos bounds the first parsed file only, mirroring
			// -start-pos which also applies to that file.
			if idx == 0 && s.opts.StopPos > 0 && int64(ev.Header.LogPos) > s.opts.StopPos {
				done = true
				return errStopParse
			}
			stop, herr := s.handleEvent(ev)
			if stop {
				done = true
				return errStopParse
			}
			return herr
		})
		if done {
			return nil
		}
		if err != nil && err != errStopParse {
			return fmt.Errorf("parse %s: %w", path, err)
		}
	}
	return nil
}

// localFiles resolves LocalBinlogFile (a file or a directory) to an ordered
// list, honoring the start/stop file bounds by basename.
func (s *parseState) localFiles() ([]string, error) {
	info, err := os.Stat(s.opts.LocalBinlogFile)
	if err != nil {
		return nil, fmt.Errorf("local binlog path: %w", err)
	}
	var paths []string
	if info.IsDir() {
		entries, err := os.ReadDir(s.opts.LocalBinlogFile)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			// Skip hidden/junk files (.DS_Store on macOS) and binlog index files.
			if strings.HasPrefix(name, ".") || isIndexFile(name) {
				continue
			}
			paths = append(paths, filepath.Join(s.opts.LocalBinlogFile, name))
		}
		sort.Slice(paths, func(i, j int) bool { return binlogSortKey(paths[i]) < binlogSortKey(paths[j]) })
	} else {
		paths = []string{s.opts.LocalBinlogFile}
	}
	// Trim to the requested [startFile, stopFile] window by basename.
	trim := func(list []string) []string {
		out := list
		if s.opts.StartFile != "" {
			for i, p := range list {
				if filepath.Base(p) == s.opts.StartFile {
					out = list[i:]
					break
				}
			}
		}
		if s.opts.StopFile != "" {
			for i, p := range out {
				if filepath.Base(p) == s.opts.StopFile {
					out = out[:i+1]
					break
				}
			}
		}
		return out
	}
	return trim(paths), nil
}

func (s *parseState) flush() {
	for _, w := range s.forwardW {
		_ = w.buf.Flush()
	}
	for _, w := range s.rollbackW {
		_ = w.buf.Flush()
	}
	if s.opts.WorkType == "stats" {
		s.writeStats()
	}
}

func (s *parseState) writeStats() {
	path := filepath.Join(s.outDir, "my2sql.stats.csv")
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()
	fmt.Fprintln(bw, "table,insert,update,delete,total")
	for _, row := range s.stats.Snapshot() {
		fmt.Fprintf(bw, "%s,%d,%d,%d,%d\n",
			row["table"], row["insert"].(int64), row["update"].(int64), row["delete"].(int64), row["total"].(int64))
	}
	fmt.Fprintf(bw, "# transactions: %d, big-trx: %d, long-trx: %d, rows: %d\n",
		s.stats.TotalTrx, s.stats.BigTrxCount, s.stats.LongTrxCount, s.stats.TotalRows)
	// file is already closed above; register metadata only so result() lists it.
	s.forwardW["__stats"] = &sinkFile{path: path, name: "my2sql.stats.csv", kind: "stats"}
	s.sqlFiles++
}

func (s *parseState) close() {
	for _, w := range s.forwardW {
		if w.file != nil {
			_ = w.file.Close()
		}
	}
	for _, w := range s.rollbackW {
		if w.file != nil {
			_ = w.file.Close()
		}
	}
}

func (s *parseState) result() JobResult {
	var files []OutputFile
	add := func(w *sinkFile) {
		if w == nil {
			return
		}
		var size int64
		if fi, err := os.Stat(w.path); err == nil {
			size = fi.Size()
		}
		files = append(files, OutputFile{Name: w.name, Path: w.path, Size: size, Kind: w.kind})
	}
	for _, w := range s.forwardW {
		add(w)
	}
	for _, w := range s.rollbackW {
		add(w)
	}
	start, end := s.stats.Range()
	statsMap := map[string]any{
		"perTable":     s.stats.Snapshot(),
		"totalRows":    s.stats.TotalRows,
		"totalTrx":     s.stats.TotalTrx,
		"bigTrx":       s.stats.BigTrxCount,
		"longTrx":      s.stats.LongTrxCount,
		"parsedEvents": s.events,
	}
	if !start.IsZero() {
		statsMap["timeStart"] = start.Format(time.RFC3339)
		statsMap["timeEnd"] = end.Format(time.RFC3339)
	}
	return JobResult{OutDir: s.outDir, Files: files, Stats: statsMap, ElapsedMs: time.Since(s.startWall).Milliseconds()}
}

// ---- shared helpers ----

var errStopParse = fmt.Errorf("stop parsing")

func isStopped(err error) bool { return err == context.Canceled || err == context.DeadlineExceeded }

func rowsKind(t replication.EventType) (EventKind, bool) {
	switch t {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return KindInsert, true
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return KindUpdate, true
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return KindDelete, true
	}
	return KindInsert, false
}

func evLabel(k EventKind) string {
	switch k {
	case KindInsert:
		return "INSERT"
	case KindUpdate:
		return "UPDATE"
	default:
		return "DELETE"
	}
}

// align pads/truncates decoded values to the column count so indexing is safe
// even when a partial row image was logged.
func align(cols []string, vals []interface{}) []any {
	out := make([]any, len(cols))
	for i := range out {
		if i < len(vals) {
			out[i] = vals[i]
		}
	}
	return out
}

func splitEndpoint(ep string) (string, int) {
	host, portStr, err := net.SplitHostPort(ep)
	if err != nil {
		return ep, 3306
	}
	port, _ := strconv.Atoi(portStr)
	if port == 0 {
		port = 3306
	}
	return host, port
}

func parseMySQLTime(s string, loc *time.Location) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05", time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05.999999"} {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized datetime %q (use 'YYYY-MM-DD HH:MM:SS')", s)
}

// parseLocation accepts IANA names and +HH:MM / +HHMM offsets (my2sql -tl).
func parseLocation(s string) *time.Location {
	s = strings.TrimSpace(s)
	if s == "" || s == "UTC" || s == "+00:00" || s == "+0000" {
		return time.UTC
	}
	if loc, err := time.LoadLocation(s); err == nil {
		return loc
	}
	if loc := parseOffset(s); loc != nil {
		return loc
	}
	return time.UTC
}

func parseOffset(s string) *time.Location {
	sign := 1
	rest := s
	if strings.HasPrefix(rest, "+") {
		rest = rest[1:]
	} else if strings.HasPrefix(rest, "-") {
		sign = -1
		rest = rest[1:]
	} else {
		return nil
	}
	rest = strings.Replace(rest, ":", "", 1)
	if len(rest) < 2 {
		return nil
	}
	hh, err := strconv.Atoi(rest[:len(rest)-2])
	if err != nil {
		return nil
	}
	mm := 0
	if len(rest) > 2 {
		mm, _ = strconv.Atoi(rest[len(rest)-2:])
	}
	secs := sign * (hh*3600 + mm*60)
	name := fmt.Sprintf("UTC%s", s)
	return time.FixedZone(name, secs)
}

func isIndexFile(name string) bool { return strings.HasSuffix(name, ".index") }

// sanitize makes a filesystem-safe table identifier for per-table output names.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// binlogSortKey orders mysql-bin.NNNNNN style names numerically.
func binlogSortKey(p string) string {
	base := filepath.Base(p)
	if idx := strings.LastIndex(base, "."); idx >= 0 {
		if n, err := strconv.Atoi(base[idx+1:]); err == nil {
			return fmt.Sprintf("%020d-%s", n, base)
		}
	}
	return base
}
