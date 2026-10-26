//go:build my2sql

package main

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

func zoneOffset(loc *time.Location) int {
	_, offset := time.Unix(0, 0).In(loc).Zone()
	return offset
}

// Regression for the "user-picked window never matches" bug: with no explicit
// -timezone the engine must interpret event timestamps and datetime windows in
// the SOURCE CONNECTION's timezone, not UTC.
func TestAnalysisLocFallsBackToConnectionTimezone(t *testing.T) {
	st := &parseState{spec: &ConnectionSpec{Timezone: "+08:00"}, opts: AnalysisOptions{}}
	if got := zoneOffset(st.analysisLoc()); got != 8*3600 {
		t.Fatalf("connection timezone ignored: offset=%d want 28800", got)
	}

	st.opts.Timezone = "UTC"
	if got := zoneOffset(st.analysisLoc()); got != 0 {
		t.Fatalf("explicit -timezone must win: offset=%d", got)
	}

	st.opts.Timezone = ""
	st.spec.Timezone = ""
	if got := zoneOffset(st.analysisLoc()); got != 0 {
		t.Fatalf("empty everywhere should fall back to UTC: offset=%d", got)
	}
}

func TestEventTimeUsesAnalysisZone(t *testing.T) {
	st := &parseState{spec: &ConnectionSpec{Timezone: "+08:00"}}
	tm := st.eventTime(1760000000)
	if _, offset := tm.Zone(); offset != 8*3600 {
		t.Fatalf("eventTime zone offset=%d want 28800", offset)
	}
}

// The JSONL protocol reserves stdout; go-mysql's default logger prints there.
// The syncer must be handed a stderr logger or "[info] binlogsyncer..." lines
// desynchronize the host's frame scanner (found via the e2e stop drill).
func TestSyncerLoggerNeverTouchesStdout(t *testing.T) {
	real := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = real }()

	cfg := replication.BinlogSyncerConfig{ServerID: 1, Host: "h", Port: 3306, Logger: syncerLogger()}
	syncer := replication.NewBinlogSyncer(cfg)
	syncer.Close()

	w.Close()
	os.Stdout = real
	captured, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != 0 {
		t.Fatalf("protocol stdout polluted with %d log bytes: %q", len(captured), captured)
	}
}

// Regression for "online analysis never terminates after the stop time": the
// old per-row stopAt check sat *after* the db/table filter, so once the matched
// tables went idle past the window, non-matching tables and query/rotate events
// kept the live stream alive forever. Termination must now happen on any
// data-bearing event past stopAt, independent of the filter.
func TestOnlineTerminatesPastStopTimeEvenWhenFiltered(t *testing.T) {
	stop := time.Unix(1760000000, 0).UTC()
	st := &parseState{
		ctx:     context.Background(),
		spec:    &ConnectionSpec{Timezone: "UTC"},
		opts:    AnalysisOptions{Databases: []string{"wanted"}},
		startAt: stop.Add(-time.Hour),
		stopAt:  stop,
		hasTime: true,
	}
	ev := &replication.BinlogEvent{
		Header: &replication.EventHeader{Timestamp: uint32(stop.Add(time.Second).Unix()), EventType: replication.WRITE_ROWS_EVENTv2},
		Event:  &replication.RowsEvent{Table: &replication.TableMapEvent{Schema: []byte("other"), Table: []byte("t")}},
	}
	done, err := st.handleEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("expected termination past stopAt even for a filtered-out table")
	}
}

// The dump handshake's fake ROTATE_EVENT carries the server's current time, so
// a naive global stopAt check would kill an online job before it reads any
// in-window data whenever the requested window sits in the past. Control events
// must never trigger time-based termination.
func TestControlEventsDoNotTerminateOnlineJob(t *testing.T) {
	stop := time.Unix(1760000000, 0).UTC()
	st := &parseState{
		ctx:     context.Background(),
		spec:    &ConnectionSpec{Timezone: "UTC"},
		opts:    AnalysisOptions{},
		startAt: stop.Add(-time.Hour),
		stopAt:  stop,
		hasTime: true,
	}
	ev := &replication.BinlogEvent{
		Header: &replication.EventHeader{Timestamp: uint32(stop.Add(time.Hour).Unix()), EventType: replication.ROTATE_EVENT},
		Event:  &replication.RotateEvent{NextLogName: []byte("mysql-bin.000002")},
	}
	if done, err := st.handleEvent(ev); err != nil || done {
		t.Fatalf("rotate event must not trigger time termination: stop=%v err=%v", done, err)
	}
}

// Regression for "a set stop time still needs a manual stop": an online stream
// that goes live-idle must end on its own once the wall clock passes stopAt
// (no future event can fall inside a closed window), but must keep waiting when
// the window is still open or when no stop time was given.
func TestIdlePollPastWindow(t *testing.T) {
	if st := (&parseState{}); st.idlePollPastWindow() {
		t.Fatal("zero stopAt (unbounded capture) must never self-end")
	}
	past := &parseState{stopAt: time.Now().Add(-time.Minute)}
	if !past.idlePollPastWindow() {
		t.Fatal("wall clock past a set stop time must end the idle scan")
	}
	future := &parseState{stopAt: time.Now().Add(time.Hour)}
	if future.idlePollPastWindow() {
		t.Fatal("window still open must keep waiting")
	}
}
