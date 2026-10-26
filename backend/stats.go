package main

import (
	"sort"
	"strings"
	"time"
)

// Stats accumulates the -work-type stats view: per-table DML counts plus the
// transaction-level big/long-trx detection driven by -big-trx-row-limit and
// -long-trx-seconds.
type Stats struct {
	PerTable     map[string]*TableStat
	BigTrxCount  int
	LongTrxCount int
	TotalRows    int64
	TotalTrx     int64

	firstTrxStart time.Time
	lastTrxEnd    time.Time

	// in-flight transaction state used for big/long detection
	inTrx       bool
	curTrxStart time.Time
	curTrxRows  int
}

type TableStat struct {
	Insert int64
	Update int64
	Delete int64
}

func NewStats() *Stats {
	return &Stats{PerTable: map[string]*TableStat{}}
}

func (s *Stats) table(key string) *TableStat {
	if s.PerTable[key] == nil {
		s.PerTable[key] = &TableStat{}
	}
	return s.PerTable[key]
}

// AddRow records one DML row for a table.
func (s *Stats) AddRow(schema, table string, kind EventKind) {
	key := schema + "." + table
	st := s.table(key)
	switch kind {
	case KindInsert:
		st.Insert++
	case KindUpdate:
		st.Update++
	case KindDelete:
		st.Delete++
	}
	s.TotalRows++
}

// BeginTrx/EndTrx bracket a transaction so big/long detection can be applied.
func (s *Stats) BeginTrx(ts time.Time) { s.curTrxStart = ts; s.curTrxRows = 0; s.inTrx = true }

func (s *Stats) RowInTrx() { s.curTrxRows++ }

func (s *Stats) EndTrx(ts time.Time, bigRowLimit int, longSeconds int) {
	if !s.inTrx {
		return
	}
	s.inTrx = false
	s.TotalTrx++
	if bigRowLimit > 0 && s.curTrxRows >= bigRowLimit {
		s.BigTrxCount++
	}
	if longSeconds > 0 && !s.curTrxStart.IsZero() && ts.Sub(s.curTrxStart) >= time.Duration(longSeconds)*time.Second {
		s.LongTrxCount++
	}
	if s.firstTrxStart.IsZero() {
		s.firstTrxStart = s.curTrxStart
	}
	s.lastTrxEnd = ts
}

// Range reports the covered binlog time window, if any transaction was seen.
func (s *Stats) Range() (time.Time, time.Time) { return s.firstTrxStart, s.lastTrxEnd }

// Snapshot returns a stable, sorted slice for JSON serialization.
func (s *Stats) Snapshot() []map[string]any {
	keys := make([]string, 0, len(s.PerTable))
	for k := range s.PerTable {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		st := s.PerTable[k]
		out = append(out, map[string]any{
			"table":  k,
			"insert": st.Insert,
			"update": st.Update,
			"delete": st.Delete,
			"total":  st.Insert + st.Update + st.Delete,
		})
	}
	return out
}

// FilterMatches applies the -databases / -tables allow-lists. Empty lists mean
// "no restriction". A table entry may be a bare table name or "db.table".
func FilterMatches(databases, tables []string, schema, table string) bool {
	if len(databases) > 0 && !containsFold(databases, schema) {
		return false
	}
	if len(tables) > 0 && !containsFold(tables, table) && !containsFold(tables, schema+"."+table) {
		return false
	}
	return true
}

func containsFold(list []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, v := range list {
		if strings.EqualFold(strings.TrimSpace(v), want) {
			return true
		}
	}
	return false
}
