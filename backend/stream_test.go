package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The statement rules must stay identical between the whole-string splitter and
// the streaming one: the streaming path is what a gigabyte rollback file takes,
// and a silent difference there would execute a different set of statements
// than the confirmation dialog counted.
func TestStreamStatementsMatchesSplitStatements(t *testing.T) {
	contents := []string{
		"",
		"\n\n",
		"-- comment only\n",
		"# my2sql header\nSET NAMES\nINSERT INTO t VALUES (1);\n",
		"UPDATE t SET a=1 WHERE id=2;\nUPDATE t SET a=3 WHERE id=4;\n",
		"UPDATE t SET a=1 WHERE id=2;",
		"UPDATE t SET a=1 WHERE id=2",
		"INSERT INTO t\nVALUES\n(1,2,3);\nDELETE FROM t WHERE id=1;\n",
		"INSERT INTO t VALUES ('a;b');\n",
		"UPDATE t SET a=1 WHERE id=2;\r\nUPDATE t SET a=3 WHERE id=4;\r\n",
		"UPDATE t SET a=1 WHERE id=2;\n\n-- trailing comment\n",
	}
	for i, content := range contents {
		path := filepath.Join(t.TempDir(), "my2sql.rollback.sql")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		want := splitStatements(content)
		var got []string
		if err := streamStatements(path, func(stmt string, _ int) error {
			got = append(got, stmt)
			return nil
		}); err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if strings.Join(want, "\x00") != strings.Join(got, "\x00") {
			t.Fatalf("case %d mismatch:\n split=%q\n stream=%q", i, want, got)
		}
	}
}

// A streaming scan must be able to stop early: fn's error ends the walk, which
// is how a stopped or failing execution unwinds without reading the rest of a
// huge file.
func TestStreamStatementsStopsOnFirstError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.sql")
	if err := os.WriteFile(path, []byte("A;\nB;\nC;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var seen []string
	stop := errStopScan
	err := streamStatements(path, func(stmt string, _ int) error {
		seen = append(seen, stmt)
		if stmt == "B" {
			return stop
		}
		return nil
	})
	if err != stop {
		t.Fatalf("want the caller error back, got %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("scan did not stop at the error: %v", seen)
	}
}

// nextLine is the shared bounded reader behind both streaming paths: it must
// keep empty lines (so line numbers stay true), report a line it had to clip,
// and never lose a final line that has no trailing newline.
func TestNextLine(t *testing.T) {
	br := bufio.NewReader(strings.NewReader("abc\n\nlongtail\nno-newline"))
	type want struct {
		line      string
		truncated bool
		found     bool
	}
	wants := []want{
		{"abc", false, true},
		{"", false, true},
		{"longtail", false, true},
		{"no-newline", false, true},
		{"", false, false},
	}
	for i, w := range wants {
		line, trunc, found, err := nextLine(br, 100)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if line != w.line || trunc != w.truncated || found != w.found {
			t.Fatalf("call %d = (%q,%v,%v) want (%q,%v,%v)", i, line, trunc, found, w.line, w.truncated, w.found)
		}
	}
	// Clipping: the cap keeps memory bounded, truncated says data was dropped.
	br = bufio.NewReader(strings.NewReader("0123456789abcdef\nrest\n"))
	line, trunc, found, err := nextLine(br, 4)
	if err != nil || line != "0123" || !trunc || !found {
		t.Fatalf("capped read = (%q,%v,%v,%v) want (0123,true,true,nil)", line, trunc, found, err)
	}
	if line, _, found, _ = nextLine(br, 100); line != "rest" || !found {
		t.Fatalf("line after a clipped one = (%q,%v) want (rest,true)", line, found)
	}
	// A line longer than the internal buffer is reassembled, not split.
	big := strings.Repeat("x", 300<<10)
	br = bufio.NewReaderSize(strings.NewReader(big+"\ny\n"), 4<<10)
	line, trunc, found, err = nextLine(br, 1<<20)
	if err != nil || len(line) != len(big) || trunc || !found {
		t.Fatalf("long line: len=%d truncated=%v found=%v err=%v", len(line), trunc, found, err)
	}
	if line, _, found, _ = nextLine(br, 100); line != "y" || !found {
		t.Fatalf("line after a long one = (%q,%v) want (y,true)", line, found)
	}
}

func TestScanLineWindow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.sql")
	var b strings.Builder
	for i := 0; i < 1000; i++ {
		b.WriteString("statement line\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	lines, total, complete, err := scanLineWindow(path, 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 500 || total != 500 || complete {
		t.Fatalf("window=%d total=%d complete=%v want 500/500/false", len(lines), total, complete)
	}
	if lines[0] != "statement line" || lines[499] != "statement line" {
		t.Fatalf("unexpected window content: %q", lines[:2])
	}
	// A file that ends exactly at the window edge is not truncated.
	exact := filepath.Join(dir, "exact.sql")
	if err := os.WriteFile(exact, []byte("a\nb\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lines, total, complete, err = scanLineWindow(exact, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 || !complete || total != 2 {
		t.Fatalf("window=%d total=%d complete=%v want 2/2/true", len(lines), total, complete)
	}
	// Offset past the end yields an empty window, not a panic.
	lines, total, complete, err = scanLineWindow(exact, 50, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 0 || !complete || total != 2 {
		t.Fatalf("offset beyond EOF: window=%d total=%d complete=%v", len(lines), total, complete)
	}
}

// Regression for the sidecar restart that surfaced as "unknown job": the preview
// read the whole output file, and a real analysis writes gigabytes.
func TestPreviewStreamsLargeOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DBX_PLUGIN_DATA_DIR", dir)
	m := NewJobManager(writingEngine{})
	doneCh, pub := newDoneChannel()
	id, err := m.Start(AnalysisOptions{Mode: "file"}, &ConnectionSpec{Host: "127.0.0.1", Port: 3306}, pub)
	if err != nil {
		t.Fatal(err)
	}
	collectDone(t, doneCh)
	jobDir := filepath.Join(dir, "jobs", id)
	// 200k lines: small enough for a test, far past the old 4 KB buffer, and the
	// shape the real rollback files have.
	var b strings.Builder
	for i := 0; i < 200000; i++ {
		b.WriteString("UPDATE t SET a=1 WHERE id=2;\n")
	}
	if err := os.WriteFile(filepath.Join(jobDir, "my2sql.rollback.sql"), []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := m.Preview(id, "my2sql.rollback.sql", 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(p["lines"].([]string)); got != 500 {
		t.Fatalf("preview lines=%d want 500", got)
	}
	if p["truncated"] != true {
		t.Fatal("a preview that stopped early must be reported as truncated")
	}
	if total := p["total"].(int); total != -1 {
		t.Fatalf("total=%v want -1 (not counted, the file is bigger than the window)", p["total"])
	}
	// A small file still reports its exact line count.
	if err := os.WriteFile(filepath.Join(jobDir, "small.sql"), []byte("a\nb\nc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err = m.Preview(id, "small.sql", 1, 500)
	if err != nil {
		t.Fatal(err)
	}
	if total := p["total"].(int); total != 3 {
		t.Fatalf("total=%v want 3", p["total"])
	}
	if lines := p["lines"].([]string); len(lines) != 2 || lines[0] != "b" {
		t.Fatalf("offset window wrong: %q", lines)
	}
}

var errStopScan = &scanStop{}

type scanStop struct{}

func (*scanStop) Error() string { return "stopped" }
