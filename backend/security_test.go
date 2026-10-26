package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- jobId path traversal (review finding: jobOutputDir joined unsanitized ids)

func TestJobOutputDirRejectsTraversalIDs(t *testing.T) {
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	for _, id := range []string{
		"", ".", "..", "../etc", "../../../../../etc",
		"a/b", "a\\b", "job/../../x", "job with space",
		strings.Repeat("x", 65), // over length cap
		"\x00evil",
	} {
		if _, err := jobOutputDir(id); err == nil {
			t.Errorf("jobOutputDir(%q) accepted a hostile id", id)
		}
	}
	// The shape nextID() produces must stay valid.
	for _, id := range []string{"20261026T081200-1", "job-42", "ABC_-9"} {
		if _, err := jobOutputDir(id); err != nil {
			t.Errorf("jobOutputDir(%q) rejected a valid id: %v", id, err)
		}
	}
}

// The reviewer's exact reproduction: preview a file with jobId climbing out of
// jobs/ must fail, not return /etc/hosts.
func TestPreviewRejectsTraversalJobID(t *testing.T) {
	base := t.TempDir()
	t.Setenv("DBX_PLUGIN_DATA_DIR", base)
	m := NewJobManager(&pendingEngine{})
	if _, err := m.Preview("../../../../../../etc", "hosts", 0, 10); err == nil {
		t.Fatal("Preview accepted a traversal jobId")
	}
	if _, err := m.ListOutputs("../../.."); err == nil {
		t.Fatal("ListOutputs accepted a traversal jobId")
	}
}

// --- exec confirm token (review finding: confirmation gate was UI-only)

func setupExecTest(t *testing.T) (*ExecManager, string) {
	t.Helper()
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	sessions := NewSessionStore()
	// Unreachable endpoint: Start may still return an execution id because the
	// dial happens on the goroutine, which is exactly what the token gates.
	sessions.Put(&ConnectionSpec{ID: "target-1", Host: "127.0.0.1", Port: 1, Username: "u"})
	m := NewExecManager(sessions, NewJobManager(&pendingEngine{}))
	dir, err := jobOutputDir("job-exec-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rollback.sql"), []byte("DELETE FROM t WHERE id=1;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return m, "job-exec-1"
}

func execPrepareToken(t *testing.T, m *ExecManager, jobID string) string {
	t.Helper()
	res, err := m.Prepare(jobID, "rollback.sql", "target-1")
	if err != nil {
		t.Fatal(err)
	}
	token, _ := res["confirmToken"].(string)
	if token == "" {
		t.Fatal("Prepare did not return a confirmToken")
	}
	return token
}

func TestExecStartRequiresToken(t *testing.T) {
	m, jobID := setupExecTest(t)

	if _, err := m.Start(jobID, "rollback.sql", "target-1", "", nil); err == nil {
		t.Fatal("Start without confirmToken executed")
	}
	if _, err := m.Start(jobID, "rollback.sql", "target-1", "deadbeef", nil); err == nil {
		t.Fatal("Start with an unknown token executed")
	}
}

func TestExecTokenIsOneShotAndBound(t *testing.T) {
	m, jobID := setupExecTest(t)
	token := execPrepareToken(t, m, jobID)

	// Wrong triple: same token, different file / target / job.
	if _, err := m.Start(jobID, "other.sql", "target-1", token, nil); err == nil {
		t.Fatal("token accepted for a different file")
	}
	// Consuming attempt above already spent it (delete-on-first-attempt), so
	// even the correct triple must now fail.
	if _, err := m.Start(jobID, "rollback.sql", "target-1", token, nil); err == nil {
		t.Fatal("token was reusable after a consumed attempt")
	}

	// Fresh prepare, correct triple: accepted.
	token2 := execPrepareToken(t, m, jobID)
	if _, err := m.Start(jobID, "rollback.sql", "target-1", token2, nil); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
	// One-shot: second use of the same token fails.
	if _, err := m.Start(jobID, "rollback.sql", "target-1", token2, nil); err == nil {
		t.Fatal("token survived after successful execution")
	}
}

func TestExecPrepareRejectsTraversal(t *testing.T) {
	m, jobID := setupExecTest(t)
	if _, err := m.Prepare("../../..", "rollback.sql", "target-1"); err == nil {
		t.Fatal("Prepare accepted a traversal jobId")
	}
	if _, err := m.Prepare(jobID, "../../etc/hosts", "target-1"); err == nil {
		t.Fatal("Prepare accepted a traversal file name")
	}
}

// --- identifier quoting (review suggestion: backticks must double)

func TestQuoteIdentDoublesBackticks(t *testing.T) {
	if got := quoteIdent("we`ird"); got != "`we``ird`" {
		t.Errorf("quoteIdent(we`ird) = %q", got)
	}
	if got := quoteIdent("plain"); got != "`plain`" {
		t.Errorf("quoteIdent(plain) = %q", got)
	}
}

// --- TLS mode mapping

func TestTLSConfigMapping(t *testing.T) {
	cases := []struct {
		mode    string
		wantNil bool
		wantErr bool
		skip    bool
		server  string
	}{
		{mode: "", wantNil: true},
		{mode: "off", wantNil: true},
		{mode: "require", skip: true},
		{mode: "verify", server: "db.example.com"},
		{mode: "bogus", wantErr: true},
	}
	for _, tc := range cases {
		spec := &ConnectionSpec{Host: "db.example.com", Tls: tc.mode}
		cfg, err := spec.TLSConfig()
		if tc.wantErr {
			if err == nil {
				t.Errorf("TLSConfig(%q) expected error", tc.mode)
			}
			continue
		}
		if err != nil {
			t.Errorf("TLSConfig(%q): unexpected error %v", tc.mode, err)
			continue
		}
		if tc.wantNil {
			if cfg != nil {
				t.Errorf("TLSConfig(%q) expected nil, got %+v", tc.mode, cfg)
			}
			continue
		}
		if cfg == nil {
			t.Fatalf("TLSConfig(%q) expected config", tc.mode)
		}
		if cfg.InsecureSkipVerify != tc.skip {
			t.Errorf("TLSConfig(%q) InsecureSkipVerify=%v want %v", tc.mode, cfg.InsecureSkipVerify, tc.skip)
		}
		if tc.server != "" && cfg.ServerName != tc.server {
			t.Errorf("TLSConfig(%q) ServerName=%q want %q", tc.mode, cfg.ServerName, tc.server)
		}
	}
}
