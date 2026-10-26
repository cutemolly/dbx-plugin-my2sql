//go:build my2sql

package main

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// End-to-end flashback validation against a real MySQL. It is the counterpart to
// the offline sqlgen unit tests: here a committed DELETE is captured from the
// live binlog by the real engine, turned into rollback SQL, and executed to
// restore the row.
//
// Prerequisites (skipped automatically when unset):
//
//	MY2SQL_TEST_DSN     go-sql-driver DSN, e.g. root:pass@tcp(127.0.0.1:3306)/
//	MY2SQL_TEST_SCHEMA  an existing schema the user may create tables in (default my2sql_it)
//	MY2SQL_TEST_SERVER_ID  optional fake-replica server id (default 999383)
//
// The MySQL must run with log_bin=ON, binlog_format=ROW, binlog_row_image=FULL and
// the user needs REPLICATION CLIENT + REPLICATION SLAVE (mysql 8: also
// REPLICATION_APPLIER or SUPER) plus CREATE/INSERT/DELETE on the test schema.
//
// Run with:  go test -tags my2sql -run TestMy2sqlEndToEnd -v ./...
func TestMy2sqlEndToEnd(t *testing.T) {
	dsn := os.Getenv("MY2SQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MY2SQL_TEST_DSN not set; skipping live binlog flashback test")
	}
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	schema := os.Getenv("MY2SQL_TEST_SCHEMA")
	if schema == "" {
		schema = "my2sql_it"
	}
	serverID := 999383
	if v := os.Getenv("MY2SQL_TEST_SERVER_ID"); v != "" {
		if n := atoiStrict(v); n > 0 {
			serverID = n
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Guard on the flashback preconditions.
	host, port := splitEndpoint(cfg.Addr)
	spec := &ConnectionSpec{ID: "it", Host: host, Port: port, Username: cfg.User, Password: cfg.Passwd, ServerID: serverID, Timezone: "+00:00"}
	pre, err := Precheck(context.Background(), spec)
	if err != nil {
		t.Fatalf("precheck: %v", err)
	}
	if ok, _ := pre["ok"].(bool); !ok {
		t.Skipf("source not flashback-ready: %v", pre["problems"])
	}

	table := "flashback_t"
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+schema+"`"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS `"+schema+"`.`"+table+"` (id INT PRIMARY KEY, name VARCHAR(64))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS `" + schema + "`.`" + table + "`") //nolint:errcheck
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE `"+schema+"`.`"+table+"`"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO `"+schema+"`.`"+table+"` (id,name) VALUES (1,'a'),(2,'b'),(3,'c')"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Mark the binlog position, then delete the row we intend to bring back.
	file, pos := masterStatus(t, db)
	if file == "" {
		t.Fatal("SHOW MASTER STATUS returned no binlog file")
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM `"+schema+"`.`"+table+"` WHERE id = 2"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	var remaining int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+schema+"`.`"+table+"`").Scan(&remaining)
	if remaining != 2 {
		t.Fatalf("setup: expected 2 rows after delete, got %d", remaining)
	}

	// Parse from the marked position; the committed DELETE is already in the
	// binlog, so the engine replays it then blocks until the timeout cancels it.
	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outDir := t.TempDir()
	opts := AnalysisOptions{
		SourceConnectionID: "it", Mode: "repl", WorkType: "rollback",
		StartFile: file, StartPos: int64(pos),
		Databases: []string{schema}, Tables: []string{table},
	}
	res, err := newMy2sqlEngine().Run(runCtx, spec, opts, outDir, nil)
	if err != nil {
		t.Fatalf("engine run: %v", err)
	}

	rbPath := ""
	for _, f := range res.Files {
		if f.Kind == "rollback" {
			rbPath = f.Path
		}
	}
	if rbPath == "" {
		t.Fatalf("no rollback file produced; files=%+v stats=%+v", res.Files, res.Stats)
	}
	body, err := os.ReadFile(rbPath)
	if err != nil {
		t.Fatalf("read rollback: %v", err)
	}
	rollbackSQL := string(body)
	t.Logf("rollback file:\n%s", rollbackSQL)
	if !strings.Contains(strings.ToUpper(rollbackSQL), "INSERT") {
		t.Fatalf("expected the flashback of a DELETE to be an INSERT, got:\n%s", rollbackSQL)
	}

	// Apply the flashback statements and assert the row is restored.
	for _, stmt := range splitStatements(rollbackSQL) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec rollback %q: %v", stmt, err)
		}
	}
	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM `"+schema+"`.`"+table+"` WHERE id = 2").Scan(&name); err != nil {
		t.Fatalf("row 2 not restored: %v", err)
	}
	if name != "b" {
		t.Fatalf("restored value mismatch: got %q want %q", name, "b")
	}
}

// masterStatus reads the current binlog file + position.
func masterStatus(t *testing.T, db *sql.DB) (string, uint32) {
	t.Helper()
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		t.Fatalf("show master status: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return "", 0
	}
	cols, _ := rows.Columns()
	holders := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range dest {
		dest[i] = &holders[i]
	}
	if err := rows.Scan(dest...); err != nil {
		t.Fatalf("scan master status: %v", err)
	}
	if len(holders) < 2 {
		return "", 0
	}
	return holders[0].String, uint32(atoiStrict(holders[1].String))
}

func atoiStrict(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return n
		}
		n = n*10 + int(r-'0')
	}
	return n
}
