package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// openDB opens a short-lived pool to the resolved endpoint. my2sql issues
// administrative queries (SHOW BINARY LOGS, INFORMATION_SCHEMA) rather than
// holding long sessions, so a per-request pool with a bounded dial timeout is
// the simplest correct shape here.
func openDB(ctx context.Context, spec *ConnectionSpec) (*sql.DB, error) {
	cfg := mysqldriver.NewConfig()
	cfg.User = spec.Username
	cfg.Passwd = spec.Password
	cfg.Net = "tcp"
	cfg.Addr = spec.Endpoint()
	cfg.Timeout = 10 * time.Second
	cfg.ReadTimeout = 30 * time.Second
	cfg.WriteTimeout = 30 * time.Second
	cfg.Params = map[string]string{"time_zone": "'+00:00'"}
	tlsCfg, err := spec.TLSConfig()
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		// go-sql-driver resolves TLS by registered name; scope one entry per
		// connection so concurrent opens of different specs cannot overwrite
		// each other's mode.
		tlsName := "my2sql-" + spec.ID
		if err := mysqldriver.RegisterTLSConfig(tlsName, tlsCfg); err != nil {
			return nil, err
		}
		cfg.TLSConfig = tlsName
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetConnMaxLifetime(2 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Precheck reports whether the source can be parsed for rollback. my2sql can
// only flash back DML when binlog_format=row and binlog_row_image=full and the
// server has binlog enabled.
func Precheck(ctx context.Context, spec *ConnectionSpec) (map[string]any, error) {
	db, err := openDB(ctx, spec)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	vars, err := showVariables(ctx, db, "log_bin", "binlog_format", "binlog_row_image", "version", "version_comment")
	if err != nil {
		return nil, err
	}

	logBin := vars["log_bin"]
	format := vars["binlog_format"]
	image := vars["binlog_row_image"]

	problems := []string{}
	warnings := []string{}
	if fmt.Sprint(logBin) != "ON" {
		problems = append(problems, "log_bin is OFF — the server is not producing a binlog to parse")
	}
	if fmt.Sprint(format) != "ROW" {
		problems = append(problems, fmt.Sprintf("binlog_format=%v — rollback/flashback requires ROW", format))
	}
	if fmt.Sprint(image) != "FULL" {
		warnings = append(warnings, fmt.Sprintf("binlog_row_image=%v — FULL is required for reliable before-image rollback", image))
	}

	return map[string]any{
		"endpoint": spec.Endpoint(),
		"version":  vars["version"],
		"logBin":   logBin,
		"format":   format,
		"rowImage": image,
		"ok":       len(problems) == 0,
		"problems": problems,
		"warnings": warnings,
	}, nil
}

func showVariables(ctx context.Context, db *sql.DB, names ...string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW VARIABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[n] = true
	}
	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		if want[k] {
			out[k] = v
		}
	}
	return out, rows.Err()
}

// ListBinlogFiles returns SHOW BINARY LOGS plus the current SHOW MASTER STATUS.
func ListBinlogFiles(ctx context.Context, spec *ConnectionSpec) (map[string]any, error) {
	db, err := openDB(ctx, spec)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	files := []map[string]any{}
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("show binary logs (does the user have REPLICATION CLIENT?): %w", err)
	}
	defer rows.Close()
	// Column layout is (Log_name, File_size[, Encrypted]); scan dynamically.
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		vals := make([]any, len(cols))
		holders := make([]sql.NullString, len(cols))
		for i := range vals {
			vals[i] = &holders[i]
		}
		if err := rows.Scan(vals...); err != nil {
			return nil, err
		}
		name := holders[0].String
		var size int64
		if len(cols) > 1 && holders[1].Valid {
			size, _ = strconv.ParseInt(holders[1].String, 10, 64)
		}
		files = append(files, map[string]any{"name": name, "size": size})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	status := map[string]any{}
	if srow, err := db.QueryContext(ctx, "SHOW MASTER STATUS"); err == nil {
		defer srow.Close()
		if scols, err := srow.Columns(); err == nil && srow.Next() {
			vals := make([]any, len(scols))
			holders := make([]sql.NullString, len(scols))
			for i := range vals {
				vals[i] = &holders[i]
			}
			if err := srow.Scan(vals...); err == nil && len(holders) >= 2 {
				status["file"] = holders[0].String
				status["position"] = holders[1].String
			}
		}
	}

	return map[string]any{"files": files, "status": status}, nil
}

// ListDatabases returns schemas visible to the replication user.
func ListDatabases(ctx context.Context, spec *ConnectionSpec) ([]string, error) {
	db, err := openDB(ctx, spec)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		"SELECT schema_name FROM information_schema.schemata "+
			"WHERE schema_name NOT IN ('information_schema','performance_schema','sys','mysql') "+
			"ORDER BY schema_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// ListTables returns base tables in one database.
func ListTables(ctx context.Context, spec *ConnectionSpec, database string) ([]string, error) {
	if database == "" {
		return nil, fmt.Errorf("missing database")
	}
	db, err := openDB(ctx, spec)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
		database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// SchemaColumns resolves the ordered column names plus primary-key and unique-
// key column sets for one table. The binlog table_map event carries types but
// not names, so the engine reads names from INFORMATION_SCHEMA to build SQL.
func SchemaColumns(ctx context.Context, spec *ConnectionSpec, database, table string) (*TableSchema, error) {
	if database == "" || table == "" {
		return nil, fmt.Errorf("missing database or table")
	}
	db, err := openDB(ctx, spec)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	sch := &TableSchema{Schema: database, Table: table, Columns: []string{}}

	crows, err := db.QueryContext(ctx,
		"SELECT column_name, column_type, is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
		database, table)
	if err != nil {
		return nil, err
	}
	defer crows.Close()
	for crows.Next() {
		var c, colType, nullable string
		if err := crows.Scan(&c, &colType, &nullable); err != nil {
			return nil, err
		}
		sch.Columns = append(sch.Columns, c)
		sch.ColumnTypes = append(sch.ColumnTypes, colType)
		sch.Nullable = append(sch.Nullable, strings.EqualFold(nullable, "YES"))
	}
	if err := crows.Err(); err != nil {
		return nil, err
	}
	if len(sch.Columns) == 0 {
		return nil, fmt.Errorf("table %s.%s not found in information_schema", database, table)
	}

	// Primary key columns, in key_seq order.
	pk, err := db.QueryContext(ctx,
		"SELECT column_name FROM information_schema.key_column_usage "+
			"WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' ORDER BY ordinal_position",
		database, table)
	if err == nil {
		defer pk.Close()
		for pk.Next() {
			var c string
			if err := pk.Scan(&c); err == nil {
				sch.PrimaryKey = append(sch.PrimaryKey, c)
			}
		}
	}

	// Unique keys (non-primary), grouped by constraint name.
	uk, err := db.QueryContext(ctx,
		"SELECT constraint_name, column_name FROM information_schema.key_column_usage "+
			"WHERE table_schema = ? AND table_name = ? AND constraint_name <> 'PRIMARY' AND constraint_name IN "+
			"(SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = ? AND table_name = ? AND constraint_type = 'UNIQUE') "+
			"ORDER BY constraint_name, ordinal_position",
		database, table, database, table)
	if err == nil {
		defer uk.Close()
		current := ""
		group := []string{}
		for uk.Next() {
			var name, col string
			if err := uk.Scan(&name, &col); err != nil {
				continue
			}
			if name != current {
				if current != "" && len(group) > 0 {
					sch.UniqueKeys = append(sch.UniqueKeys, group)
				}
				current = name
				group = []string{col}
			} else {
				group = append(group, col)
			}
		}
		if current != "" && len(group) > 0 {
			sch.UniqueKeys = append(sch.UniqueKeys, group)
		}
	}

	return sch, nil
}
