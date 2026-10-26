//go:build !my2sql

package main

// Default build selects the placeholder engine so the sidecar compiles and
// packages with only the standard library + go-sql-driver (no network needed to
// fetch the binlog parsing kernel). Building with `-tags my2sql` links the real
// go-mysql based engine in engine_my2sql.go instead.
func engineFor() Engine { return pendingEngine{} }
