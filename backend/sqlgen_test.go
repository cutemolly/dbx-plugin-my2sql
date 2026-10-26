package main

import (
	"testing"
	"time"
)

func ts() *TableSchema {
	return &TableSchema{
		Schema:     "app",
		Table:      "orders",
		Columns:    []string{"id", "status", "amount", "note"},
		PrimaryKey: []string{"id"},
		UniqueKeys: [][]string{{"status", "note"}},
	}
}

func TestGenerateInsertAndRollback(t *testing.T) {
	sch := ts()
	after := []any{int64(7), "paid", 12.5, "hi"}
	fwd, rb := sch.Generate(KindInsert, Pair{After: after}, GenOptions{})
	wantFwd := "INSERT INTO `app`.`orders` (`id`, `status`, `amount`, `note`) VALUES (7, 'paid', 12.5, 'hi');"
	wantRb := "DELETE FROM `app`.`orders` WHERE `id` = 7 LIMIT 1;"
	if fwd != wantFwd {
		t.Fatalf("insert fwd:\n got %s\nwant %s", fwd, wantFwd)
	}
	if rb != wantRb {
		t.Fatalf("insert rb:\n got %s\nwant %s", rb, wantRb)
	}
}

func TestGenerateDeleteRollbackIsInsert(t *testing.T) {
	sch := ts()
	before := []any{int64(3), "new", float64(1), nil}
	fwd, rb := sch.Generate(KindDelete, Pair{Before: before}, GenOptions{})
	wantFwd := "DELETE FROM `app`.`orders` WHERE `id` = 3 LIMIT 1;"
	wantRb := "INSERT INTO `app`.`orders` (`id`, `status`, `amount`, `note`) VALUES (3, 'new', 1, NULL);"
	if fwd != wantFwd {
		t.Fatalf("delete fwd:\n got %s\nwant %s", fwd, wantFwd)
	}
	if rb != wantRb {
		t.Fatalf("delete rb:\n got %s\nwant %s", rb, wantRb)
	}
}

func TestGenerateUpdateChangedOnlyAndRollback(t *testing.T) {
	sch := ts()
	before := []any{int64(9), "new", 5.0, "a"}
	after := []any{int64(9), "paid", 5.0, "a"} // only status changed
	fwd, rb := sch.Generate(KindUpdate, Pair{Before: before, After: after}, GenOptions{})
	wantFwd := "UPDATE `app`.`orders` SET `status` = 'paid' WHERE `id` = 9 LIMIT 1;"
	wantRb := "UPDATE `app`.`orders` SET `status` = 'new' WHERE `id` = 9 LIMIT 1;"
	if fwd != wantFwd {
		t.Fatalf("update fwd:\n got %s\nwant %s", fwd, wantFwd)
	}
	if rb != wantRb {
		t.Fatalf("update rb:\n got %s\nwant %s", rb, wantRb)
	}
}

func TestUpdateFullColumns(t *testing.T) {
	sch := ts()
	before := []any{int64(9), "new", 5.0, "a"}
	after := []any{int64(9), "paid", 5.0, "a"}
	fwd, _ := sch.Generate(KindUpdate, Pair{Before: before, After: after}, GenOptions{FullColumns: true})
	want := "UPDATE `app`.`orders` SET `id` = 9, `status` = 'paid', `amount` = 5, `note` = 'a' WHERE `id` = 9 LIMIT 1;"
	if fwd != want {
		t.Fatalf("full update:\n got %s\nwant %s", fwd, want)
	}
}

func TestKeyColumnsNoPrimaryKeyUsesAllColumns(t *testing.T) {
	sch := &TableSchema{Schema: "app", Table: "logs", Columns: []string{"a", "b"}}
	// no PK, no unique -> WHERE uses every column
	fwd, _ := sch.Generate(KindDelete, Pair{Before: []any{int64(1), "x"}}, GenOptions{})
	want := "DELETE FROM `app`.`logs` WHERE `a` = 1 AND `b` = 'x' LIMIT 1;"
	if fwd != want {
		t.Fatalf("no-key delete:\n got %s\nwant %s", fwd, want)
	}
}

func TestUseUniqueKeySelectsSmallestKey(t *testing.T) {
	sch := &TableSchema{
		Schema: "app", Table: "t", Columns: []string{"id", "code", "name"},
		PrimaryKey: []string{"id"},
		UniqueKeys: [][]string{{"code", "name"}, {"code"}},
	}
	fwd, _ := sch.Generate(KindDelete, Pair{Before: []any{int64(1), "C", "N"}}, GenOptions{UseUniqueKey: true})
	want := "DELETE FROM `app`.`t` WHERE `code` = 'C' LIMIT 1;"
	if fwd != want {
		t.Fatalf("unique key delete:\n got %s\nwant %s", fwd, want)
	}
}

func TestDoNotPrefixDBAndTypeFilter(t *testing.T) {
	sch := ts()
	after := []any{int64(1), "p", 1.0, "n"}
	fwd, _ := sch.Generate(KindInsert, Pair{After: after}, GenOptions{DoNotPrefixDB: true})
	if want := "INSERT INTO `orders`"; len(fwd) < len(want) || fwd[:len(want)] != want {
		t.Fatalf("unprefixed insert should start with %q, got %s", want, fwd)
	}
	// delete filtered out when only insert requested
	fwd, rb := sch.Generate(KindDelete, Pair{Before: after}, GenOptions{OnlyTypes: []string{"insert"}})
	if fwd != "" || rb != "" {
		t.Fatalf("filtered delete should be empty, got fwd=%q rb=%q", fwd, rb)
	}
}

func TestIgnorePrimaryKeyForInsertSkipsZeroAutoInc(t *testing.T) {
	sch := ts()
	after := []any{int64(0), "p", 1.0, "n"}
	fwd, _ := sch.Generate(KindInsert, Pair{After: after}, GenOptions{IgnorePKForInsert: true})
	want := "INSERT INTO `app`.`orders` (`status`, `amount`, `note`) VALUES ('p', 1, 'n');"
	if fwd != want {
		t.Fatalf("ignore pk insert:\n got %s\nwant %s", fwd, want)
	}
}

func TestValueFormatting(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, "NULL"},
		{true, "1"},
		{false, "0"},
		{int64(-5), "-5"},
		{uint8(200), "200"},
		{"it's", "'it''s'"},
		{"a\\b", "'a\\\\b'"},
		{"line\nbr", "'line\\nbr'"},
		{[]byte{0x01, 0xff}, "0x01FF"},
		{time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC), "'2026-03-04 05:06:07'"},
	}
	for _, c := range cases {
		if got := FormatValue(c.in); got != c.want {
			t.Errorf("FormatValue(%#v) = %s want %s", c.in, got, c.want)
		}
	}
}

func TestFilterMatches(t *testing.T) {
	if !FilterMatches(nil, nil, "any", "tbl") {
		t.Fatal("empty filters must match everything")
	}
	if !FilterMatches([]string{"app"}, nil, "app", "t") {
		t.Fatal("db filter should match")
	}
	if FilterMatches([]string{"other"}, nil, "app", "t") {
		t.Fatal("db mismatch should not match")
	}
	if !FilterMatches(nil, []string{"app.orders"}, "app", "orders") {
		t.Fatal("qualified table entry should match")
	}
	if !FilterMatches(nil, []string{"Orders"}, "app", "orders") {
		t.Fatal("table match must be case-insensitive")
	}
}

func TestStatsBigAndLongTrx(t *testing.T) {
	s := NewStats()
	t0 := time.Unix(1000, 0)
	s.BeginTrx(t0)
	for i := 0; i < 5; i++ {
		s.RowInTrx()
		s.AddRow("app", "orders", KindInsert)
	}
	s.EndTrx(t0.Add(30*time.Second), 5, 10) // big (>=5 rows) and long (>=10s)
	if s.BigTrxCount != 1 || s.LongTrxCount != 1 {
		t.Fatalf("expected big=1 long=1 got big=%d long=%d", s.BigTrxCount, s.LongTrxCount)
	}
	if s.TotalRows != 5 || s.TotalTrx != 1 {
		t.Fatalf("totals wrong: rows=%d trx=%d", s.TotalRows, s.TotalTrx)
	}
	snap := s.Snapshot()
	if len(snap) != 1 || snap[0]["table"] != "app.orders" || snap[0]["insert"].(int64) != 5 {
		t.Fatalf("snapshot wrong: %#v", snap)
	}
}
