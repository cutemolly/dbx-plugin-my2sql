package main

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// EventKind is the DML operation a decoded row event represents.
type EventKind int

const (
	KindInsert EventKind = iota
	KindUpdate
	KindDelete
)

// TableSchema is the column layout my2sql needs to turn a decoded row image
// into SQL. The binlog table_map event carries column *types* but not column
// *names*, so the engine resolves names + keys from INFORMATION_SCHEMA (see
// mysql.go SchemaColumns) and passes them here.
type TableSchema struct {
	Schema     string     `json:"schema"`
	Table      string     `json:"table"`
	Columns    []string   `json:"columns"`    // ordered, matches row-image value positions
	PrimaryKey []string   `json:"primaryKey"` // column names forming the primary key (may be empty)
	UniqueKeys [][]string `json:"uniqueKeys"` // secondary unique keys, used for WHERE when -U is set

	// Display-only metadata parallel to Columns; never affects SQL generation.
	// Populated from INFORMATION_SCHEMA for the workbench column preview.
	ColumnTypes []string `json:"columnTypes,omitempty"` // e.g. "varchar(64)", "bigint"
	Nullable    []bool   `json:"nullable,omitempty"`    // is_nullable == YES
}

// GenOptions mirrors the SQL-shaping my2sql flags that affect emitted text.
type GenOptions struct {
	IgnorePKForInsert bool     // -ignorePrimaryKeyForInsert
	FullColumns       bool     // -full-columns (SET all columns on UPDATE, WHERE all on DELETE)
	UseUniqueKey      bool     // -U prefer a unique key over the primary key in WHERE
	DoNotPrefixDB     bool     // -doNotAddPrifixDb
	OnlyTypes         []string // -sql insert,update,delete subset (empty = all)
}

// RowKey selects the columns used to build a WHERE clause.
func (ts *TableSchema) keyColumns(o GenOptions) []string {
	// With no key information we must match on every column.
	if len(ts.PrimaryKey) == 0 && len(ts.UniqueKeys) == 0 {
		return ts.Columns
	}
	if o.UseUniqueKey && len(ts.UniqueKeys) > 0 {
		// Choose the first single-column unique key for compact predicates, else smallest.
		best := ts.UniqueKeys[0]
		for _, k := range ts.UniqueKeys {
			if len(k) < len(best) {
				best = k
			}
		}
		return best
	}
	if len(ts.PrimaryKey) > 0 {
		return ts.PrimaryKey
	}
	return ts.Columns
}

func (ts *TableSchema) qualifiedName(o GenOptions) string {
	if o.DoNotPrefixDB {
		return "`" + ts.Table + "`"
	}
	return "`" + ts.Schema + "`.`" + ts.Table + "`"
}

func (ts *TableSchema) indexOf(col string) int {
	for i, c := range ts.Columns {
		if c == col {
			return i
		}
	}
	return -1
}

// Pair is one row's before/after image. Insert/Delete carry one side; Update
// carries both. Values are aligned to TableSchema.Columns.
type Pair struct {
	Before []any
	After  []any
}

// Generate returns the forward SQL and the rollback (flashback) SQL for one
// event, or empty strings when the event type is filtered out by GenOptions.
func (ts *TableSchema) Generate(kind EventKind, p Pair, o GenOptions) (forward string, rollback string) {
	if !o.typeAllowed(kind) {
		return "", ""
	}
	switch kind {
	case KindInsert:
		forward = ts.insertStmt(p.After, o, true)
		rollback = ts.deleteStmt(p.After, o)
	case KindDelete:
		forward = ts.deleteStmt(p.Before, o)
		rollback = ts.insertStmt(p.Before, o, false)
	case KindUpdate:
		forward = ts.updateStmt(p.Before, p.After, o, false)
		rollback = ts.updateStmt(p.Before, p.After, o, true)
	}
	return forward, rollback
}

func (o GenOptions) typeAllowed(k EventKind) bool {
	if len(o.OnlyTypes) == 0 {
		return true
	}
	name := map[EventKind]string{KindInsert: "insert", KindUpdate: "update", KindDelete: "delete"}[k]
	for _, t := range o.OnlyTypes {
		if strings.EqualFold(strings.TrimSpace(t), name) {
			return true
		}
	}
	return false
}

func (ts *TableSchema) insertStmt(vals []any, o GenOptions, isForward bool) string {
	cols := ts.Columns
	skip := map[int]bool{}
	if o.IgnorePKForInsert {
		for _, pk := range ts.PrimaryKey {
			if i := ts.indexOf(pk); i >= 0 {
				// Only skip an auto-increment-looking PK when the value is 0/nil.
				if v := vals[i]; v == nil || isZeroNumber(v) {
					skip[i] = true
				}
			}
		}
	}
	names := make([]string, 0, len(cols))
	placeholders := make([]string, 0, len(cols))
	for i, c := range cols {
		if skip[i] {
			continue
		}
		names = append(names, "`"+c+"`")
		placeholders = append(placeholders, FormatValue(vals[i]))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		ts.qualifiedName(o), strings.Join(names, ", "), strings.Join(placeholders, ", "))
}

func (ts *TableSchema) deleteStmt(vals []any, o GenOptions) string {
	where := ts.whereClause(ts.keyColumns(o), vals)
	if where == "" {
		where = "1=1"
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT 1;", ts.qualifiedName(o), where)
}

func (ts *TableSchema) updateStmt(before, after []any, o GenOptions, reverse bool) string {
	// Forward writes the after-image and locates rows by the before-image;
	// the rollback writes the before-image and locates rows by the after-image.
	setVals := after
	keyImage := before
	if reverse {
		setVals = before
		keyImage = after
	}
	var set []string
	if o.FullColumns {
		for i, c := range ts.Columns {
			set = append(set, "`"+c+"` = "+FormatValue(setVals[i]))
		}
	} else {
		for i, c := range ts.Columns {
			if !sameValue(before[i], after[i]) {
				set = append(set, "`"+c+"` = "+FormatValue(setVals[i]))
			}
		}
	}
	where := ts.whereClause(ts.keyColumns(o), keyImage)
	if where == "" {
		return ""
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s LIMIT 1;", ts.qualifiedName(o), strings.Join(set, ", "), where)
}

func (ts *TableSchema) whereClause(keyCols []string, vals []any) string {
	parts := make([]string, 0, len(keyCols))
	for _, name := range keyCols {
		i := ts.indexOf(name)
		if i < 0 {
			continue
		}
		v := vals[i]
		if v == nil {
			parts = append(parts, "`"+name+"` IS NULL")
		} else {
			parts = append(parts, "`"+name+"` = "+FormatValue(v))
		}
	}
	return strings.Join(parts, " AND ")
}

// FormatValue renders a decoded binlog value as a MySQL literal. It covers the
// Go types the go-mysql RowsEvent decoder produces (see its documented mapping).
func FormatValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if t {
			return "1"
		}
		return "0"
	case int:
		return strconv.FormatInt(int64(t), 10)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint8:
		return strconv.FormatUint(uint64(t), 10)
	case uint16:
		return strconv.FormatUint(uint64(t), 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case *big.Int:
		if t == nil {
			return "NULL"
		}
		return t.String()
	case time.Time:
		return quote(t.Format("2006-01-02 15:04:05.999999"))
	case []byte:
		return hexLiteral(t)
	case string:
		return quote(t)
	default:
		// Decimals arrive as string already; anything else is stringified safely.
		return quote(fmt.Sprint(v))
	}
}

// quote produces a single-quoted MySQL string literal with standard escaping.
func quote(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\'':
			b.WriteString("''")
		case '\\':
			b.WriteString("\\\\")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		case 0:
			b.WriteString("\\0")
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

// hexLiteral renders binary data as 0xHEX so blob/json/geometry round-trips are
// byte-exact without guessing the column charset.
func hexLiteral(data []byte) string {
	if len(data) == 0 {
		return "''"
	}
	const digits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(2 + len(data)*2)
	b.WriteString("0x")
	for _, by := range data {
		b.WriteByte(digits[by>>4])
		b.WriteByte(digits[by&0x0f])
	}
	return b.String()
}

func isZeroNumber(v any) bool {
	switch t := v.(type) {
	case int:
		return t == 0
	case int8:
		return t == 0
	case int16:
		return t == 0
	case int32:
		return t == 0
	case int64:
		return t == 0
	case uint:
		return t == 0
	case uint8:
		return t == 0
	case uint16:
		return t == 0
	case uint32:
		return t == 0
	case uint64:
		return t == 0
	case nil:
		return true
	}
	return false
}

func sameValue(a, b any) bool {
	return FormatValue(a) == FormatValue(b)
}
