package raftsqlite

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// store is the interface that both SQLiteStore and BoltStore satisfy.
type store interface {
	raft.LogStore
	raft.StableStore
	Close() error
}

// storePair holds a matched pair of stores for comparison testing.
type storePair struct {
	sqlite *SQLiteStore
	bolt   *raftboltdb.BoltStore
}

func newStorePair(t testing.TB) storePair {
	t.Helper()
	dir := t.TempDir()

	sqliteStore, err := New(Options{Path: filepath.Join(dir, "sqlite.db"), Logf: t.Logf})
	if err != nil {
		t.Fatalf("creating sqlite store: %s", err)
	}
	t.Cleanup(func() { sqliteStore.Close() })

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dir, "bolt.db"))
	if err != nil {
		t.Fatalf("creating bolt store: %s", err)
	}
	t.Cleanup(func() { boltStore.Close() })

	return storePair{sqlite: sqliteStore, bolt: boltStore}
}

// assertEqualErr checks that both errors are either nil or both satisfy the
// same sentinel check. Returns the error (from sqlite) for further use.
func assertEqualErr(t *testing.T, label string, sqliteErr, boltErr error) {
	t.Helper()
	sNil := sqliteErr == nil
	bNil := boltErr == nil
	if sNil != bNil {
		t.Fatalf("%s: error mismatch: sqlite=%v bolt=%v", label, sqliteErr, boltErr)
	}
}

func TestIntegration_FirstIndex_Empty(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	sIdx, sErr := p.sqlite.FirstIndex()
	bIdx, bErr := p.bolt.FirstIndex()
	assertEqualErr(t, "FirstIndex empty", sErr, bErr)
	if sIdx != bIdx {
		t.Fatalf("FirstIndex empty: sqlite=%d bolt=%d", sIdx, bIdx)
	}
}

func TestIntegration_LastIndex_Empty(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	sIdx, sErr := p.sqlite.LastIndex()
	bIdx, bErr := p.bolt.LastIndex()
	assertEqualErr(t, "LastIndex empty", sErr, bErr)
	if sIdx != bIdx {
		t.Fatalf("LastIndex empty: sqlite=%d bolt=%d", sIdx, bIdx)
	}
}

func TestIntegration_GetLog_NotFound(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	sErr := p.sqlite.GetLog(1, new(raft.Log))
	bErr := p.bolt.GetLog(1, new(raft.Log))
	if sErr != bErr {
		t.Fatalf("GetLog not found: sqlite=%v bolt=%v", sErr, bErr)
	}
}

func TestIntegration_StoreAndGetLogs(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("first")},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("second")},
		{Index: 3, Term: 2, Type: raft.LogNoop, Data: []byte("third")},
	}

	sErr := p.sqlite.StoreLogs(logs)
	bErr := p.bolt.StoreLogs(logs)
	assertEqualErr(t, "StoreLogs", sErr, bErr)

	// Compare FirstIndex
	sFirst, _ := p.sqlite.FirstIndex()
	bFirst, _ := p.bolt.FirstIndex()
	if sFirst != bFirst {
		t.Fatalf("FirstIndex: sqlite=%d bolt=%d", sFirst, bFirst)
	}

	// Compare LastIndex
	sLast, _ := p.sqlite.LastIndex()
	bLast, _ := p.bolt.LastIndex()
	if sLast != bLast {
		t.Fatalf("LastIndex: sqlite=%d bolt=%d", sLast, bLast)
	}

	// Compare each log entry
	for _, idx := range []uint64{1, 2, 3} {
		sLog, bLog := new(raft.Log), new(raft.Log)
		sErr := p.sqlite.GetLog(idx, sLog)
		bErr := p.bolt.GetLog(idx, bLog)
		assertEqualErr(t, fmt.Sprintf("GetLog(%d)", idx), sErr, bErr)

		if sLog.Index != bLog.Index {
			t.Fatalf("log %d Index: sqlite=%d bolt=%d", idx, sLog.Index, bLog.Index)
		}
		if sLog.Term != bLog.Term {
			t.Fatalf("log %d Term: sqlite=%d bolt=%d", idx, sLog.Term, bLog.Term)
		}
		if sLog.Type != bLog.Type {
			t.Fatalf("log %d Type: sqlite=%v bolt=%v", idx, sLog.Type, bLog.Type)
		}
		if !bytes.Equal(sLog.Data, bLog.Data) {
			t.Fatalf("log %d Data: sqlite=%q bolt=%q", idx, sLog.Data, bLog.Data)
		}
	}
}

func TestIntegration_StoreLog_Single(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	log := &raft.Log{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("cmd")}

	sErr := p.sqlite.StoreLog(log)
	bErr := p.bolt.StoreLog(log)
	assertEqualErr(t, "StoreLog", sErr, bErr)

	sLog, bLog := new(raft.Log), new(raft.Log)
	sErr = p.sqlite.GetLog(1, sLog)
	bErr = p.bolt.GetLog(1, bLog)
	assertEqualErr(t, "GetLog after StoreLog", sErr, bErr)

	if sLog.Index != bLog.Index || sLog.Term != bLog.Term || sLog.Type != bLog.Type || !bytes.Equal(sLog.Data, bLog.Data) {
		t.Fatalf("logs differ: sqlite=%+v bolt=%+v", sLog, bLog)
	}
}

func TestIntegration_DeleteRange(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("a")},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("b")},
		{Index: 3, Term: 1, Type: raft.LogCommand, Data: []byte("c")},
		{Index: 4, Term: 2, Type: raft.LogCommand, Data: []byte("d")},
		{Index: 5, Term: 2, Type: raft.LogCommand, Data: []byte("e")},
	}

	p.sqlite.StoreLogs(logs)
	p.bolt.StoreLogs(logs)

	// Delete middle range
	sErr := p.sqlite.DeleteRange(2, 4)
	bErr := p.bolt.DeleteRange(2, 4)
	assertEqualErr(t, "DeleteRange", sErr, bErr)

	// Check each index for matching behavior
	for _, idx := range []uint64{1, 2, 3, 4, 5} {
		sErr := p.sqlite.GetLog(idx, new(raft.Log))
		bErr := p.bolt.GetLog(idx, new(raft.Log))
		if sErr != bErr {
			t.Fatalf("GetLog(%d) after delete: sqlite=%v bolt=%v", idx, sErr, bErr)
		}
	}

	// Compare indices
	sFirst, _ := p.sqlite.FirstIndex()
	bFirst, _ := p.bolt.FirstIndex()
	if sFirst != bFirst {
		t.Fatalf("FirstIndex after delete: sqlite=%d bolt=%d", sFirst, bFirst)
	}
	sLast, _ := p.sqlite.LastIndex()
	bLast, _ := p.bolt.LastIndex()
	if sLast != bLast {
		t.Fatalf("LastIndex after delete: sqlite=%d bolt=%d", sLast, bLast)
	}
}

func TestIntegration_DeleteRange_Empty(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	sErr := p.sqlite.DeleteRange(1, 100)
	bErr := p.bolt.DeleteRange(1, 100)
	assertEqualErr(t, "DeleteRange empty", sErr, bErr)
}

func TestIntegration_DeleteRange_All(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("a")},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("b")},
	}
	p.sqlite.StoreLogs(logs)
	p.bolt.StoreLogs(logs)

	sErr := p.sqlite.DeleteRange(1, 2)
	bErr := p.bolt.DeleteRange(1, 2)
	assertEqualErr(t, "DeleteRange all", sErr, bErr)

	sFirst, _ := p.sqlite.FirstIndex()
	bFirst, _ := p.bolt.FirstIndex()
	if sFirst != bFirst {
		t.Fatalf("FirstIndex after delete all: sqlite=%d bolt=%d", sFirst, bFirst)
	}
	sLast, _ := p.sqlite.LastIndex()
	bLast, _ := p.bolt.LastIndex()
	if sLast != bLast {
		t.Fatalf("LastIndex after delete all: sqlite=%d bolt=%d", sLast, bLast)
	}
}

func TestIntegration_Set_Get(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	// Both should error on missing key
	_, sErr := p.sqlite.Get([]byte("missing"))
	_, bErr := p.bolt.Get([]byte("missing"))
	if (sErr == nil) != (bErr == nil) {
		t.Fatalf("Get missing: sqlite=%v bolt=%v", sErr, bErr)
	}

	// Set and read back
	k, v := []byte("hello"), []byte("world")
	sErr = p.sqlite.Set(k, v)
	bErr = p.bolt.Set(k, v)
	assertEqualErr(t, "Set", sErr, bErr)

	sVal, sErr := p.sqlite.Get(k)
	bVal, bErr := p.bolt.Get(k)
	assertEqualErr(t, "Get", sErr, bErr)
	if !bytes.Equal(sVal, bVal) {
		t.Fatalf("Get value: sqlite=%q bolt=%q", sVal, bVal)
	}
}

func TestIntegration_Set_Overwrite(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	k := []byte("key")
	p.sqlite.Set(k, []byte("v1"))
	p.bolt.Set(k, []byte("v1"))
	p.sqlite.Set(k, []byte("v2"))
	p.bolt.Set(k, []byte("v2"))

	sVal, _ := p.sqlite.Get(k)
	bVal, _ := p.bolt.Get(k)
	if !bytes.Equal(sVal, bVal) {
		t.Fatalf("overwrite: sqlite=%q bolt=%q", sVal, bVal)
	}
}

func TestIntegration_SetUint64_GetUint64(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	// Both should error on missing key
	_, sErr := p.sqlite.GetUint64([]byte("missing"))
	_, bErr := p.bolt.GetUint64([]byte("missing"))
	if (sErr == nil) != (bErr == nil) {
		t.Fatalf("GetUint64 missing: sqlite=%v bolt=%v", sErr, bErr)
	}

	k := []byte("counter")
	sErr = p.sqlite.SetUint64(k, 42)
	bErr = p.bolt.SetUint64(k, 42)
	assertEqualErr(t, "SetUint64", sErr, bErr)

	sVal, sErr := p.sqlite.GetUint64(k)
	bVal, bErr := p.bolt.GetUint64(k)
	assertEqualErr(t, "GetUint64", sErr, bErr)
	if sVal != bVal {
		t.Fatalf("GetUint64 value: sqlite=%d bolt=%d", sVal, bVal)
	}
}

func TestIntegration_SetUint64_Overwrite(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	k := []byte("counter")
	p.sqlite.SetUint64(k, 1)
	p.bolt.SetUint64(k, 1)
	p.sqlite.SetUint64(k, 2)
	p.bolt.SetUint64(k, 2)

	sVal, _ := p.sqlite.GetUint64(k)
	bVal, _ := p.bolt.GetUint64(k)
	if sVal != bVal {
		t.Fatalf("overwrite: sqlite=%d bolt=%d", sVal, bVal)
	}
}

func TestIntegration_LogCompaction(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	// Write 100 logs
	var logs []*raft.Log
	for i := uint64(1); i <= 100; i++ {
		logs = append(logs, &raft.Log{
			Index: i,
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte(fmt.Sprintf("data-%d", i)),
		})
	}
	p.sqlite.StoreLogs(logs)
	p.bolt.StoreLogs(logs)

	// Compact first 50
	p.sqlite.DeleteRange(1, 50)
	p.bolt.DeleteRange(1, 50)

	// Write 50 more
	var moreLogs []*raft.Log
	for i := uint64(101); i <= 150; i++ {
		moreLogs = append(moreLogs, &raft.Log{
			Index: i,
			Term:  2,
			Type:  raft.LogCommand,
			Data:  []byte(fmt.Sprintf("data-%d", i)),
		})
	}
	p.sqlite.StoreLogs(moreLogs)
	p.bolt.StoreLogs(moreLogs)

	// Compare indices
	sFirst, _ := p.sqlite.FirstIndex()
	bFirst, _ := p.bolt.FirstIndex()
	if sFirst != bFirst {
		t.Fatalf("FirstIndex: sqlite=%d bolt=%d", sFirst, bFirst)
	}
	sLast, _ := p.sqlite.LastIndex()
	bLast, _ := p.bolt.LastIndex()
	if sLast != bLast {
		t.Fatalf("LastIndex: sqlite=%d bolt=%d", sLast, bLast)
	}

	// Spot-check logs at boundaries
	for _, idx := range []uint64{50, 51, 100, 101, 150} {
		sLog, bLog := new(raft.Log), new(raft.Log)
		sErr := p.sqlite.GetLog(idx, sLog)
		bErr := p.bolt.GetLog(idx, bLog)
		if sErr != bErr {
			t.Fatalf("GetLog(%d): sqlite=%v bolt=%v", idx, sErr, bErr)
		}
		if sErr == nil && !bytes.Equal(sLog.Data, bLog.Data) {
			t.Fatalf("GetLog(%d) data: sqlite=%q bolt=%q", idx, sLog.Data, bLog.Data)
		}
	}
}

func TestIntegration_MixedOperations(t *testing.T) {
	t.Parallel()
	p := newStorePair(t)

	// Interleave log and kv operations like real raft usage
	steps := []func(s store){
		func(s store) { s.SetUint64([]byte("CurrentTerm"), 1) },
		func(s store) { s.Set([]byte("LastVoteCand"), []byte("node-a")) },
		func(s store) {
			s.StoreLogs([]*raft.Log{
				{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("cmd1")},
				{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("cmd2")},
			})
		},
		func(s store) { s.SetUint64([]byte("CurrentTerm"), 2) },
		func(s store) { s.Set([]byte("LastVoteCand"), []byte("node-b")) },
		func(s store) {
			s.StoreLogs([]*raft.Log{
				{Index: 3, Term: 2, Type: raft.LogCommand, Data: []byte("cmd3")},
			})
		},
		func(s store) { s.DeleteRange(1, 1) },
	}

	for _, step := range steps {
		step(p.sqlite)
		step(p.bolt)
	}

	// Compare final state: indices
	sFirst, _ := p.sqlite.FirstIndex()
	bFirst, _ := p.bolt.FirstIndex()
	if sFirst != bFirst {
		t.Fatalf("FirstIndex: sqlite=%d bolt=%d", sFirst, bFirst)
	}
	sLast, _ := p.sqlite.LastIndex()
	bLast, _ := p.bolt.LastIndex()
	if sLast != bLast {
		t.Fatalf("LastIndex: sqlite=%d bolt=%d", sLast, bLast)
	}

	// Compare final state: remaining logs
	for idx := sFirst; idx <= sLast; idx++ {
		sLog, bLog := new(raft.Log), new(raft.Log)
		sErr := p.sqlite.GetLog(idx, sLog)
		bErr := p.bolt.GetLog(idx, bLog)
		if sErr != bErr {
			t.Fatalf("GetLog(%d): sqlite=%v bolt=%v", idx, sErr, bErr)
		}
		if sErr == nil {
			if sLog.Index != bLog.Index || sLog.Term != bLog.Term || sLog.Type != bLog.Type || !bytes.Equal(sLog.Data, bLog.Data) {
				t.Fatalf("GetLog(%d) mismatch: sqlite=%+v bolt=%+v", idx, sLog, bLog)
			}
		}
	}

	// Compare final state: kv
	sTerm, _ := p.sqlite.GetUint64([]byte("CurrentTerm"))
	bTerm, _ := p.bolt.GetUint64([]byte("CurrentTerm"))
	if sTerm != bTerm {
		t.Fatalf("CurrentTerm: sqlite=%d bolt=%d", sTerm, bTerm)
	}

	sVote, _ := p.sqlite.Get([]byte("LastVoteCand"))
	bVote, _ := p.bolt.Get([]byte("LastVoteCand"))
	if !bytes.Equal(sVote, bVote) {
		t.Fatalf("LastVoteCand: sqlite=%q bolt=%q", sVote, bVote)
	}
}
