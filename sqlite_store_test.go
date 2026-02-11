package raftsqlite

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func testSQLiteStore(t testing.TB) *SQLiteStore {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.db")

	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestSQLiteStore_Implements(t *testing.T) {
	var store interface{} = &SQLiteStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("SQLiteStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("SQLiteStore does not implement raft.LogStore")
	}
}

func TestNewSQLiteStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.db")

	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer store.Close()

	// Ensure the file was created
	if store.path != path {
		t.Fatalf("unexpected file path %q", store.path)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestSQLiteStore_FirstIndex(t *testing.T) {
	store := testSQLiteStore(t)

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestSQLiteStore_LastIndex(t *testing.T) {
	store := testSQLiteStore(t)

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestSQLiteStore_GetLog(t *testing.T) {
	store := testSQLiteStore(t)

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestSQLiteStore_StoreLog(t *testing.T) {
	store := testSQLiteStore(t)

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestSQLiteStore_StoreLogs(t *testing.T) {
	store := testSQLiteStore(t)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestSQLiteStore_DeleteRange(t *testing.T) {
	store := testSQLiteStore(t)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

func TestSQLiteStore_Set_Get(t *testing.T) {
	store := testSQLiteStore(t)

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestSQLiteStore_SetUint64_GetUint64(t *testing.T) {
	store := testSQLiteStore(t)

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}

// Integration tests that verify behavioral contracts matching the boltdb store.

func TestSQLiteStore_AllLogFields(t *testing.T) {
	store := testSQLiteStore(t)

	now := time.Now()
	log := &raft.Log{
		Index:      10,
		Term:       3,
		Type:       raft.LogConfiguration,
		Data:       []byte("some data"),
		Extensions: []byte("extensions data"),
		AppendedAt: now,
	}

	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	result := new(raft.Log)
	if err := store.GetLog(10, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	if result.Index != 10 {
		t.Fatalf("bad Index: %d", result.Index)
	}
	if result.Term != 3 {
		t.Fatalf("bad Term: %d", result.Term)
	}
	if result.Type != raft.LogConfiguration {
		t.Fatalf("bad Type: %v", result.Type)
	}
	if !bytes.Equal(result.Data, []byte("some data")) {
		t.Fatalf("bad Data: %v", result.Data)
	}
	if !bytes.Equal(result.Extensions, []byte("extensions data")) {
		t.Fatalf("bad Extensions: %v", result.Extensions)
	}
	if !result.AppendedAt.Equal(now) {
		t.Fatalf("bad AppendedAt: got %v, want %v", result.AppendedAt, now)
	}
}

func TestSQLiteStore_Persistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.db")

	// Write data, then close the store.
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.Set([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.SetUint64([]byte("key2"), 42); err != nil {
		t.Fatalf("err: %s", err)
	}
	store.Close()

	// Reopen and verify all data persisted.
	store2, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer store2.Close()

	idx, err := store2.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad first index: %d", idx)
	}

	idx, err = store2.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad last index: %d", idx)
	}

	result := new(raft.Log)
	if err := store2.GetLog(2, result); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(result, logs[1]) {
		t.Fatalf("bad: %#v", result)
	}

	val, err := store2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Fatalf("bad: %v", val)
	}

	uval, err := store2.GetUint64([]byte("key2"))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if uval != 42 {
		t.Fatalf("bad: %v", uval)
	}
}

func TestSQLiteStore_DeleteRange_EdgeCases(t *testing.T) {
	store := testSQLiteStore(t)

	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
		testRaftLog(4, "log4"),
		testRaftLog(5, "log5"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Delete a range in the middle
	if err := store.DeleteRange(2, 4); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Verify boundaries: 1 and 5 should still exist
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("log 1 should exist: %s", err)
	}
	if err := store.GetLog(5, result); err != nil {
		t.Fatalf("log 5 should exist: %s", err)
	}

	// Verify deleted logs are gone
	for i := uint64(2); i <= 4; i++ {
		if err := store.GetLog(i, result); err != raft.ErrLogNotFound {
			t.Fatalf("log %d should have been deleted, got: %v", i, err)
		}
	}

	// FirstIndex and LastIndex should reflect remaining logs
	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if first != 1 {
		t.Fatalf("bad first: %d", first)
	}

	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if last != 5 {
		t.Fatalf("bad last: %d", last)
	}

	// Delete from the beginning - simulates log compaction
	if err := store.DeleteRange(1, 1); err != nil {
		t.Fatalf("err: %s", err)
	}
	first, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if first != 5 {
		t.Fatalf("bad first after compaction: %d", first)
	}
}

func TestSQLiteStore_Set_Overwrite(t *testing.T) {
	store := testSQLiteStore(t)

	k := []byte("key")

	// Set initial value
	if err := store.Set(k, []byte("val1")); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Overwrite with new value
	if err := store.Set(k, []byte("val2")); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Should get the new value
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, []byte("val2")) {
		t.Fatalf("bad: %s", val)
	}
}

func TestSQLiteStore_SetUint64_Overwrite(t *testing.T) {
	store := testSQLiteStore(t)

	k := []byte("key")

	if err := store.SetUint64(k, 1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.SetUint64(k, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != 2 {
		t.Fatalf("bad: %d", val)
	}
}

func TestSQLiteStore_LogCompaction(t *testing.T) {
	// Simulates a typical raft log compaction workflow:
	// 1. Write a bunch of logs
	// 2. Delete old logs (compaction after snapshot)
	// 3. Continue writing new logs
	// 4. Verify indices and data integrity throughout
	store := testSQLiteStore(t)

	// Phase 1: Write initial logs
	var logs []*raft.Log
	for i := uint64(1); i <= 100; i++ {
		logs = append(logs, testRaftLog(i, "data"))
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	if first != 1 || last != 100 {
		t.Fatalf("bad indices: first=%d last=%d", first, last)
	}

	// Phase 2: Compact (delete old logs up through index 50)
	if err := store.DeleteRange(1, 50); err != nil {
		t.Fatalf("err: %s", err)
	}

	first, _ = store.FirstIndex()
	last, _ = store.LastIndex()
	if first != 51 || last != 100 {
		t.Fatalf("bad indices after compaction: first=%d last=%d", first, last)
	}

	// Phase 3: Write more logs
	var moreLogs []*raft.Log
	for i := uint64(101); i <= 150; i++ {
		moreLogs = append(moreLogs, testRaftLog(i, "more data"))
	}
	if err := store.StoreLogs(moreLogs); err != nil {
		t.Fatalf("err: %s", err)
	}

	first, _ = store.FirstIndex()
	last, _ = store.LastIndex()
	if first != 51 || last != 150 {
		t.Fatalf("bad indices after more writes: first=%d last=%d", first, last)
	}

	// Verify data integrity of boundary entries
	result := new(raft.Log)
	if err := store.GetLog(51, result); err != nil {
		t.Fatalf("err: %s", err)
	}
	if string(result.Data) != "data" {
		t.Fatalf("bad data for log 51: %s", result.Data)
	}

	if err := store.GetLog(101, result); err != nil {
		t.Fatalf("err: %s", err)
	}
	if string(result.Data) != "more data" {
		t.Fatalf("bad data for log 101: %s", result.Data)
	}

	// Compacted entries should be gone
	if err := store.GetLog(50, result); err != raft.ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound for compacted log 50, got: %v", err)
	}
}

func TestSQLiteStore_NoSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.db")

	store, err := New(Options{Path: path, NoSync: true})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer store.Close()

	// Basic operations should still work with NoSync
	if err := store.StoreLog(testRaftLog(1, "log1")); err != nil {
		t.Fatalf("err: %s", err)
	}

	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}
	if string(result.Data) != "log1" {
		t.Fatalf("bad: %s", result.Data)
	}
}

func TestSQLiteStore_DeleteRange_Empty(t *testing.T) {
	store := testSQLiteStore(t)

	// Deleting a range on an empty store should not error
	if err := store.DeleteRange(1, 100); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestSQLiteStore_DeleteRange_All(t *testing.T) {
	store := testSQLiteStore(t)

	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Delete all logs
	if err := store.DeleteRange(1, 3); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Should be back to empty state
	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if first != 0 {
		t.Fatalf("bad first: %d", first)
	}

	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if last != 0 {
		t.Fatalf("bad last: %d", last)
	}
}
