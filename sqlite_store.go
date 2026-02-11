package raftsqlite

import (
	"database/sql"
	"errors"

	"github.com/hashicorp/raft"
	_ "modernc.org/sqlite"
)

var (
	// ErrKeyNotFound indicates a given key does not exist.
	ErrKeyNotFound = errors.New("not found")

	// ErrNotImplemented is returned for methods not yet implemented.
	ErrNotImplemented = errors.New("not implemented")
)

// SQLiteStore provides access to a SQLite database for Raft to store and
// retrieve log entries. It also provides key/value storage, and can be used
// as a LogStore and StableStore.
type SQLiteStore struct {
	db   *sql.DB
	path string
}

// Options contains all the configuration used to open the SQLite store.
type Options struct {
	// Path is the file path to the SQLite database.
	Path string

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool
}

// New uses the supplied options to open the SQLite database and prepare it
// for use as a raft backend.
func New(options Options) (*SQLiteStore, error) {
	return nil, ErrNotImplemented
}

// NewSQLiteStore takes a file path and returns a connected Raft backend.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	return New(Options{Path: path})
}

// Close is used to gracefully close the DB connection.
func (s *SQLiteStore) Close() error {
	return ErrNotImplemented
}

// FirstIndex returns the first known index from the Raft log.
func (s *SQLiteStore) FirstIndex() (uint64, error) {
	return 0, ErrNotImplemented
}

// LastIndex returns the last known index from the Raft log.
func (s *SQLiteStore) LastIndex() (uint64, error) {
	return 0, ErrNotImplemented
}

// GetLog gets a log entry at a given index.
func (s *SQLiteStore) GetLog(index uint64, log *raft.Log) error {
	return ErrNotImplemented
}

// StoreLog stores a single raft log entry.
func (s *SQLiteStore) StoreLog(log *raft.Log) error {
	return ErrNotImplemented
}

// StoreLogs stores multiple raft log entries.
func (s *SQLiteStore) StoreLogs(logs []*raft.Log) error {
	return ErrNotImplemented
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (s *SQLiteStore) DeleteRange(min, max uint64) error {
	return ErrNotImplemented
}

// Set is used to set a key/value pair outside of the raft log.
func (s *SQLiteStore) Set(key []byte, val []byte) error {
	return ErrNotImplemented
}

// Get is used to retrieve a value from the k/v store by key.
func (s *SQLiteStore) Get(key []byte) ([]byte, error) {
	return nil, ErrNotImplemented
}

// SetUint64 is like Set, but handles uint64 values.
func (s *SQLiteStore) SetUint64(key []byte, val uint64) error {
	return ErrNotImplemented
}

// GetUint64 is like Get, but handles uint64 values.
func (s *SQLiteStore) GetUint64(key []byte) (uint64, error) {
	return 0, ErrNotImplemented
}

// Verify interface compliance.
var (
	_ raft.LogStore    = (*SQLiteStore)(nil)
	_ raft.StableStore = (*SQLiteStore)(nil)
)
