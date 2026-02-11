package raftsqlite

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"time"

	"github.com/hashicorp/raft"
	_ "modernc.org/sqlite"
)

var (
	// ErrKeyNotFound indicates a given key does not exist.
	ErrKeyNotFound = errors.New("not found")
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
	db, err := sql.Open("sqlite", options.Path)
	if err != nil {
		return nil, err
	}

	// Enable WAL mode for better concurrent performance.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, err
	}

	if options.NoSync {
		if _, err := db.Exec("PRAGMA synchronous=OFF"); err != nil {
			db.Close()
			return nil, err
		}
	}

	store := &SQLiteStore{
		db:   db,
		path: options.Path,
	}

	if err := store.initialize(); err != nil {
		db.Close()
		return nil, err
	}

	return store, nil
}

// NewSQLiteStore takes a file path and returns a connected Raft backend.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	return New(Options{Path: path})
}

// initialize creates the tables if they don't already exist.
func (s *SQLiteStore) initialize() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS logs (
			idx INTEGER PRIMARY KEY,
			term INTEGER NOT NULL,
			type INTEGER NOT NULL,
			data BLOB NOT NULL,
			extensions BLOB NOT NULL,
			appended_at INTEGER NOT NULL
		) STRICT;
		CREATE TABLE IF NOT EXISTS kv (
			key TEXT PRIMARY KEY,
			value BLOB NOT NULL
		) STRICT;
	`)
	return err
}

// Close is used to gracefully close the DB connection.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (s *SQLiteStore) FirstIndex() (uint64, error) {
	var idx sql.NullInt64
	if err := s.db.QueryRow("SELECT MIN(idx) FROM logs").Scan(&idx); err != nil {
		return 0, err
	}
	if !idx.Valid {
		return 0, nil
	}
	return uint64(idx.Int64), nil
}

// LastIndex returns the last known index from the Raft log.
func (s *SQLiteStore) LastIndex() (uint64, error) {
	var idx sql.NullInt64
	if err := s.db.QueryRow("SELECT MAX(idx) FROM logs").Scan(&idx); err != nil {
		return 0, err
	}
	if !idx.Valid {
		return 0, nil
	}
	return uint64(idx.Int64), nil
}

// GetLog gets a log entry at a given index.
func (s *SQLiteStore) GetLog(index uint64, log *raft.Log) error {
	var appendedAtNanos int64
	err := s.db.QueryRow(
		"SELECT idx, term, type, data, extensions, appended_at FROM logs WHERE idx = ?",
		index,
	).Scan(&log.Index, &log.Term, &log.Type, &log.Data, &log.Extensions, &appendedAtNanos)
	if err == sql.ErrNoRows {
		return raft.ErrLogNotFound
	}
	if err != nil {
		return err
	}
	if len(log.Data) == 0 {
		log.Data = nil
	}
	if len(log.Extensions) == 0 {
		log.Extensions = nil
	}
	if appendedAtNanos != 0 {
		log.AppendedAt = time.Unix(0, appendedAtNanos)
	}
	return nil
}

// StoreLog stores a single raft log entry.
func (s *SQLiteStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple raft log entries.
func (s *SQLiteStore) StoreLogs(logs []*raft.Log) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(
		"INSERT OR REPLACE INTO logs (idx, term, type, data, extensions, appended_at) VALUES (?, ?, ?, ?, ?, ?)",
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, log := range logs {
		var appendedAtNanos int64
		if !log.AppendedAt.IsZero() {
			appendedAtNanos = log.AppendedAt.UnixNano()
		}
		data := log.Data
		if data == nil {
			data = []byte{}
		}
		extensions := log.Extensions
		if extensions == nil {
			extensions = []byte{}
		}
		if _, err := stmt.Exec(log.Index, log.Term, log.Type, data, extensions, appendedAtNanos); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (s *SQLiteStore) DeleteRange(min, max uint64) error {
	_, err := s.db.Exec("DELETE FROM logs WHERE idx >= ? AND idx <= ?", min, max)
	return err
}

// Set is used to set a key/value pair outside of the raft log.
func (s *SQLiteStore) Set(key []byte, val []byte) error {
	_, err := s.db.Exec(
		"INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
		string(key), val,
	)
	return err
}

// Get is used to retrieve a value from the k/v store by key.
func (s *SQLiteStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.QueryRow("SELECT value FROM kv WHERE key = ?", string(key)).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

// SetUint64 is like Set, but handles uint64 values.
func (s *SQLiteStore) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return s.Set(key, buf)
}

// GetUint64 is like Get, but handles uint64 values.
func (s *SQLiteStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(val), nil
}

// Verify interface compliance.
var (
	_ raft.LogStore    = (*SQLiteStore)(nil)
	_ raft.StableStore = (*SQLiteStore)(nil)
)
