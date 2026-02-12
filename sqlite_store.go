package raftsqlite

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"time"

	"github.com/hashicorp/raft"
)

var (
	// ErrKeyNotFound indicates a given key does not exist.
	ErrKeyNotFound = errors.New("not found")
)

// SQL queries used by the store. These are stored as constants so that tests
// can verify query plans against the exact queries used in production.
const (
	queryFirstIndex  = "SELECT MIN(idx) FROM logs"
	queryLastIndex   = "SELECT MAX(idx) FROM logs"
	queryGetLog      = "SELECT idx, term, type, data, extensions, appended_at FROM logs WHERE idx = ?"
	queryStoreLogs   = "INSERT OR REPLACE INTO logs (idx, term, type, data, extensions, appended_at) VALUES (?, ?, ?, ?, ?, ?)"
	queryDeleteRange = "DELETE FROM logs WHERE idx >= ? AND idx <= ?"
	querySet         = "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)"
	queryGet         = "SELECT value FROM kv WHERE key = ?"
)

// SQLiteStore provides access to a SQLite database for Raft to store and
// retrieve log entries. It also provides key/value storage, and can be used
// as a LogStore and StableStore.
type SQLiteStore struct {
	baseStore
}

// Options contains all the configuration used to open the SQLite store.
type Options struct {
	// Path is the file path to the SQLite database.
	//
	// Either Path or TxFactory must be set, but not both.
	Path string

	// TxFactory is an optional function that can be used to provide custom
	// transaction handling. If set, the store will use this function to create
	// transactions instead of opening a database at Path.
	//
	// Either TxFactory or Path must be set, but not both.
	TxFactory func(context.Context, *sql.TxOptions) (*sql.Tx, error)

	// Logf is an optional log function for diagnostic messages.
	// If nil, log messages are discarded.
	Logf func(string, ...any)
}

// New uses the supplied options to open the SQLite database and prepare it
// for use as a raft backend.
func New(options Options) (*SQLiteStore, error) {
	if options.Path != "" && options.TxFactory != nil {
		return nil, errors.New("only one of Path or TxFactory may be set")
	}
	if options.Path == "" && options.TxFactory == nil {
		return nil, errors.New("one of Path or TxFactory must be set")
	}

	logf := options.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	ctx, cancel := context.WithCancel(context.Background())

	if options.TxFactory != nil {
		store := &SQLiteStore{
			baseStore: baseStore{
				txFactory: options.TxFactory,
				logf:      logf,
				ctx:       ctx,
				cancel:    cancel,
			},
		}
		if err := store.initialize(); err != nil {
			cancel()
			return nil, err
		}
		return store, nil
	}

	db, err := openDB(options.Path)
	if err != nil {
		cancel()
		return nil, err
	}

	store := &SQLiteStore{
		baseStore: baseStore{
			db:        db,
			txFactory: db.BeginTx,
			logf:      logf,
			ctx:       ctx,
			cancel:    cancel,
		},
	}

	if err := store.initialize(); err != nil {
		db.Close()
		cancel()
		return nil, err
	}

	store.wg.Go(store.vacuumLoop)

	return store, nil
}

// NewSQLiteStore takes a file path and returns a connected Raft backend.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	return New(Options{Path: path})
}

// initialize creates the tables if they don't already exist.
func (s *SQLiteStore) initialize() error {
	tx, err := s.beginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS logs (
			idx		INTEGER PRIMARY KEY,
			term		INTEGER NOT NULL,
			type		INTEGER NOT NULL,
			data		BLOB NOT NULL,
			extensions	BLOB NOT NULL,
			appended_at	INTEGER NOT NULL
		) STRICT;
		CREATE TABLE IF NOT EXISTS kv (
			key	TEXT PRIMARY KEY,
			value	BLOB NOT NULL
		) STRICT;
	`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// FirstIndex returns the first known index from the Raft log.
func (s *SQLiteStore) FirstIndex() (uint64, error) {
	tx, err := s.beginReadTx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var idx sql.NullInt64
	if err := tx.QueryRow(queryFirstIndex).Scan(&idx); err != nil {
		return 0, err
	}
	if !idx.Valid {
		return 0, nil
	}
	return uint64(idx.Int64), nil
}

// LastIndex returns the last known index from the Raft log.
func (s *SQLiteStore) LastIndex() (uint64, error) {
	tx, err := s.beginReadTx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var idx sql.NullInt64
	if err := tx.QueryRow(queryLastIndex).Scan(&idx); err != nil {
		return 0, err
	}
	if !idx.Valid {
		return 0, nil
	}
	return uint64(idx.Int64), nil
}

// GetLog gets a log entry at a given index.
func (s *SQLiteStore) GetLog(index uint64, log *raft.Log) error {
	tx, err := s.beginReadTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var appendedAtNanos int64
	err = tx.QueryRow(queryGetLog, index).Scan(
		&log.Index, &log.Term, &log.Type, &log.Data, &log.Extensions, &appendedAtNanos,
	)
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
	tx, err := s.beginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(queryStoreLogs)
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
	tx, err := s.beginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(queryDeleteRange, min, max); err != nil {
		return err
	}
	return tx.Commit()
}

// Set is used to set a key/value pair outside of the raft log.
func (s *SQLiteStore) Set(key []byte, val []byte) error {
	tx, err := s.beginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(querySet, string(key), val); err != nil {
		return err
	}
	return tx.Commit()
}

// Get is used to retrieve a value from the k/v store by key.
func (s *SQLiteStore) Get(key []byte) ([]byte, error) {
	tx, err := s.beginReadTx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var val []byte
	err = tx.QueryRow(queryGet, string(key)).Scan(&val)
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
