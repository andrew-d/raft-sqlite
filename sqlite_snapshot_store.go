package raftsqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

// SQL queries used by the snapshot store.
//
// The "WHERE term >= 0" clauses are tautological conditions (term is a uint64
// stored as INTEGER, so always >= 0) that force SQLite to use an index SEARCH
// (seek) on idx_snapshots_term_idx_id instead of a full table SCAN. Without a
// WHERE clause, SQLite has no predicate to anchor the index seek, even when
// the ORDER BY matches the index perfectly.
const (
	snapshotQueryList   = "SELECT id, version, term, idx, configuration, configuration_index, size, crc, created_at FROM snapshots WHERE term >= 0 ORDER BY term DESC, idx DESC, id DESC"
	snapshotQueryOpen   = "SELECT id, version, term, idx, configuration, configuration_index, size, crc, data FROM snapshots WHERE id = ?"
	snapshotQueryInsert = "INSERT INTO snapshots (id, version, term, idx, configuration, configuration_index, size, crc, data, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	snapshotQueryReap   = "DELETE FROM snapshots WHERE id IN (SELECT id FROM snapshots WHERE term >= 0 ORDER BY term DESC, idx DESC, id DESC LIMIT -1 OFFSET ?)"
)

// This type assertion confirms that 'term' is in fact a uint64, so the >= 0
// condition in the queries is valid.
var _ uint64 = (raft.SnapshotMeta{}).Term

// SnapshotStore is an implementation of [raft.SnapshotStore] that uses SQLite
// as the backend.
type SnapshotStore struct {
	baseStore
	retain int
}

// SnapshotStoreOptions contains all the configuration used to open the
// snapshot store.
type SnapshotStoreOptions struct {
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

	// Retain is the number of snapshots to retain. Must be >= 1.
	Retain int
}

var crc64Table = crc64.MakeTable(crc64.ECMA)

// NewSnapshotStore uses the supplied options to open the SQLite database and
// prepare it for use as a raft snapshot store.
func NewSnapshotStore(options SnapshotStoreOptions) (*SnapshotStore, error) {
	if options.Path != "" && options.TxFactory != nil {
		return nil, errors.New("only one of Path or TxFactory may be set")
	}
	if options.Path == "" && options.TxFactory == nil {
		return nil, errors.New("one of Path or TxFactory must be set")
	}
	if options.Retain < 1 {
		return nil, errors.New("retain must be >= 1")
	}

	logf := options.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	ctx, cancel := context.WithCancel(context.Background())

	if options.TxFactory != nil {
		store := &SnapshotStore{
			baseStore: baseStore{
				txFactory: options.TxFactory,
				logf:      logf,
				ctx:       ctx,
				cancel:    cancel,
			},
			retain: options.Retain,
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

	store := &SnapshotStore{
		baseStore: baseStore{
			db:        db,
			txFactory: db.BeginTx,
			logf:      logf,
			ctx:       ctx,
			cancel:    cancel,
		},
		retain: options.Retain,
	}

	if err := store.initialize(); err != nil {
		db.Close()
		cancel()
		return nil, err
	}

	store.wg.Go(store.vacuumLoop)

	return store, nil
}

// initialize creates the snapshots table if it doesn't already exist.
func (s *SnapshotStore) initialize() error {
	tx, err := s.beginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS snapshots (
			id                  TEXT PRIMARY KEY,
			version             INTEGER NOT NULL,
			term                INTEGER NOT NULL,
			idx                 INTEGER NOT NULL,
			configuration       BLOB NOT NULL,
			configuration_index INTEGER NOT NULL,
			size                INTEGER NOT NULL,
			crc                 BLOB NOT NULL,
			data                BLOB NOT NULL,
			created_at          INTEGER NOT NULL
		) STRICT;
		CREATE INDEX IF NOT EXISTS idx_snapshots_term_idx_id
			ON snapshots (term DESC, idx DESC, id DESC);
	`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Create is used to begin a snapshot at a given index and term, and with the
// given committed configuration. The version parameter controls which snapshot
// version to create.
func (s *SnapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration, configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	if version != 1 {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	id := fmt.Sprintf("%d-%d-%d", term, index, time.Now().UnixMilli())

	sink := &snapshotSink{
		store: s,
		meta: raft.SnapshotMeta{
			Version:            version,
			ID:                 id,
			Index:              index,
			Term:               term,
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
		},
		hash: crc64.New(crc64Table),
	}
	sink.w = io.MultiWriter(&sink.buf, sink.hash)

	return sink, nil
}

// List is used to list the available snapshots in the store. It returns them
// in descending order, with the highest index first.
func (s *SnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	tx, err := s.beginReadTx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(snapshotQueryList)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []*raft.SnapshotMeta
	for rows.Next() {
		var (
			meta      raft.SnapshotMeta
			confBytes []byte
			crcBytes  []byte
			createdAt int64
		)
		if err := rows.Scan(
			&meta.ID, &meta.Version, &meta.Term, &meta.Index,
			&confBytes, &meta.ConfigurationIndex, &meta.Size,
			&crcBytes, &createdAt,
		); err != nil {
			return nil, err
		}
		meta.Configuration = raft.DecodeConfiguration(confBytes)
		snapshots = append(snapshots, &meta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Return empty slice, not nil.
	if snapshots == nil {
		snapshots = []*raft.SnapshotMeta{}
	}
	return snapshots, nil
}

// Open takes a snapshot ID and provides a ReadCloser. Once close is called
// it is assumed the snapshot is no longer needed.
func (s *SnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	tx, err := s.beginReadTx()
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	var (
		meta      raft.SnapshotMeta
		confBytes []byte
		crcBytes  []byte
		data      []byte
	)
	err = tx.QueryRow(snapshotQueryOpen, id).Scan(
		&meta.ID, &meta.Version, &meta.Term, &meta.Index,
		&confBytes, &meta.ConfigurationIndex, &meta.Size,
		&crcBytes, &data,
	)
	if err == sql.ErrNoRows {
		return nil, nil, fmt.Errorf("snapshot %q not found", id)
	}
	if err != nil {
		return nil, nil, err
	}

	meta.Configuration = raft.DecodeConfiguration(confBytes)

	// Verify CRC integrity.
	h := crc64.New(crc64Table)
	h.Write(data)
	computed := make([]byte, 8)
	binary.BigEndian.PutUint64(computed, h.Sum64())
	if !bytes.Equal(computed, crcBytes) {
		return nil, nil, fmt.Errorf("snapshot %q CRC mismatch", id)
	}

	return &meta, io.NopCloser(bytes.NewReader(data)), nil
}

// snapshotSink implements raft.SnapshotSink.
type snapshotSink struct {
	store  *SnapshotStore
	meta   raft.SnapshotMeta
	buf    bytes.Buffer
	hash   hash.Hash64
	w      io.Writer
	closed bool
}

// ID returns the ID of the snapshot being written.
func (s *snapshotSink) ID() string {
	return s.meta.ID
}

// Write writes data to the snapshot.
func (s *snapshotSink) Write(p []byte) (int, error) {
	return s.w.Write(p)
}

// Close finalizes the snapshot and writes it to the database.
func (s *snapshotSink) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	s.meta.Size = int64(s.buf.Len())

	crcBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(crcBytes, s.hash.Sum64())

	confBytes := raft.EncodeConfiguration(s.meta.Configuration)

	tx, err := s.store.beginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(snapshotQueryInsert,
		s.meta.ID,
		s.meta.Version,
		s.meta.Term,
		s.meta.Index,
		confBytes,
		s.meta.ConfigurationIndex,
		s.meta.Size,
		crcBytes,
		s.buf.Bytes(),
		time.Now().UnixMilli(),
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(snapshotQueryReap, s.store.retain)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Cancel aborts the snapshot. No data is written to the database.
func (s *snapshotSink) Cancel() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return nil
}

// Verify interface compliance.
var _ raft.SnapshotStore = (*SnapshotStore)(nil)
