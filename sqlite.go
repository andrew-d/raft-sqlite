package raftsqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// baseStore holds the shared SQLite infrastructure used by both SQLiteStore
// and SnapshotStore.
type baseStore struct {
	txFactory func(context.Context, *sql.TxOptions) (*sql.Tx, error)
	db        *sql.DB // may be nil
	logf      func(string, ...any)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// openDB opens a SQLite database at the given path and configures it with
// the standard PRAGMAs for raft storage.
func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	pragmas := []string{
		// busy_timeout tells SQLite to wait for 10 seconds if the
		// database is locked before giving up.
		"PRAGMA busy_timeout=10000;",
		// auto_vacuum=INCREMENTAL allows incremental VACUUMs to be
		// triggered using PRAGMA incremental_vacuum.
		//
		// NOTE: this must be set before any tables are created or
		// journal_mode is set.
		"PRAGMA auto_vacuum=INCREMENTAL;",
		// journal_mode=WAL enables write-ahead logging.
		"PRAGMA journal_mode=WAL;",
		// synchronous=FULL is the default, but we set it explicitly here to ensure
		// that we have full ACID consistency.
		"PRAGMA synchronous=FULL;",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to set pragma %q: %w", pragma, err)
		}
	}

	return db, nil
}

// beginTx starts a new read-write transaction.
func (s *baseStore) beginTx() (*sql.Tx, error) {
	return s.txFactory(s.ctx, nil)
}

// beginReadTx starts a new read-only transaction.
func (s *baseStore) beginReadTx() (*sql.Tx, error) {
	return s.txFactory(s.ctx, &sql.TxOptions{ReadOnly: true})
}

// Close cancels background goroutines and closes the database connection.
func (s *baseStore) Close() error {
	s.cancel()
	s.wg.Wait()
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// vacuumLoop periodically runs an incremental vacuum.
func (s *baseStore) vacuumLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.db.Exec("PRAGMA incremental_vacuum"); err != nil {
				s.logf("incremental vacuum failed: %v", err)
			}
		}
	}
}
