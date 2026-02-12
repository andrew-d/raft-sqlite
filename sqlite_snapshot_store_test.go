package raftsqlite

import (
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hashicorp/raft"
)

func testSnapshotStore(t testing.TB) *SnapshotStore {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshots.db")

	store, err := NewSnapshotStore(SnapshotStoreOptions{
		Path:   path,
		Logf:   t.Logf,
		Retain: 2,
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func testConfiguration() raft.Configuration {
	return raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       "node-1",
				Address:  "127.0.0.1:8300",
			},
		},
	}
}

func createTestSnapshot(t *testing.T, store *SnapshotStore, term, index uint64, data string) {
	t.Helper()
	sink, err := store.Create(1, index, term, testConfiguration(), index, nil)
	if err != nil {
		t.Fatalf("creating snapshot: %v", err)
	}
	if _, err := io.WriteString(sink, data); err != nil {
		t.Fatalf("writing snapshot data: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("closing snapshot sink: %v", err)
	}
}

func TestSnapshotStore_CreateAndOpen(t *testing.T) {
	t.Parallel()
	store := testSnapshotStore(t)

	data := "hello snapshot world"

	sink, err := store.Create(1, 10, 3, testConfiguration(), 8, nil)
	if err != nil {
		t.Fatalf("creating snapshot: %v", err)
	}

	if _, err := io.WriteString(sink, data); err != nil {
		t.Fatalf("writing snapshot data: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("closing snapshot sink: %v", err)
	}

	// Open it back
	meta, rc, err := store.Open(sink.ID())
	if err != nil {
		t.Fatalf("opening snapshot: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading snapshot data: %v", err)
	}

	if string(got) != data {
		t.Fatalf("data mismatch: got %q, want %q", got, data)
	}

	if meta.Index != 10 {
		t.Fatalf("Index: got %d, want 10", meta.Index)
	}
	if meta.Term != 3 {
		t.Fatalf("Term: got %d, want 3", meta.Term)
	}
	if meta.Version != 1 {
		t.Fatalf("Version: got %d, want 1", meta.Version)
	}
	if meta.ConfigurationIndex != 8 {
		t.Fatalf("ConfigurationIndex: got %d, want 8", meta.ConfigurationIndex)
	}
	if meta.Size != int64(len(data)) {
		t.Fatalf("Size: got %d, want %d", meta.Size, len(data))
	}
	if len(meta.Configuration.Servers) != 1 {
		t.Fatalf("Configuration.Servers: got %d servers, want 1", len(meta.Configuration.Servers))
	}
	if meta.Configuration.Servers[0].ID != "node-1" {
		t.Fatalf("Configuration.Servers[0].ID: got %q, want %q", meta.Configuration.Servers[0].ID, "node-1")
	}
}

func TestSnapshotStore_List_Ordering(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		store := testSnapshotStore(t)

		// Create snapshots with different terms/indices.
		// Sleep between to ensure different timestamps in IDs.
		createTestSnapshot(t, store, 1, 5, "snap1")
		time.Sleep(2 * time.Millisecond)
		createTestSnapshot(t, store, 2, 10, "snap2")

		snapshots, err := store.List()
		if err != nil {
			t.Fatalf("listing snapshots: %v", err)
		}
		if len(snapshots) != 2 {
			t.Fatalf("got %d snapshots, want 2", len(snapshots))
		}

		// Should be descending: term 2 first, then term 1.
		if snapshots[0].Term != 2 {
			t.Fatalf("first snapshot Term: got %d, want 2", snapshots[0].Term)
		}
		if snapshots[1].Term != 1 {
			t.Fatalf("second snapshot Term: got %d, want 1", snapshots[1].Term)
		}
	})
}

func TestSnapshotStore_List_Empty(t *testing.T) {
	t.Parallel()
	store := testSnapshotStore(t)

	snapshots, err := store.List()
	if err != nil {
		t.Fatalf("listing snapshots: %v", err)
	}
	if snapshots == nil {
		t.Fatal("List returned nil, want empty slice")
	}
	if len(snapshots) != 0 {
		t.Fatalf("got %d snapshots, want 0", len(snapshots))
	}
}

func TestSnapshotStore_Cancel(t *testing.T) {
	t.Parallel()
	store := testSnapshotStore(t)

	sink, err := store.Create(1, 10, 3, testConfiguration(), 8, nil)
	if err != nil {
		t.Fatalf("creating snapshot: %v", err)
	}

	if _, err := io.WriteString(sink, "some data"); err != nil {
		t.Fatalf("writing snapshot data: %v", err)
	}

	if err := sink.Cancel(); err != nil {
		t.Fatalf("canceling snapshot: %v", err)
	}

	snapshots, err := store.List()
	if err != nil {
		t.Fatalf("listing snapshots: %v", err)
	}
	if len(snapshots) != 0 {
		t.Fatalf("got %d snapshots after cancel, want 0", len(snapshots))
	}
}

func TestSnapshotStore_Retention(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "snapshots.db")

		store, err := NewSnapshotStore(SnapshotStoreOptions{
			Path:   path,
			Logf:   t.Logf,
			Retain: 2,
		})
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		t.Cleanup(func() { store.Close() })

		// Create 4 snapshots; only 2 should remain.
		for i := uint64(1); i <= 4; i++ {
			createTestSnapshot(t, store, i, i*10, "data")
			time.Sleep(2 * time.Millisecond)
		}

		snapshots, err := store.List()
		if err != nil {
			t.Fatalf("listing snapshots: %v", err)
		}
		if len(snapshots) != 2 {
			t.Fatalf("got %d snapshots, want 2", len(snapshots))
		}

		// The two newest should remain (term 4 and term 3).
		if snapshots[0].Term != 4 {
			t.Fatalf("first snapshot Term: got %d, want 4", snapshots[0].Term)
		}
		if snapshots[1].Term != 3 {
			t.Fatalf("second snapshot Term: got %d, want 3", snapshots[1].Term)
		}
	})
}

func TestSnapshotStore_CRC_Integrity(t *testing.T) {
	t.Parallel()
	store := testSnapshotStore(t)

	createTestSnapshot(t, store, 1, 10, "important data")

	snapshots, err := store.List()
	if err != nil {
		t.Fatalf("listing snapshots: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("got %d snapshots, want 1", len(snapshots))
	}

	// Corrupt the data directly in the DB.
	tx, err := store.beginTx()
	if err != nil {
		t.Fatalf("beginning tx: %v", err)
	}
	// Overwrite data with garbage but keep the original CRC.
	_, err = tx.Exec("UPDATE snapshots SET data = ? WHERE id = ?", []byte("corrupted!"), snapshots[0].ID)
	if err != nil {
		tx.Rollback()
		t.Fatalf("corrupting data: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("committing corruption: %v", err)
	}

	// Open should detect the CRC mismatch.
	_, _, err = store.Open(snapshots[0].ID)
	if err == nil {
		t.Fatal("expected error opening corrupted snapshot, got nil")
	}
}

func TestSnapshotStore_Close_Idempotent(t *testing.T) {
	t.Parallel()
	store := testSnapshotStore(t)

	sink, err := store.Create(1, 10, 3, testConfiguration(), 8, nil)
	if err != nil {
		t.Fatalf("creating snapshot: %v", err)
	}
	if _, err := io.WriteString(sink, "data"); err != nil {
		t.Fatalf("writing snapshot data: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestSnapshotStore_Cancel_Idempotent(t *testing.T) {
	t.Parallel()
	store := testSnapshotStore(t)

	sink, err := store.Create(1, 10, 3, testConfiguration(), 8, nil)
	if err != nil {
		t.Fatalf("creating snapshot: %v", err)
	}

	if err := sink.Cancel(); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("second cancel: %v", err)
	}
}

func TestSnapshotStore_Open_NotFound(t *testing.T) {
	t.Parallel()
	store := testSnapshotStore(t)

	_, _, err := store.Open("nonexistent-id")
	if err == nil {
		t.Fatal("expected error opening nonexistent snapshot, got nil")
	}
}

func TestSnapshotStore_BadOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options SnapshotStoreOptions
	}{
		{
			name:    "no_path_or_txfactory",
			options: SnapshotStoreOptions{Retain: 1},
		},
		{
			name: "both_path_and_txfactory",
			options: SnapshotStoreOptions{
				Path:      "/tmp/test.db",
				TxFactory: func(context.Context, *sql.TxOptions) (*sql.Tx, error) { return nil, nil },
				Retain:    1,
			},
		},
		{
			name:    "retain_zero",
			options: SnapshotStoreOptions{Path: "/tmp/test.db", Retain: 0},
		},
		{
			name:    "retain_negative",
			options: SnapshotStoreOptions{Path: "/tmp/test.db", Retain: -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewSnapshotStore(tt.options)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}
