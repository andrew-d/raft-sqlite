package raftsqlite

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hashicorp/raft"
)

// snapshotStorePair holds a matched pair of snapshot stores for comparison testing.
type snapshotStorePair struct {
	sqlite *SnapshotStore
	file   *raft.FileSnapshotStore
	trans  raft.Transport
}

func newSnapshotStorePair(t *testing.T) snapshotStorePair {
	t.Helper()
	dir := t.TempDir()

	sqliteStore, err := NewSnapshotStore(SnapshotStoreOptions{
		Path:   filepath.Join(dir, "snapshots.db"),
		Logf:   t.Logf,
		Retain: 2,
	})
	if err != nil {
		t.Fatalf("creating sqlite snapshot store: %s", err)
	}
	t.Cleanup(func() { sqliteStore.Close() })

	_, fileStore := raft.FileSnapTest(t)

	_, trans := raft.NewInmemTransportWithTimeout("", time.Second)

	return snapshotStorePair{sqlite: sqliteStore, file: fileStore, trans: trans}
}

func TestIntegration_Snapshot_List_Empty(t *testing.T) {
	t.Parallel()
	p := newSnapshotStorePair(t)

	sList, sErr := p.sqlite.List()
	fList, fErr := p.file.List()
	assertEqualErr(t, "List empty", sErr, fErr)

	if len(sList) != len(fList) {
		t.Fatalf("List empty length: sqlite=%d file=%d", len(sList), len(fList))
	}
}

func TestIntegration_Snapshot_CreateAndOpen(t *testing.T) {
	t.Parallel()
	p := newSnapshotStorePair(t)

	conf := testConfiguration()
	data := "integration snapshot data"

	sID := createSnapshotOnStore(t, p.sqlite, 1, 10, 3, conf, 8, nil, data)
	fID := createSnapshotOnStore(t, p.file, 1, 10, 3, conf, 8, p.trans, data)

	// Open both and compare.
	sMeta, sRC, sErr := p.sqlite.Open(sID)
	fMeta, fRC, fErr := p.file.Open(fID)
	assertEqualErr(t, "Open", sErr, fErr)
	defer sRC.Close()
	defer fRC.Close()

	// Compare metadata.
	if sMeta.Version != fMeta.Version {
		t.Fatalf("Version: sqlite=%d file=%d", sMeta.Version, fMeta.Version)
	}
	if sMeta.Index != fMeta.Index {
		t.Fatalf("Index: sqlite=%d file=%d", sMeta.Index, fMeta.Index)
	}
	if sMeta.Term != fMeta.Term {
		t.Fatalf("Term: sqlite=%d file=%d", sMeta.Term, fMeta.Term)
	}
	if sMeta.ConfigurationIndex != fMeta.ConfigurationIndex {
		t.Fatalf("ConfigurationIndex: sqlite=%d file=%d", sMeta.ConfigurationIndex, fMeta.ConfigurationIndex)
	}
	if sMeta.Size != fMeta.Size {
		t.Fatalf("Size: sqlite=%d file=%d", sMeta.Size, fMeta.Size)
	}
	if len(sMeta.Configuration.Servers) != len(fMeta.Configuration.Servers) {
		t.Fatalf("Configuration.Servers length: sqlite=%d file=%d",
			len(sMeta.Configuration.Servers), len(fMeta.Configuration.Servers))
	}
	for i := range sMeta.Configuration.Servers {
		if sMeta.Configuration.Servers[i].ID != fMeta.Configuration.Servers[i].ID {
			t.Fatalf("Configuration.Servers[%d].ID: sqlite=%q file=%q",
				i, sMeta.Configuration.Servers[i].ID, fMeta.Configuration.Servers[i].ID)
		}
		if sMeta.Configuration.Servers[i].Address != fMeta.Configuration.Servers[i].Address {
			t.Fatalf("Configuration.Servers[%d].Address: sqlite=%q file=%q",
				i, sMeta.Configuration.Servers[i].Address, fMeta.Configuration.Servers[i].Address)
		}
		if sMeta.Configuration.Servers[i].Suffrage != fMeta.Configuration.Servers[i].Suffrage {
			t.Fatalf("Configuration.Servers[%d].Suffrage: sqlite=%v file=%v",
				i, sMeta.Configuration.Servers[i].Suffrage, fMeta.Configuration.Servers[i].Suffrage)
		}
	}

	// Compare data.
	sData, err := io.ReadAll(sRC)
	if err != nil {
		t.Fatalf("reading sqlite snapshot data: %v", err)
	}
	fData, err := io.ReadAll(fRC)
	if err != nil {
		t.Fatalf("reading file snapshot data: %v", err)
	}
	if !bytes.Equal(sData, fData) {
		t.Fatalf("data mismatch: sqlite=%q file=%q", sData, fData)
	}
}

func TestIntegration_Snapshot_List_Ordering(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		p := newSnapshotStorePair(t)

		conf := testConfiguration()

		createSnapshotOnStore(t, p.sqlite, 1, 5, 1, conf, 3, nil, "s1")
		createSnapshotOnStore(t, p.file, 1, 5, 1, conf, 3, p.trans, "s1")
		time.Sleep(2 * time.Millisecond)

		createSnapshotOnStore(t, p.sqlite, 1, 10, 2, conf, 8, nil, "s2")
		createSnapshotOnStore(t, p.file, 1, 10, 2, conf, 8, p.trans, "s2")

		sList, sErr := p.sqlite.List()
		fList, fErr := p.file.List()
		assertEqualErr(t, "List ordering", sErr, fErr)

		if len(sList) != len(fList) {
			t.Fatalf("List length: sqlite=%d file=%d", len(sList), len(fList))
		}

		// Both should return highest term/index first.
		for i := range sList {
			if sList[i].Term != fList[i].Term {
				t.Fatalf("List[%d].Term: sqlite=%d file=%d", i, sList[i].Term, fList[i].Term)
			}
			if sList[i].Index != fList[i].Index {
				t.Fatalf("List[%d].Index: sqlite=%d file=%d", i, sList[i].Index, fList[i].Index)
			}
		}
	})
}

func createSnapshotOnStore(t *testing.T, store raft.SnapshotStore, version raft.SnapshotVersion, index, term uint64, conf raft.Configuration, confIndex uint64, trans raft.Transport, data string) string {
	t.Helper()
	sink, err := store.Create(version, index, term, conf, confIndex, trans)
	if err != nil {
		t.Fatalf("creating snapshot: %v", err)
	}
	if _, err := io.WriteString(sink, data); err != nil {
		t.Fatalf("writing snapshot data: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("closing snapshot sink: %v", err)
	}
	return sink.ID()
}
