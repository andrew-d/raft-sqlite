# raft-sqlite

A SQLite-backed implementation of the [`LogStore`](https://pkg.go.dev/github.com/hashicorp/raft#LogStore) and [`StableStore`](https://pkg.go.dev/github.com/hashicorp/raft#StableStore) interfaces from [hashicorp/raft](https://github.com/hashicorp/raft), using the pure-Go [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) driver.

## Usage

```go
store, err := raftsqlite.NewSQLiteStore("/path/to/raft.db")
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Use as both LogStore and StableStore
raft.NewRaft(config, fsm, store, store, snapshots, transport)
```

For more control, use `raftsqlite.New` with `Options`:

```go
store, err := raftsqlite.New(raftsqlite.Options{
    Path:   "/path/to/raft.db",
    NoSync: true, // unsafe, but faster
})
```
