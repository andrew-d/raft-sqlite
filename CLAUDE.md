# CLAUDE.md

## Project

This is a Go library that implements `raft.LogStore` and `raft.StableStore` from `hashicorp/raft` using SQLite via the pure-Go `modernc.org/sqlite` driver.

## Commands

- `go test ./...` — run all tests
- `go test ./... -run TestIntegration` — run only integration tests (compare behavior against the BoltDB reference implementation)

## Conventions

- All SQL tables must use `STRICT` mode
- Tests use `t.TempDir()` and `t.Cleanup()` for automatic resource management — no manual `os.Remove` or `defer os.RemoveAll`
- Integration tests instantiate both this store and the BoltDB store, run identical operations, and assert matching results
- Use `go tool doc` to read interface documentation from dependencies rather than guessing

## Miscellaneous

- Use the `scratch/` directory in the repository root for any temporary files (log files, etc.) instead of the `/tmp` directory.
- `git add` and `git commit` in two separate tool calls
