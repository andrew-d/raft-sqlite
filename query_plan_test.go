package raftsqlite

import (
	"testing"
)

// queryPlanDetails runs EXPLAIN QUERY PLAN on the given query and returns
// the detail strings from each row of the result.
func queryPlanDetails(t *testing.T, store *SQLiteStore, query string, args ...any) []string {
	t.Helper()
	rows, err := store.db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	var details []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scanning EXPLAIN QUERY PLAN row: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating EXPLAIN QUERY PLAN rows: %v", err)
	}
	return details
}

func TestQueryPlans(t *testing.T) {
	t.Parallel()
	store := testSQLiteStore(t)

	tests := []struct {
		name  string
		query string
		args  []any
		want  []string
	}{
		{
			name:  "FirstIndex",
			query: queryFirstIndex,
			want:  []string{"SEARCH logs"},
		},
		{
			name:  "LastIndex",
			query: queryLastIndex,
			want:  []string{"SEARCH logs"},
		},
		{
			name:  "GetLog",
			query: queryGetLog,
			args:  []any{1},
			want:  []string{"SEARCH logs USING INTEGER PRIMARY KEY (rowid=?)"},
		},
		{
			name:  "StoreLogs",
			query: queryStoreLogs,
			args:  []any{1, 1, 0, []byte{}, []byte{}, 0},
			want:  nil,
		},
		{
			name:  "DeleteRange",
			query: queryDeleteRange,
			args:  []any{1, 3},
			want:  []string{"SEARCH logs USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)"},
		},
		{
			name:  "Set",
			query: querySet,
			args:  []any{"key", []byte("value")},
			want:  nil,
		},
		{
			name:  "Get",
			query: queryGet,
			args:  []any{"key"},
			want:  []string{"SEARCH kv USING INDEX sqlite_autoindex_kv_1 (key=?)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := queryPlanDetails(t, store, tt.query, tt.args...)
			// Print actual details for debugging
			for i, d := range got {
				t.Logf("detail[%d]: %q", i, d)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d detail rows, want %d\ngot:  %q\nwant: %q", len(got), len(tt.want), got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("detail[%d]:\n  got:  %q\n  want: %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

