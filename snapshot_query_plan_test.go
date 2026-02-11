package raftsqlite

import (
	"testing"
)

func TestSnapshotQueryPlans(t *testing.T) {
	t.Parallel()
	store := testSnapshotStore(t)

	tests := []struct {
		name  string
		query string
		args  []any
		want  []string
	}{
		{
			name:  "List",
			query: snapshotQueryList,
			want:  []string{"SCAN snapshots", "USE TEMP B-TREE FOR ORDER BY"},
		},
		{
			name:  "Open",
			query: snapshotQueryOpen,
			args:  []any{"1-10-1234"},
			want:  []string{"SEARCH snapshots USING INDEX sqlite_autoindex_snapshots_1 (id=?)"},
		},
		{
			name:  "Reap",
			query: snapshotQueryReap,
			args:  []any{2},
			want:  []string{"SCAN snapshots", "LIST SUBQUERY 1", "SCAN snapshots", "USE TEMP B-TREE FOR ORDER BY"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := queryPlanDetails(t, store.db, tt.query, tt.args...)
			// Print actual details for debugging.
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
