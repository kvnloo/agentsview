package remotesync

import (
	"os"
	"path/filepath"
	"testing"
)

// A multi-GB state.db snapshot must not default to /tmp, which is often a small
// tmpfs; with TMPDIR unset it goes under the user cache dir. An explicit TMPDIR wins.
func TestSQLiteSnapshotTempRoot(t *testing.T) {
	cache := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", cache)
	t.Setenv("TMPDIR", "")
	if got, want := sqliteSnapshotTempRoot(), filepath.Join(cache, "agentsview", "tmp"); got != want {
		t.Fatalf("TMPDIR unset: got %q, want %q", got, want)
	}
	if _, err := os.Stat(filepath.Join(cache, "agentsview", "tmp")); err != nil {
		t.Fatalf("cache tmp dir not created: %v", err)
	}
	explicit := t.TempDir()
	t.Setenv("TMPDIR", explicit)
	if got := sqliteSnapshotTempRoot(); got != "" {
		t.Fatalf("explicit TMPDIR must be honoured via os.TempDir, got %q", got)
	}
}
