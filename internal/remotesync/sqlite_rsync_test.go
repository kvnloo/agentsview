package remotesync

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rsyncFakeSource struct{ Source }

func (rsyncFakeSource) SQLiteRsyncOrigin(remotePath string) string { return "fake:" + remotePath }

// fakeRsync installs a sqlite3_rsync stand-in that runs body and records its argv.
func fakeRsync(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "sqlite3_rsync")
	require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh\necho \"$@\" > "+dir+"/argv\n"+body+"\n"), 0o755))
	old := sqlite3RsyncPath
	sqlite3RsyncPath = func() (string, error) { return bin, nil }
	t.Cleanup(func() { sqlite3RsyncPath = old })
	return dir
}

func TestRsyncSQLiteSnapshotsReplacesArchiveFetchForStateDB(t *testing.T) {
	origin := filepath.Join(t.TempDir(), "origin.db")
	require.NoError(t, os.WriteFile(origin, []byte("new pages"), 0o600))
	t.Setenv("FAKE_ORIGIN", origin)
	argvDir := fakeRsync(t, `cp "$FAKE_ORIGIN" "$2"`)

	mirror := t.TempDir()
	const stateDB = "/srv/agent/.hermes/state.db"
	const other = "/srv/agent/.hermes/sessions/a.json"
	mtime := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	entry := ManifestEntry{Path: stateDB, Size: int64(len("new pages")), MtimeNS: mtime.UnixNano()}
	hs := RemoteSync{Host: "mbp", Source: rsyncFakeSource{}}

	left := hs.rsyncSQLiteSnapshots(t.Context(), []string{stateDB, other},
		map[string]ManifestEntry{stateDB: entry}, mirror)

	assert.Equal(t, []string{other}, left, "state.db must not be re-shipped in the archive")
	local, err := safeRemappedRemotePath(mirror, stateDB)
	require.NoError(t, err)
	got, err := os.ReadFile(local)
	require.NoError(t, err)
	assert.Equal(t, "new pages", string(got))
	argv, err := os.ReadFile(filepath.Join(argvDir, "argv"))
	require.NoError(t, err)
	assert.Equal(t, "fake:"+stateDB+" "+local+"\n", string(argv))

	// The stamped replica matches its manifest entry, so the next diff
	// does not fetch it again.
	diff := newMirrorDiff(mirror)
	defer diff.close()
	require.NoError(t, diff.add(entry))
	delta, err := diff.finish()
	require.NoError(t, err)
	defer delta.Close()
	assert.Zero(t, delta.Fetch.Len())
}

func TestRsyncSQLiteSnapshotsFallsBackToArchiveOnFailure(t *testing.T) {
	fakeRsync(t, "exit 3")
	const stateDB = "/srv/agent/.hermes/state.db"
	entries := map[string]ManifestEntry{stateDB: {Path: stateDB, Size: 1, MtimeNS: 1}}
	hs := RemoteSync{Host: "mbp", Source: rsyncFakeSource{}}
	left := hs.rsyncSQLiteSnapshots(context.Background(), []string{stateDB}, entries, t.TempDir())
	assert.Equal(t, []string{stateDB}, left)
}

func TestRsyncSQLiteSnapshotsNeedsCapableSource(t *testing.T) {
	fakeRsync(t, "exit 0")
	const stateDB = "/srv/agent/.hermes/state.db"
	entries := map[string]ManifestEntry{stateDB: {Path: stateDB, Size: 1, MtimeNS: 1}}
	hs := RemoteSync{Host: "mbp"} // e.g. the HTTP source: no rsync origin
	left := hs.rsyncSQLiteSnapshots(context.Background(), []string{stateDB}, entries, t.TempDir())
	assert.Equal(t, []string{stateDB}, left)
}

func TestRsyncSQLiteSnapshotsDisabledByEnv(t *testing.T) {
	t.Setenv("AGENTSVIEW_NO_SQLITE3_RSYNC", "1")
	_, err := sqlite3RsyncPath()
	assert.Error(t, err)
}
