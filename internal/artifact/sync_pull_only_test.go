package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshotTestFolder records every entry under root with its mode, size,
// modification time and content hash, so a test can prove nothing changed.
func snapshotTestFolder(t *testing.T, root string) map[string]string {
	t.Helper()
	snapshot := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(
		path string,
		entry fs.DirEntry,
		err error,
	) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		value := fmt.Sprintf("%v %d %d", info.Mode(), info.Size(), info.ModTime().UnixNano())
		if info.Mode().IsRegular() {
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(body)
			value += " " + hex.EncodeToString(sum[:])
		}
		snapshot[rel] = value
		return nil
	}))
	return snapshot
}

// mirrorTestFolder copies src into dst one way, the way the replica's rsync
// does: objects first, then the journal, each file through a temporary name
// and a rename. Files already present with the same content are left alone.
// The exchange lock is not copied.
func mirrorTestFolder(t *testing.T, src, dst string) {
	t.Helper()
	copyPhase := func(journal bool) {
		require.NoError(t, filepath.WalkDir(src, func(
			path string,
			entry fs.DirEntry,
			err error,
		) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(src, path)
			if err != nil {
				return err
			}
			inJournal := rel == folderJournalDirectory ||
				strings.HasPrefix(rel, folderJournalDirectory+string(filepath.Separator))
			if entry.IsDir() {
				return os.MkdirAll(filepath.Join(dst, rel), 0o755)
			}
			if inJournal != journal || rel == folderExchangeLockName {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			destination := filepath.Join(dst, rel)
			if existing, err := os.ReadFile(destination); err == nil &&
				string(existing) == string(body) {
				return nil
			}
			temporary := destination + ".rsync-tmp"
			if err := os.WriteFile(temporary, body, 0o644); err != nil {
				return err
			}
			return os.Rename(temporary, destination)
		}))
	}
	require.NoError(t, os.MkdirAll(dst, 0o755))
	copyPhase(false)
	copyPhase(true)
}

// A replica imports a one-way copy of another machine's target without ever
// writing to it, and keeps up while the origin changes sessions every cycle.
func TestArtifactSyncPullOnlyReplicaFollowsCopiedFolderWithoutWriting(
	t *testing.T,
) {
	t.Parallel()

	hub := t.TempDir()
	replica := filepath.Join(t.TempDir(), "replica")
	databaseA := testDB(t)
	databaseB := testDB(t)
	repositoryA, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repositoryA.Close()) })
	repositoryB, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repositoryB.Close()) })
	originA := "studio-a1b2c3"
	optsA := SyncOptions{Target: hub, Origin: originA}
	optsB := SyncOptions{Target: replica, Origin: "groot-d4e5f6", PullOnly: true}

	// More objects than one exchange's budget, so the replica drains in pages.
	const sessions = 80
	for index := range sessions {
		id := fmt.Sprintf("s%03d", index)
		seedSession(t, databaseA, id, "alpha")
		// Distinct content gives each session its own segment.
		touchTestSession(t, databaseA, id, 1000+index)
	}
	initial, _ := drainTestArtifactSync(t, databaseA, repositoryA, optsA, 10)
	require.False(t, initial.More)
	require.Greater(t, readTestFolderJournalSequence(t, hub),
		int64(folderExchangeMaxObjects),
		"the journal must be larger than one pull page")

	for cycle := 1; cycle <= 3; cycle++ {
		mirrorTestFolder(t, hub, replica)
		before := snapshotTestFolder(t, replica)
		pulled, rounds := drainTestArtifactSync(t, databaseB, repositoryB, optsB, 10)
		require.False(t, pulled.More, "cycle %d: replica must drain", cycle)
		assert.Zero(t, pulled.PublishedArtifacts, "cycle %d", cycle)
		assert.Zero(t, pulled.ExportedSessions, "cycle %d", cycle)
		assert.Equal(t, before, snapshotTestFolder(t, replica),
			"cycle %d: a pull-only sync must not write to the folder", cycle)
		t.Logf("cycle %d: replica drained in %d exchanges, imported %d sessions",
			cycle, rounds, pulled.ImportedSessions)

		messages, err := databaseB.GetMessages(
			t.Context(), originA+"~s040", 0, 10, true,
		)
		require.NoError(t, err)
		if cycle == 1 {
			for _, id := range []string{"s000", "s040", "s079"} {
				session, err := databaseB.GetSessionFull(t.Context(), originA+"~"+id)
				require.NoError(t, err)
				require.NotNil(t, session, "replica is missing %s", id)
			}
		} else {
			require.Len(t, messages, 2)
			assert.Equal(t, fmt.Sprintf("response %d", cycle-1), messages[1].Content,
				"cycle %d: replica must have the latest revision", cycle)
			assert.Equal(t, 1, pulled.ImportedSessions,
				"cycle %d: only the changed session moves", cycle)
		}

		touchTestSession(t, databaseA, "s040", cycle)
		published, err := SyncWithRepository(t.Context(), databaseA, repositoryA, optsA)
		require.NoError(t, err)
		require.False(t, published.More)
	}

	// Nothing new: a quiet run imports nothing and still writes nothing.
	mirrorTestFolder(t, hub, replica)
	_, err = SyncWithRepository(t.Context(), databaseB, repositoryB, optsB)
	require.NoError(t, err)
	before := snapshotTestFolder(t, replica)
	quiet, err := SyncWithRepository(t.Context(), databaseB, repositoryB, optsB)
	require.NoError(t, err)
	assert.Zero(t, quiet.ImportedSessions)
	assert.Zero(t, quiet.ReceivedArtifacts)
	assert.Equal(t, before, snapshotTestFolder(t, replica))

	// A full pull-only sync replays the whole journal, which is larger than
	// one page, in a single call.
	full := optsB
	full.Full = true
	replayed, err := SyncWithRepository(t.Context(), databaseB, repositoryB, full)
	require.NoError(t, err)
	assert.False(t, replayed.More)
	assert.Equal(t, before, snapshotTestFolder(t, replica))
}

// A pull-only reader must never initialize a folder, including an empty one
// or one that does not exist yet.
func TestArtifactSyncPullOnlyRequiresExistingMarkedFolder(t *testing.T) {
	t.Parallel()

	database := testDB(t)
	repository, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repository.Close()) })

	empty := t.TempDir()
	_, err = SyncWithRepository(t.Context(), database, repository, SyncOptions{
		Target: empty, Origin: "groot-d4e5f6", PullOnly: true,
	})
	require.Error(t, err)
	entries, err := os.ReadDir(empty)
	require.NoError(t, err)
	assert.Empty(t, entries, "a pull-only sync must not create a marker")

	missing := filepath.Join(t.TempDir(), "missing")
	_, err = SyncWithRepository(t.Context(), database, repository, SyncOptions{
		Target: missing, Origin: "groot-d4e5f6", PullOnly: true,
	})
	require.Error(t, err)
	assert.NoDirExists(t, missing)
}

// An object missing from the copy is skipped without a rejection marker, and
// a full pull-only sync imports it once a later copy delivers it.
func TestArtifactSyncPullOnlySkipsAbsentObjectWithoutWriting(t *testing.T) {
	t.Parallel()

	hub := t.TempDir()
	replica := filepath.Join(t.TempDir(), "replica")
	databaseA := testDB(t)
	databaseB := testDB(t)
	repositoryA, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repositoryA.Close()) })
	repositoryB, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repositoryB.Close()) })
	originA := "studio-a1b2c3"
	optsA := SyncOptions{Target: hub, Origin: originA}
	optsB := SyncOptions{Target: replica, Origin: "groot-d4e5f6", PullOnly: true}
	seedSession(t, databaseA, "one", "alpha")
	seedSession(t, databaseA, "two", "alpha")
	touchTestSession(t, databaseA, "two", 2)
	_, err = SyncWithRepository(t.Context(), databaseA, repositoryA, optsA)
	require.NoError(t, err)

	mirrorTestFolder(t, hub, replica)
	segmentPaths, err := filepath.Glob(filepath.Join(
		replica, originA, string(KindSegments), "*"+segmentExtension,
	))
	require.NoError(t, err)
	require.Len(t, segmentPaths, 2)
	require.NoError(t, os.Remove(segmentPaths[0]))
	before := snapshotTestFolder(t, replica)

	first, err := SyncWithRepository(t.Context(), databaseB, repositoryB, optsB)
	require.NoError(t, err, "an absent object must not fail a pull-only sync")
	assert.Equal(t, 1, first.SkippedArtifacts)
	assert.Equal(t, 1, first.ImportedSessions)
	assert.NoFileExists(t, segmentPaths[0]+".rejected")
	assert.Equal(t, before, snapshotTestFolder(t, replica))

	// The next copy delivers the object again; a full pull-only sync heals.
	mirrorTestFolder(t, hub, replica)
	require.FileExists(t, segmentPaths[0])
	healed, err := SyncWithRepository(t.Context(), databaseB, repositoryB,
		SyncOptions{Target: replica, Origin: "groot-d4e5f6", PullOnly: true, Full: true})
	require.NoError(t, err)
	assert.Zero(t, healed.SkippedArtifacts)
	for _, id := range []string{"one", "two"} {
		session, err := databaseB.GetSessionFull(t.Context(), originA+"~"+id)
		require.NoError(t, err)
		require.NotNil(t, session, "replica is missing %s", id)
	}
}
