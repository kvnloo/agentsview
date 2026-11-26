package artifact

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
)

// touchTestSession changes one session so the next export bumps the
// publication generation, the way a local provider sync does between
// continuous folder exchanges.
func touchTestSession(t *testing.T, database *db.DB, id string, round int) {
	t.Helper()
	require.NoError(t, database.ReplaceSessionMessages(t.Context(), id, []db.Message{
		{
			SessionID: id, Ordinal: 0, Role: "user",
			Content: fmt.Sprintf("prompt %d", round), ContentLength: 8,
		},
		{
			SessionID: id, Ordinal: 1, Role: "assistant",
			Content: fmt.Sprintf("response %d", round), ContentLength: 10,
		},
	}))
}

func drainTestArtifactSync(
	t *testing.T,
	database *db.DB,
	repository *Repository,
	opts SyncOptions,
	rounds int,
) (SyncResult, int) {
	t.Helper()
	var total SyncResult
	for round := 1; round <= rounds; round++ {
		result, err := SyncWithRepository(t.Context(), database, repository, opts)
		require.NoError(t, err)
		total.ExportedSessions += result.ExportedSessions
		total.ImportedSessions += result.ImportedSessions
		total.PublishedArtifacts += result.PublishedArtifacts
		total.ReceivedArtifacts += result.ReceivedArtifacts
		if !result.More {
			return total, round
		}
	}
	total.More = true
	return total, rounds
}

// A continuous sync re-exports a few sessions on every run, which changes the
// publication generation. Publishing that generation must cost work in
// proportion to the changed sessions, not to everything ever published.
func TestArtifactSyncIncrementalPushJournalsOnlyChangedPublications(
	t *testing.T,
) {
	t.Parallel()

	target := t.TempDir()
	database := testDB(t)
	repository, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repository.Close()) })
	opts := SyncOptions{Target: target, Origin: "laptop-a1b2c3"}

	// 80 sessions publish more objects than one exchange's object budget.
	const sessions = 80
	for index := range sessions {
		seedSession(t, database, fmt.Sprintf("s%03d", index), "alpha")
	}
	initial, rounds := drainTestArtifactSync(t, database, repository, opts, 10)
	require.False(t, initial.More, "initial publication must drain")
	require.Equal(t, sessions, initial.ExportedSessions)
	t.Logf("initial publication drained in %d exchanges", rounds)

	for cycle := 1; cycle <= 3; cycle++ {
		before := readTestFolderJournalSequence(t, target)
		touchTestSession(t, database, "s040", cycle)
		result, err := SyncWithRepository(t.Context(), database, repository, opts)
		require.NoError(t, err)
		assert.Equal(t, 1, result.ExportedSessions, "cycle %d", cycle)
		assert.False(t, result.More,
			"cycle %d: one changed session must publish in one exchange", cycle)
		journaled := readTestFolderJournalSequence(t, target) - before
		// One changed session: its new segment(s), its manifest, and the
		// new checkpoint. Re-journaling the unchanged closure is the bug.
		assert.LessOrEqual(t, journaled, int64(4),
			"cycle %d journaled %d events for one changed session", cycle, journaled)
		assert.Positive(t, journaled, "cycle %d must journal its change", cycle)
	}
}

// When the generation changes on every exchange while the backlog is larger
// than one exchange's budget, the push must still make progress and publish a
// checkpoint that a fresh peer can import completely.
func TestArtifactSyncConvergesWhileGenerationChangesEveryExchange(
	t *testing.T,
) {
	t.Parallel()

	target := t.TempDir()
	databaseA := testDB(t)
	databaseB := testDB(t)
	repositoryA, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repositoryA.Close()) })
	repositoryB, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repositoryB.Close()) })
	optsA := SyncOptions{Target: target, Origin: "laptop-a1b2c3"}
	optsB := SyncOptions{Target: target, Origin: "desktop-d4e5f6"}

	const sessions = 200
	for index := range sessions {
		seedSession(t, databaseA, fmt.Sprintf("s%03d", index), "alpha")
	}
	converged := false
	for exchange := 1; exchange <= 12; exchange++ {
		result, err := SyncWithRepository(t.Context(), databaseA, repositoryA, optsA)
		require.NoError(t, err)
		if !result.More {
			converged = true
			t.Logf("publisher converged after %d exchanges", exchange)
			break
		}
		// A local provider sync touches one session between exchanges.
		touchTestSession(t, databaseA, fmt.Sprintf("s%03d", exchange%sessions), exchange)
	}
	require.True(t, converged,
		"publisher never converged while the generation kept changing")

	imported, _ := drainTestArtifactSync(t, databaseB, repositoryB, optsB, 20)
	require.False(t, imported.More)
	for _, id := range []string{"s000", "s001", "s100", "s199"} {
		session, err := databaseB.GetSessionFull(t.Context(), optsA.Origin+"~"+id)
		require.NoError(t, err)
		require.NotNil(t, session, "peer is missing %s", id)
	}
	touched, err := databaseB.GetMessages(
		t.Context(), optsA.Origin+"~s001", 0, 10, true,
	)
	require.NoError(t, err)
	require.Len(t, touched, 2)
	assert.Equal(t, "response 1", touched[1].Content,
		"peer must receive the latest revision of a session changed mid-backlog")
}

// A journal entry whose object is absent from the folder (for example after a
// partial copy of the exchange folder) must not fail every later exchange. The
// reader records a rejection so the publisher's repair pass re-journals it.
func TestArtifactSyncPullSkipsAbsentJournaledObject(t *testing.T) {
	t.Parallel()

	target := t.TempDir()
	databaseA := testDB(t)
	databaseB := testDB(t)
	repositoryA, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repositoryA.Close()) })
	repositoryB, err := OpenRepository(t.Context(), t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repositoryB.Close()) })
	originA := "laptop-a1b2c3"
	optsA := SyncOptions{Target: target, Origin: originA}
	optsB := SyncOptions{Target: target, Origin: "desktop-d4e5f6"}
	seedSession(t, databaseA, "one", "alpha")
	seedSession(t, databaseA, "two", "alpha")
	// Distinct content gives each session its own content-addressed segment.
	touchTestSession(t, databaseA, "two", 2)

	_, err = SyncWithRepository(t.Context(), databaseA, repositoryA, optsA)
	require.NoError(t, err)
	segmentPaths, err := filepath.Glob(filepath.Join(
		target, originA, string(KindSegments), "*"+segmentExtension,
	))
	require.NoError(t, err)
	require.Len(t, segmentPaths, 2)
	require.NoError(t, os.Remove(segmentPaths[0]))

	first, err := SyncWithRepository(t.Context(), databaseB, repositoryB, optsB)
	require.NoError(t, err, "an absent journaled object must not fail the exchange")
	assert.False(t, first.More)
	assert.Equal(t, 1, first.ImportedSessions,
		"the session whose objects are present must still import")
	assert.FileExists(t, segmentPaths[0]+".rejected")

	again, err := SyncWithRepository(t.Context(), databaseB, repositoryB, optsB)
	require.NoError(t, err)
	assert.Zero(t, again.ImportedSessions)

	optsA.Full = true
	repaired, err := SyncWithRepository(t.Context(), databaseA, repositoryA, optsA)
	require.NoError(t, err)
	assert.Equal(t, 1, repaired.PublishedArtifacts)
	assert.FileExists(t, segmentPaths[0])

	healed, err := SyncWithRepository(t.Context(), databaseB, repositoryB, optsB)
	require.NoError(t, err)
	assert.Equal(t, 1, healed.ImportedSessions)
	for _, id := range []string{"one", "two"} {
		session, err := databaseB.GetSessionFull(t.Context(), originA+"~"+id)
		require.NoError(t, err)
		require.NotNil(t, session, "peer is missing %s", id)
	}
}
