package sync_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/sync"
)

// A one-shot `agentsview sync` is a fresh engine every run. An unchanged OMP
// transcript must be provable from a persisted stat digest; without one the
// engine re-hashes the whole file, which on groot re-read 4.4 GB per run.
func TestOMPUnchangedSourceSkipsContentHashInFreshEngine(t *testing.T) {
	env := setupTestEnv(t)
	path := env.writeSession(t, env.ompDir,
		filepath.Join("encoded-cwd", "omp-digest.jsonl"),
		piLikeProviderFixture("omp-digest", "/Users/alice/code/omp-app"),
	)
	runSyncAndAssert(t, env.engine, sync.SyncStats{TotalSessions: 1, Synced: 1})

	digest, ok, err := env.db.GetProviderStatHash(t.Context(), parser.AgentOMP, path)
	require.NoError(t, err)
	require.True(t, ok, "a committed OMP write must persist its stat digest")
	require.NotZero(t, digest)

	fresh := sync.NewEngine(t.Context(), env.db, sync.EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentOMP: {env.ompDir}},
		Machine:   "local",
	})
	t.Cleanup(fresh.Close)
	require.Equal(t, 0, fresh.SyncAll(t.Context(), nil).Synced)
	again, ok, err := env.db.GetProviderStatHash(t.Context(), parser.AgentOMP, path)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, digest, again, "an unchanged transcript must keep its digest")

	dbtest.WriteTestFile(t, path, []byte(piLikeProviderFixture("omp-digest", "/Users/alice/code/omp-moved")))
	require.Equal(t, 1, fresh.SyncAll(t.Context(), nil).Synced,
		"a rewritten transcript must break the digest and reparse")
}
