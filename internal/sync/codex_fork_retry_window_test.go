package sync

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

// writeOrphanCodexFork writes a local Codex fork whose explicit parent
// rollout does not exist, with the given modification time.
func writeOrphanCodexFork(t *testing.T, mtime time.Time) (string, string) {
	t.Helper()
	const childID = "22222222-2222-4222-8222-222222222222"
	const parentID = "11111111-1111-4111-8111-111111111111"
	codexDir := filepath.Join(t.TempDir(), "sessions")
	dayDir := filepath.Join(codexDir, "2024", "01", "01")
	require.NoError(t, os.MkdirAll(dayDir, 0o755))
	path := filepath.Join(dayDir, "rollout-2024-01-01T10-00-00-"+childID+".jsonl")
	require.NoError(t, os.WriteFile(path, []byte(testjsonl.JoinJSONL(
		testjsonl.CodexForkedSessionMetaJSON(
			childID, parentID, "/workspace/project", "codex_cli_rs",
			"2024-01-01T10:00:00Z",
		),
		testjsonl.CodexTurnContextWithIDJSON("gpt-5.4", "child-turn", "2024-01-01T10:00:01Z"),
		testjsonl.CodexMsgJSON("user", "child task", "2024-01-01T10:00:01Z"),
		testjsonl.CodexMsgJSON("assistant", "child answer", "2024-01-01T10:00:05Z"),
	)), 0o600))
	require.NoError(t, os.Chtimes(path, mtime, mtime))
	return codexDir, "codex:" + childID
}

func TestCodexForkWithLongMissingParentDoesNotDeferSync(t *testing.T) {
	stale := time.Now().Add(-codexForkParentRetryWindow - time.Hour)
	codexDir, sessionID := writeOrphanCodexFork(t, stale)
	database := openTestDB(t)
	engine := NewEngine(t.Context(), database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentCodex: {codexDir}},
		Machine:   "local",
	})
	t.Cleanup(engine.Close)

	stats := engine.SyncAll(t.Context(), nil)

	assert.Zero(t, stats.Deferred)
	assert.True(t, stats.ProcessingComplete())
	sess, err := database.GetSession(t.Context(), sessionID)
	require.NoError(t, err)
	require.NotNil(t, sess, "the orphaned fork stays visible")

	resync := engine.ResyncAll(t.Context(), nil)

	assert.False(t, resync.Aborted,
		"a parent missing for good must not block the resync swap")
	assert.False(t, database.NeedsResync())
}

func TestCodexForkWithRecentlyMissingParentStillDefers(t *testing.T) {
	codexDir, _ := writeOrphanCodexFork(t, time.Now())
	database := openTestDB(t)
	engine := NewEngine(t.Context(), database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentCodex: {codexDir}},
		Machine:   "local",
	})
	t.Cleanup(engine.Close)

	stats := engine.SyncAll(t.Context(), nil)

	assert.Equal(t, 1, stats.Deferred,
		"a parent may still appear while the child is fresh")
}
