package parser

import (
	"database/sql"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createHermesMarkerStateDB builds a current-schema state.db: transcript_revision
// on sessions plus the write-side trigger Hermes ships, so in-place rewrites move
// the revision.
func createHermesMarkerStateDB(t *testing.T, root string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(root, "state.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecContext(t.Context(), `
		CREATE TABLE sessions (
			id TEXT PRIMARY KEY, source TEXT NOT NULL, model TEXT,
			parent_session_id TEXT, started_at REAL NOT NULL, ended_at REAL,
			message_count INTEGER DEFAULT 0, input_tokens INTEGER DEFAULT 0,
			output_tokens INTEGER DEFAULT 0, cache_read_tokens INTEGER DEFAULT 0,
			cache_write_tokens INTEGER DEFAULT 0, reasoning_tokens INTEGER DEFAULT 0,
			estimated_cost_usd REAL, actual_cost_usd REAL, cost_status TEXT,
			cost_source TEXT, title TEXT, api_call_count INTEGER DEFAULT 0,
			transcript_revision INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
			role TEXT NOT NULL, content TEXT, tool_call_id TEXT, tool_calls TEXT,
			tool_name TEXT, timestamp REAL NOT NULL, finish_reason TEXT,
			reasoning TEXT, reasoning_content TEXT, reasoning_details TEXT,
			codex_reasoning_items TEXT, codex_message_items TEXT
		);
		CREATE INDEX idx_messages_session ON messages(session_id, timestamp);
		CREATE TRIGGER messages_transcript_revision_update
		AFTER UPDATE OF role, content, tool_calls, tool_call_id, tool_name,
			reasoning, reasoning_content, reasoning_details ON messages
		BEGIN
			UPDATE sessions SET transcript_revision = transcript_revision + 1
			WHERE id = new.session_id;
		END;
		INSERT INTO sessions (id, source, model, started_at, message_count, title)
		VALUES ('a', 'cli', 'opus', 1778767200.0, 1, 'A'),
		       ('b', 'cli', 'opus', 1778767300.0, 1, 'B');
		INSERT INTO messages (session_id, role, content, timestamp)
		VALUES ('a', 'user', 'hello a', 1778767210.0),
		       ('b', 'user', 'hello b', 1778767310.0);
	`)
	require.NoError(t, err)
	return db
}

func markerHash(t *testing.T, stateDB, id string) string {
	t.Helper()
	conn, err := openSQLiteReadOnly(stateDB, sqliteReadOptions{})
	require.NoError(t, err)
	defer conn.Close()
	h, _, ok := hermesStateMemberMarkerCoreConn(t.Context(), conn, stateDB, id)
	require.True(t, ok, "current schema must take the marker path")
	return hex.EncodeToString(h.Sum(nil))
}

// seededMemberCore runs the per-pass reconciliation seed the sync engine uses
// and returns one member's resumed digest.
func seededMemberCore(t *testing.T, stateDB, id string) string {
	t.Helper()
	ctx, cleanup, err := WithReconciliationCache(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()
	require.NoError(t, seedHermesMemberCoresLocked(ctx, stateDB))
	h, _, ok := cachedHermesMemberCore(ctx, hermesSource{Path: VirtualSourcePath(stateDB, id)})
	require.True(t, ok, id)
	return hex.EncodeToString(h.Sum(nil))
}

func TestHermesMemberMarkerChangesOnlyForTheChangedSession(t *testing.T) {
	root := t.TempDir()
	db := createHermesMarkerStateDB(t, root)
	stateDB := filepath.Join(root, "state.db")
	a0, b0 := markerHash(t, stateDB, "a"), markerHash(t, stateDB, "b")

	// Append to b: a's marker must not move (this is what keeps a sync
	// incremental), b's must.
	_, err := db.Exec(`INSERT INTO messages (session_id, role, content, timestamp)
		VALUES ('b', 'assistant', 'reply', 1778767320.0)`)
	require.NoError(t, err)
	a1, b1 := markerHash(t, stateDB, "a"), markerHash(t, stateDB, "b")
	assert.Equal(t, a0, a1)
	assert.NotEqual(t, b0, b1)

	// In-place rewrite of a's message: count and max id stay, the trigger
	// moves transcript_revision, so the marker still changes.
	_, err = db.Exec(`UPDATE messages SET content = 'redacted' WHERE session_id = 'a'`)
	require.NoError(t, err)
	a2 := markerHash(t, stateDB, "a")
	assert.NotEqual(t, a1, a2)

	// A timestamp rewrite moves SUM(timestamp).
	_, err = db.Exec(`UPDATE messages SET timestamp = timestamp + 1 WHERE session_id = 'a'`)
	require.NoError(t, err)
	assert.NotEqual(t, a2, markerHash(t, stateDB, "a"))

	// A delete moves COUNT(*).
	b2 := markerHash(t, stateDB, "b")
	_, err = db.Exec(`DELETE FROM messages WHERE session_id = 'b' AND role = 'assistant'`)
	require.NoError(t, err)
	b3 := markerHash(t, stateDB, "b")
	assert.NotEqual(t, b2, b3)

	// Session-row change (title) changes the marker too.
	_, err = db.Exec(`UPDATE sessions SET title = 'B renamed' WHERE id = 'b'`)
	require.NoError(t, err)
	assert.NotEqual(t, b3, markerHash(t, stateDB, "b"))
}

func TestHermesMemberMarkerFallsBackWithoutRevision(t *testing.T) {
	legacy := t.TempDir()
	createHermesStateDB(t, legacy) // no transcript_revision column
	stateDB := filepath.Join(legacy, "state.db")
	conn, err := openSQLiteReadOnly(stateDB, sqliteReadOptions{})
	require.NoError(t, err)
	defer conn.Close()
	_, _, ok := hermesStateMemberMarkerCoreConn(t.Context(), conn, stateDB, "child")
	assert.False(t, ok, "old schema must keep the content hash")
}

// The steady-state cost this guards: the per-pass seed must fingerprint a
// current-schema member without reading message content. A content rewrite
// that bypasses Hermes' revision trigger is invisible to the marker by design,
// so the seeded digest must not move; the content hash would.
func TestHermesMemberSeedUsesMarkerNotMessageContent(t *testing.T) {
	root := t.TempDir()
	db := createHermesMarkerStateDB(t, root)
	stateDB := filepath.Join(root, "state.db")
	before := seededMemberCore(t, stateDB, "a")
	assert.Equal(t, markerHash(t, stateDB, "a"), before,
		"the seed must cache the marker digest")

	_, err := db.Exec(`DROP TRIGGER messages_transcript_revision_update;
		UPDATE messages SET content = 'same length' WHERE session_id = 'a'`)
	require.NoError(t, err)
	assert.Equal(t, before, seededMemberCore(t, stateDB, "a"),
		"seed re-read message content instead of the change markers")

	// With the trigger in place an in-place rewrite still invalidates.
	_, err = db.Exec(`CREATE TRIGGER messages_transcript_revision_update
		AFTER UPDATE OF content ON messages BEGIN
			UPDATE sessions SET transcript_revision = transcript_revision + 1
			WHERE id = new.session_id;
		END;
		UPDATE messages SET content = 'rewritten' WHERE session_id = 'a'`)
	require.NoError(t, err)
	assert.NotEqual(t, before, seededMemberCore(t, stateDB, "a"))
}

// A transcript beside state.db stays a content-hashed candidate on top of the
// marker, so a transcript-only change still moves the member fingerprint.
func TestHermesMemberMarkerKeepsTranscriptCandidates(t *testing.T) {
	root := t.TempDir()
	createHermesMarkerStateDB(t, root)
	stateDB := filepath.Join(root, "state.db")
	writeSourceFile(t, filepath.Join(root, "sessions", "a.jsonl"), "{}\n")
	conn, err := openSQLiteReadOnly(stateDB, sqliteReadOptions{})
	require.NoError(t, err)
	defer conn.Close()
	_, candidates, ok := hermesStateMemberMarkerCoreConn(t.Context(), conn, stateDB, "a")
	require.True(t, ok)
	assert.Equal(t, []string{filepath.Join(root, "sessions", "a.jsonl")}, candidates)
}
