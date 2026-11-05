package parser

import (
	"database/sql"
	"encoding/hex"
	"hash"
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

func markerFor(t *testing.T, stateDB, id string) (hash.Hash, bool) {
	t.Helper()
	conn, err := openSQLiteReadOnly(stateDB, sqliteReadOptions{})
	require.NoError(t, err)
	defer conn.Close()
	return hermesStateMemberMarkerHash(t.Context(), conn, stateDB, id)
}

func markerOK(t *testing.T, stateDB, id string) bool {
	_, ok := markerFor(t, stateDB, id)
	return ok
}

func markerHash(t *testing.T, stateDB, id string) string {
	t.Helper()
	h, ok := markerFor(t, stateDB, id)
	require.True(t, ok, "current schema must take the marker path")
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
	assert.NotEqual(t, a1, markerHash(t, stateDB, "a"))

	// Session-row change (title) changes the marker too.
	b2 := markerHash(t, stateDB, "b")
	_, err = db.Exec(`UPDATE sessions SET title = 'B renamed' WHERE id = 'b'`)
	require.NoError(t, err)
	assert.NotEqual(t, b2, markerHash(t, stateDB, "b"))
}

func TestHermesMemberMarkerFallsBackWithoutRevisionOrWithTranscript(t *testing.T) {
	legacy := t.TempDir()
	createHermesStateDB(t, legacy) // no transcript_revision column
	ok := markerOK(t, filepath.Join(legacy, "state.db"), "child")
	assert.False(t, ok, "old schema must keep the content hash")

	root := t.TempDir()
	createHermesMarkerStateDB(t, root)
	writeSourceFile(t, filepath.Join(root, "sessions", "a.jsonl"), "{}\n")
	ok = markerOK(t, filepath.Join(root, "state.db"), "a")
	assert.False(t, ok, "a transcript beside state.db needs the content comparison")
	ok = markerOK(t, filepath.Join(root, "state.db"), "b")
	assert.True(t, ok)
}
