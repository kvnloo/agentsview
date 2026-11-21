package parser

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"hash"
	"path/filepath"
)

// hermesStateMemberMarkerCoreConn fingerprints the DB side of one state.db
// member from change markers instead of message content.
//
// Every member inherits its container's size/mtime, so on a live profile every
// member's stat mismatches on every sync and freshness falls to this hash. The
// content hash (addHermesStateSessionFingerprint) covers every message of every
// session, which made each steady-state sync re-read whole archives (groot,
// 2026-11-21: ~7 GB of state.db pages per hourly run, clean alone 3.7 GB) only
// to prove nothing changed.
//
// Hermes bumps sessions.transcript_revision on every in-place rewrite of the
// message columns this parser reads (triggers messages_transcript_revision_update
// and messages_display_revision_update); appends and deletes move
// COUNT(*)/MAX(id); timestamp rewrites move SUM(timestamp). Those come from the
// sessions row plus the (session_id, timestamp) index, so an unchanged member
// costs a few index pages. The session row, model usage and cron job are
// hashed exactly as the content hash does. Transcript candidates beside
// state.db are still hashed by content in hermesStateMemberFingerprintFinish.
//
// ok=false (callers use the content hash) when the database predates
// transcript_revision or the row is missing or NULL.
func hermesStateMemberMarkerCoreConn(
	ctx context.Context, conn *sql.DB, stateDB, rawSessionID string,
) (hash.Hash, []string, bool) {
	var revision sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		`SELECT transcript_revision FROM sessions WHERE id = ?`, rawSessionID,
	).Scan(&revision); err != nil || !revision.Valid {
		return nil, nil, false // no such column, no row, or NULL
	}
	ss, found, err := readHermesStateSession(ctx, conn, rawSessionID)
	if err != nil || !found {
		return nil, nil, false
	}
	if err := resolveHermesStateCronJobConn(ctx, conn, &ss); err != nil {
		return nil, nil, false
	}
	var count, maxID int64
	var tsSum float64
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MAX(id), 0), COALESCE(SUM(timestamp), 0)
		FROM messages WHERE session_id = ?`, rawSessionID,
	).Scan(&count, &maxID, &tsSum); err != nil {
		return nil, nil, false
	}
	h := sha256.New()
	if ss.source == "cron" {
		if _, err := fmt.Fprintf(h, "cron-job\x00%q\x00", ss.cronJob); err != nil {
			return nil, nil, false
		}
	}
	if _, err := fmt.Fprintf(h,
		"state-member-marker-v1\x00%q\x00%q\x00%q\x00%q\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%t\x00%g\x00%t\x00%g\x00%q\x00%q\x00%q\x00%d\x00%d\x00%d\x00%g\x00",
		ss.id, ss.source, ss.model, ss.parentSessionID,
		ss.startedAt.UnixNano(), ss.endedAt.UnixNano(), ss.messageCount,
		ss.inputTokens, ss.outputTokens, ss.cacheReadTokens,
		ss.cacheWriteTokens, ss.reasoningTokens, ss.apiCallCount,
		ss.estimatedCost.Valid, ss.estimatedCost.Float64,
		ss.actualCost.Valid, ss.actualCost.Float64,
		ss.costStatus, ss.costSource, ss.title,
		revision.Int64, count, maxID, tsSum,
	); err != nil {
		return nil, nil, false
	}
	if err := json.MarshalWrite(h, ss.modelUsage); err != nil {
		return nil, nil, false
	}
	sessionsDir := filepath.Join(filepath.Dir(stateDB), "sessions")
	var candidates []string
	for _, p := range []string{
		filepath.Join(sessionsDir, "session_"+rawSessionID+".json"),
		filepath.Join(sessionsDir, rawSessionID+".jsonl"),
	} {
		if IsRegularFile(p) {
			candidates = append(candidates, p)
		}
	}
	return h, candidates, true
}
