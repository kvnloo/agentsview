package sync

import (
	"time"

	"go.kenn.io/agentsview/internal/parser"
)

// codexForkParentRetryWindow bounds how long an explicit Codex fork whose
// parent rollout cannot be read stays marked for retry. Codex writes a
// subagent's parent rollout before the child, so a local parent still
// unreadable this long after the child's last write was deleted or never
// archived. Retrying it forever defers every sync and blocks the
// data-version resync swap, which leaves the archive permanently stale.
const codexForkParentRetryWindow = 24 * time.Hour

// settleLongUnresolvedCodexForks treats retry results from a local
// Codex-format source unchanged for longer than codexForkParentRetryWindow
// as final. The child rows stay exactly as parsed (fail-open: replayed parent
// history is kept). S3 sources keep retrying because an uploader can publish
// the parent long after the child.
func settleLongUnresolvedCodexForks(
	agent parser.AgentType,
	path string,
	mtimeNS int64,
	now time.Time,
	results []parser.ParseResultOutcome,
) {
	if !isCodexFormatAgent(agent) || isS3SourcePath(path) || mtimeNS == 0 ||
		now.Sub(time.Unix(0, mtimeNS)) <= codexForkParentRetryWindow {
		return
	}
	for i := range results {
		if results[i].DataVersion == parser.DataVersionNeedsRetry {
			results[i].DataVersion = parser.DataVersionCurrent
			results[i].RetryReason = ""
		}
	}
}
