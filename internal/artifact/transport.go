package artifact

import (
	"context"
)

// Transport exchanges immutable artifact objects through their external wire
// representation. Implementations pull supported peer objects but publish only
// the explicitly authorized local origin. They never receive the Docbank
// physical layout.
type Transport interface {
	Prepare(context.Context, ArtifactStore) error
	Exchange(
		context.Context,
		ArtifactStore,
		string,
	) (ExchangeResult, error)
	Close() error
}

// ExchangeResult reports the logical effects of one transport exchange.
type ExchangeResult struct {
	Received    int
	Published   int
	Quarantined int
	// Skipped counts journaled objects a pull-only exchange could not import
	// because they were absent or invalid in the folder. A pull-only reader
	// cannot leave a rejection marker, so it skips them; an explicit full
	// pull-only sync replays the journal from the start.
	Skipped int
	More    bool
}

// FolderTransportOptions configures a bounded folder exchange. RepairPublished
// verifies an already-completed authoritative generation and journals only
// restored or rejection-marked objects so advanced peers can retry them. It is
// intended for explicit full synchronization.
type FolderTransportOptions struct {
	ForbiddenRoots  []string
	MaxObjects      int
	MaxBytes        int64
	StateStore      FolderTransportStateStore
	RepairPublished bool
	// PullOnly opens an existing marked folder as a read-only source. The
	// exchange imports journaled peer objects but never publishes, locks,
	// quarantines, or writes anything in the folder, so the folder can be a
	// one-way copy of another machine's target. With RepairPublished, the
	// journal is replayed from the first entry.
	PullOnly bool
}

// FolderTransportStateStore persists target-bound continuation state between
// bounded exchanges. Implementations must treat namespaceID as an opaque key.
type FolderTransportStateStore interface {
	LoadFolderTransportState(context.Context, string) (string, error)
	SaveFolderTransportState(context.Context, string, string) error
}

type transportChangeRecorder interface {
	RecordTransportChanged(context.Context, Entry) error
}
