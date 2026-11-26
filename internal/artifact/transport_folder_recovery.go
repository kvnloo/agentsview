package artifact

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"
)

const (
	// folderPublishIntentName lists the objects of the page an origin is
	// publishing. It is durable before any of them becomes visible and is
	// removed after all of them are journaled.
	folderPublishIntentName       = ".agentsview-publish-intent.json"
	folderPublishIntentTempPrefix = ".agentsview-publish-intent.tmp-"
	folderPublishIntentMaxBytes   = int64(4 << 20)
	// folderRejectionsPendingName tells an origin that a reader rejected one
	// of its objects, so the origin republishes rejected objects without a
	// full pass.
	folderRejectionsPendingName = ".agentsview-rejections-pending"
)

type folderPublishIntent struct {
	Objects []folderPublishIntentObject `json:"objects"`
}

type folderPublishIntentObject struct {
	Kind   Kind   `json:"kind"`
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

func (t *folderTransport) writeFolderPublishIntentLocked(
	originRoot *os.Root,
	entries []Entry,
) (retErr error) {
	intent := folderPublishIntent{
		Objects: make([]folderPublishIntentObject, 0, len(entries)),
	}
	for _, entry := range entries {
		intent.Objects = append(intent.Objects, folderPublishIntentObject{
			Kind:   entry.Ref.Kind,
			Name:   entry.Ref.Name,
			SHA256: entry.Identity.SHA256,
			Size:   entry.Identity.Size,
		})
	}
	body, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	tempName, file, err := createFolderTemp(
		originRoot,
		folderPublishIntentTempPrefix,
	)
	if err != nil {
		return err
	}
	tempExists := true
	defer func() {
		if tempExists {
			retErr = errors.Join(retErr, removeFolderFile(originRoot, tempName))
		}
	}()
	if _, err := file.Write(body); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := originRoot.Rename(tempName, folderPublishIntentName); err != nil {
		return err
	}
	tempExists = false
	return t.syncFolderDirectoryLocked(originRoot)
}

func (t *folderTransport) clearFolderPublishIntentLocked(
	originRoot *os.Root,
) error {
	if err := originRoot.Remove(folderPublishIntentName); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	return t.syncFolderDirectoryLocked(originRoot)
}

// recoverFolderPublishIntentLocked finishes a page that an interrupted
// exchange started. Each listed object is published again, which verifies or
// restores it, and journaled whether or not it already existed, because the
// interruption may have come between its publication and its journal entry.
func (t *folderTransport) recoverFolderPublishIntentLocked(
	ctx context.Context,
	store ArtifactStore,
	originRoot *os.Root,
	origin string,
) (published int, retErr error) {
	intent, found, err := t.readFolderPublishIntentLocked(originRoot)
	if err != nil || !found {
		return 0, err
	}
	for _, object := range intent.Objects {
		ref, err := NewRef(origin, object.Kind, object.Name)
		if err != nil {
			return published, err
		}
		identity, err := NewIdentity(object.SHA256, object.Size)
		if err != nil {
			return published, err
		}
		entry := Entry{Ref: ref, Identity: identity}
		created, err := t.republishFolderEntryLocked(ctx, store, originRoot, entry)
		if errors.Is(err, ErrArtifactNotFound) {
			// The local object is gone; the next pass decides whether the
			// current generation still needs it.
			continue
		}
		if err != nil {
			return published, err
		}
		if created {
			published++
		}
		if err := t.appendFolderJournalLocked(ctx, entry); err != nil {
			return published, err
		}
	}
	return published, t.clearFolderPublishIntentLocked(originRoot)
}

func (t *folderTransport) readFolderPublishIntentLocked(
	originRoot *os.Root,
) (folderPublishIntent, bool, error) {
	file, _, err := openFolderRegularFile(originRoot, folderPublishIntentName)
	if errors.Is(err, fs.ErrNotExist) {
		return folderPublishIntent{}, false, nil
	}
	if err != nil {
		return folderPublishIntent{}, false, err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, folderPublishIntentMaxBytes+1))
	if err != nil {
		return folderPublishIntent{}, false, err
	}
	var intent folderPublishIntent
	if int64(len(body)) <= folderPublishIntentMaxBytes {
		err = json.Unmarshal(body, &intent, json.RejectUnknownMembers(true))
	} else {
		err = errors.New("publish intent is too large")
	}
	if err != nil {
		// The intent is renamed into place whole, so an unreadable one was
		// not written by this protocol. Set it aside and let a later full
		// repair settle the folder.
		if quarantineErr := t.quarantineFolderEntryLocked(
			originRoot,
			folderPublishIntentName,
		); quarantineErr != nil {
			return folderPublishIntent{}, false, errors.Join(err, quarantineErr)
		}
		return folderPublishIntent{}, false, nil
	}
	return intent, true, nil
}

func (t *folderTransport) republishFolderEntryLocked(
	ctx context.Context,
	store ArtifactStore,
	originRoot *os.Root,
	entry Entry,
) (_ bool, retErr error) {
	kindRoot, err := t.ensureFolderSubrootLocked(
		originRoot,
		string(entry.Ref.Kind),
		"kind",
	)
	if err != nil {
		return false, err
	}
	defer func() { retErr = errors.Join(retErr, kindRoot.Close()) }()
	if _, err := store.Stat(ctx, entry.Ref); err != nil {
		return false, err
	}
	return t.publishFolderEntryLocked(ctx, store, kindRoot, entry)
}

// markFolderRejectionsPendingLocked records, next to an origin's objects, that
// a reader rejected one of them.
func (t *folderTransport) markFolderRejectionsPendingLocked(
	origin string,
) (retErr error) {
	originRoot, err := t.ensureFolderSubrootLocked(t.root, origin, "origin")
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, originRoot.Close()) }()
	if _, err := originRoot.Lstat(folderRejectionsPendingName); err == nil {
		return nil
	}
	file, err := originRoot.OpenFile(
		folderRejectionsPendingName,
		os.O_WRONLY|os.O_CREATE,
		0o600,
	)
	if err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	return t.syncFolderDirectoryLocked(originRoot)
}

// republishRejectedObjectsLocked restores this origin's objects that readers
// rejected as absent or corrupt, and journals them again so readers that
// advanced past the rejection retry them. Without it, a publication whose
// manifest is still present would never republish a lost segment.
func (t *folderTransport) republishRejectedObjectsLocked(
	ctx context.Context,
	store ArtifactStore,
	originRoot *os.Root,
	origin string,
) (published int, retErr error) {
	if _, err := originRoot.Lstat(folderRejectionsPendingName); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	// Checkpoints are sequence numbered; an old one is never republished.
	// The current checkpoint is verified on every new generation.
	for _, kind := range [...]Kind{KindSegments, KindManifests} {
		kindRoot, err := openOptionalFolderSubroot(originRoot, string(kind), "kind")
		if err != nil {
			return published, err
		}
		if kindRoot == nil {
			continue
		}
		var rejected []string
		err = t.visitFolderDirectory(ctx, kindRoot, ".", func(entry os.DirEntry) error {
			name, found := strings.CutSuffix(entry.Name(), ".rejected")
			if found && entry.Type().IsRegular() {
				rejected = append(rejected, name)
			}
			return nil
		})
		for _, wireName := range rejected {
			if err != nil {
				break
			}
			var created bool
			created, err = t.republishRejectedObjectLocked(
				ctx,
				store,
				kindRoot,
				origin,
				kind,
				wireName,
			)
			if created {
				published++
			}
		}
		if closeErr := kindRoot.Close(); err != nil || closeErr != nil {
			return published, errors.Join(err, closeErr)
		}
	}
	if err := originRoot.Remove(folderRejectionsPendingName); err != nil &&
		!errors.Is(err, fs.ErrNotExist) {
		return published, err
	}
	return published, t.syncFolderDirectoryLocked(originRoot)
}

func (t *folderTransport) republishRejectedObjectLocked(
	ctx context.Context,
	store ArtifactStore,
	kindRoot *os.Root,
	origin string,
	kind Kind,
	wireName string,
) (bool, error) {
	ref, err := FromWireRef(origin, kind, wireName)
	if err != nil {
		// Not a marker this protocol writes; leave it alone.
		return false, nil
	}
	entry, err := store.Stat(ctx, ref)
	if errors.Is(err, ErrArtifactNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := validateFolderJournalRejection(
		kindRoot,
		wireName,
		entry.Identity,
	); err != nil {
		if errors.Is(err, ErrArtifactInvalid) ||
			errors.Is(err, ErrArtifactConflict) {
			// The marker names other content; a full repair settles it.
			return false, nil
		}
		return false, err
	}
	created, err := t.publishFolderEntryLocked(ctx, store, kindRoot, entry)
	if err != nil {
		return false, err
	}
	if err := t.appendFolderJournalLocked(ctx, entry); err != nil {
		return created, err
	}
	return created, t.clearFolderJournalRejectionLocked(kindRoot, wireName)
}
