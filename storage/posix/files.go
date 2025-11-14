// Copyright 2024 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package posix

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/fetcher"
	"github.com/transparency-dev/tessera/internal/migrate"
	"github.com/transparency-dev/tessera/internal/parse"
	storage "github.com/transparency-dev/tessera/storage/internal"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/klog/v2"
)

const (
	// compatibilityVersion is the required version of the log state directory.
	// This should be bumped whenever a change is made that would break compatibility with old versions.
	// When this is bumped, ensure that the version file is only written when a new log is being
	// created. Currently, this version is written whenever it is missing in order to upgrade logs
	// that were created before we introduced this.
	compatibilityVersion = 1

	// stateDir holds any private (but not secret) internal state needed to maintain/operate the log.
	stateDir = ".state"
	// gcStateFile contains the state of the garbage collection operations.
	gcStateFile = "gcState"
	// gcStateLock must be held when performing GC operations and updating the gcState file.
	gcStateLock = gcStateFile + ".lock"
	// publishLock must be held when checking/updating the published checkpoint.
	publishLock = "publish.lock"
	// treeStateFile contains the integrated (but not necessarily published) state of the tree.
	treeStateFile = "treeState"
	// treeStateLock must be held when integrating entries into the tree or writing to the treeState file.
	treeStateLock = treeStateFile + ".lock"

	minCheckpointInterval = 100 * time.Millisecond
)

// Storage implements storage functions for a POSIX filesystem.
// It leverages the POSIX atomic operations where needed.
type Storage struct {
	mu  sync.Mutex
	cfg Config
}

// appender implements the Tessera append lifecycle.
type appender struct {
	s          *Storage
	logStorage *logResourceStorage
	queue      *storage.Queue

	curSize uint64
	newCP   func(context.Context, uint64, []byte) ([]byte, error) // May be nil for mirrored logs.

	cpUpdated chan struct{}
}

// logResourceStorage knows how to read and write tiled log resources via a
// POSIX storage instance
type logResourceStorage struct {
	s           *Storage
	entriesPath func(uint64, uint8) string
}

// NewTreeFunc is the signature of a function which receives information about newly integrated trees.
type NewTreeFunc func(size uint64, root []byte) error

type Config struct {
	// HTTPClient will be used for outgoing HTTP requests. If unset, Tessera will use the net/http DefaultClient.
	HTTPClient *http.Client

	// Path is the path to a directory in which the log should be stored.
	Path string
}

// New creates a new POSIX storage.
func New(ctx context.Context, cfg Config) (tessera.Driver, error) {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}

	return &Storage{
		cfg: cfg,
	}, nil
}

func (s *Storage) Appender(ctx context.Context, opts *tessera.AppendOptions) (*tessera.Appender, tessera.LogReader, error) {
	logStorage := &logResourceStorage{
		s:           s,
		entriesPath: opts.EntriesPath(),
	}

	a, lr, err := s.newAppender(ctx, logStorage, opts)
	if err != nil {
		return nil, nil, err
	}

	return &tessera.Appender{
		Add: a.Add,
	}, lr, nil
}

func (s *Storage) newAppender(ctx context.Context, o *logResourceStorage, opts *tessera.AppendOptions) (*appender, tessera.LogReader, error) {
	if opts.CheckpointInterval() < minCheckpointInterval {
		return nil, nil, fmt.Errorf("requested CheckpointInterval (%v) is less than minimum permitted %v", opts.CheckpointInterval(), minCheckpointInterval)
	}

	a := &appender{
		s:          s,
		logStorage: o,
		cpUpdated:  make(chan struct{}),
		newCP:      opts.CheckpointPublisher(o, s.cfg.HTTPClient),
	}
	if err := a.initialise(ctx); err != nil {
		return nil, nil, err
	}
	a.queue = storage.NewQueue(ctx, opts.BatchMaxAge(), opts.BatchMaxSize(), a.sequenceBatch)

	go func(ctx context.Context, i time.Duration) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-a.cpUpdated:
			case <-time.After(i):
			}
			if err := a.publishCheckpoint(ctx, i, opts.CheckpointRepublishInterval()); err != nil {
				klog.Warningf("publishCheckpoint: %v", err)
			}
		}
	}(ctx, opts.CheckpointInterval())
	if i := opts.GarbageCollectionInterval(); i > 0 {
		go a.garbageCollectorJob(ctx, i)
	}

	return a, a.logStorage, nil
}

// lockFile creates/opens a lock file at the specified path, and flocks it.
// Once locked, the caller perform whatever operations are necessary, before
// calling the returned function to unlock it.
//
// Note that a) this is advisory, and b) should use an non-API specified file
// (e.g. <something>.lock>) to avoid inherent brittleness of the `fcntrl` API
// (*any* `Close` operation on this file (even if it's a different FD) from
// this PID, or overwriting of the file by *any* process breaks the lock.)
func (s *Storage) lockFile(ctx context.Context, p string) (func() error, error) {
	now := time.Now()

	p = filepath.Join(s.cfg.Path, stateDir, p)
	f, err := os.OpenFile(p, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC, filePerm)
	if err != nil {
		return nil, err
	}

	flockT := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: io.SeekStart,
		Start:  0,
		Len:    0,
	}
	// Keep trying until we manage to get an answer without being interrupted.
	for {
		if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLKW, &flockT); err != syscall.EINTR {
			if err == nil {
				posixOpsHistogram.Record(ctx, time.Since(now).Milliseconds(), metric.WithAttributes(opNameKey.String(fmt.Sprintf("lock-%s", p))))
			}
			return f.Close, err
		}
	}

}

// Add takes an entry and queues it for inclusion in the log.
// Upon placing the entry in an in-memory queue to be sequenced, it returns a future that will
// evaluate to either the sequence number assigned to this entry, or an error.
// This future is made available when the entry is queued. Any further calls to Add after
// this returns will guarantee that the later entry appears later in the log than any
// earlier entries. Concurrent calls to Add are supported, but the order they are queued and
// thus included in the log is non-deterministic.
//
// If the future resolves to a non-error state then it means that the entry is both
// sequenced and integrated into the log. This means that a checkpoint will be available
// that commits to this entry.
//
// It is recommended that the caller keeps the process running until all futures returned
// by this method have successfully evaluated. Terminating earlier than this will likely
// mean that some of the entries added are not committed to by a checkpoint, and thus are
// not considered to be in the log.
func (a *appender) Add(ctx context.Context, e *tessera.Entry) tessera.IndexFuture {
	return a.queue.Add(ctx, e)
}

func (l *logResourceStorage) ReadCheckpoint(_ context.Context) ([]byte, error) {
	r, err := os.ReadFile(filepath.Join(l.s.cfg.Path, layout.CheckpointPath))
	if errors.Is(err, fs.ErrNotExist) {
		return r, os.ErrNotExist
	}
	return r, err
}

// ReadEntryBundle retrieves the Nth entries bundle for a log of the given size.
func (l *logResourceStorage) ReadEntryBundle(ctx context.Context, index uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		return os.ReadFile(filepath.Join(l.s.cfg.Path, l.entriesPath(index, p)))
	})
}

func (l *logResourceStorage) ReadTile(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		return os.ReadFile(filepath.Join(l.s.cfg.Path, layout.TilePath(level, index, p)))
	})
}

func (l *logResourceStorage) IntegratedSize(ctx context.Context) (uint64, error) {
	size, _, err := l.s.readTreeState(ctx)
	return size, err
}

func (l *logResourceStorage) NextIndex(ctx context.Context) (uint64, error) {
	return l.IntegratedSize(ctx)
}

// sequenceBatch writes the entries from the provided batch into the entry bundle files of the log.
//
// This func starts filling entries bundles at the next available slot in the log, ensuring that the
// sequenced entries are contiguous from the zeroth entry (i.e left-hand dense).
// We try to minimise the number of partially complete entry bundles by writing entries in chunks rather
// than one-by-one.
func (a *appender) sequenceBatch(ctx context.Context, entries []*tessera.Entry) error {
	// Double locking:
	// - The mutex `Lock()` ensures that multiple concurrent calls to this function within a task are serialised.
	// - The POSIX `lockFile()` ensures that distinct tasks are serialised.
	a.s.mu.Lock()
	unlock, err := a.s.lockFile(ctx, treeStateLock)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := unlock(); err != nil {
			panic(err)
		}
		a.s.mu.Unlock()
	}()

	size, _, err := a.s.readTreeState(ctx)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		size = 0
	}
	a.curSize = size
	klog.V(1).Infof("Sequencing from %d", a.curSize)

	if len(entries) == 0 {
		return nil
	}
	currTile := &bytes.Buffer{}
	seq := a.curSize
	bundleIndex, entriesInBundle := seq/layout.EntryBundleWidth, seq%layout.EntryBundleWidth
	if entriesInBundle > 0 {
		// If the latest bundle is partial, we need to read the data it contains in for our newer, larger, bundle.
		part, err := a.logStorage.ReadEntryBundle(ctx, bundleIndex, uint8(a.curSize%layout.EntryBundleWidth))
		if err != nil {
			return err
		}
		if _, err := currTile.Write(part); err != nil {
			return fmt.Errorf("failed to write partial bundle into buffer: %v", err)
		}
	}
	writeBundle := func(bundleIndex uint64, partialSize uint8) error {
		return a.logStorage.writeBundle(ctx, bundleIndex, partialSize, currTile.Bytes())
	}

	leafHashes := make([][]byte, 0, len(entries))
	// Add new entries to the bundle
	for i, e := range entries {
		bundleData := e.MarshalBundleData(seq + uint64(i))
		if _, err := currTile.Write(bundleData); err != nil {
			return fmt.Errorf("failed to write entry %d to currTile: %v", i, err)
		}
		leafHashes = append(leafHashes, e.LeafHash())

		entriesInBundle++
		if entriesInBundle == layout.EntryBundleWidth {
			//  This bundle is full, so we need to write it out...
			// ... and prepare the next entry bundle for any remaining entries in the batch
			if err := writeBundle(bundleIndex, 0); err != nil {
				return err
			}
			bundleIndex++
			entriesInBundle = 0
			currTile = &bytes.Buffer{}
		}
	}
	// If we have a partial bundle remaining once we've added all the entries from the batch,
	// this needs writing out too.
	if entriesInBundle > 0 {
		// This check should be redundant since this is [currently] checked above, but an overflow around the uint8 below could
		// potentially be bad news if that check was broken/defeated as we'd be writing invalid bundle data, so do a belt-and-braces
		// check and bail if need be.
		if entriesInBundle > layout.EntryBundleWidth {
			return fmt.Errorf("logic error: entriesInBundle(%d) > max bundle size %d", entriesInBundle, layout.EntryBundleWidth)
		}
		if err := writeBundle(bundleIndex, uint8(entriesInBundle)); err != nil {
			return err
		}
	}

	// For simplicity, in-line the integration of these new entries into the Merkle structure too.
	// If this is broken out into an async process, we'll need to update the implementation of NextIndex, too.
	newSize, newRoot, err := doIntegrate(ctx, seq, leafHashes, a.logStorage)
	if err != nil {
		klog.Errorf("Integrate failed: %v", err)
		return err
	}
	if err := a.s.writeTreeState(ctx, newSize, newRoot); err != nil {
		return fmt.Errorf("failed to write new tree state: %v", err)
	}
	// Notify that we know for sure there's a new checkpoint, but don't block if there's already
	// an outstanding notification in the channel.
	select {
	case a.cpUpdated <- struct{}{}:
	default:
	}
	return nil
}

// doIntegrate handles integrating new leaf hashes into the log, and returns the new state.
func doIntegrate(ctx context.Context, fromSeq uint64, leafHashes [][]byte, ls *logResourceStorage) (uint64, []byte, error) {
	getTiles := func(ctx context.Context, tileIDs []storage.TileID, treeSize uint64) ([]*api.HashTile, error) {
		n, err := ls.readTiles(ctx, tileIDs, treeSize)
		if err != nil {
			return nil, fmt.Errorf("getTiles: %w", err)
		}
		return n, nil
	}

	newSize, newRoot, tiles, err := storage.Integrate(ctx, getTiles, fromSeq, leafHashes)
	if err != nil {
		klog.Errorf("Integrate: %v", err)
		return 0, nil, fmt.Errorf("error in Integrate: %v", err)
	}
	for k, v := range tiles {
		if err := ls.storeTile(ctx, uint64(k.Level), k.Index, newSize, v); err != nil {
			return 0, nil, fmt.Errorf("failed to set tile(%v): %v", k, err)
		}
	}

	klog.V(1).Infof("New tree: %d, %x", newSize, newRoot)

	return newSize, newRoot, nil
}

func (lrs *logResourceStorage) readTiles(ctx context.Context, tileIDs []storage.TileID, treeSize uint64) ([]*api.HashTile, error) {
	r := make([]*api.HashTile, 0, len(tileIDs))
	for _, id := range tileIDs {
		t, err := lrs.readTile(ctx, id.Level, id.Index, layout.PartialTileSize(id.Level, id.Index, treeSize))
		if err != nil {
			return nil, err
		}
		r = append(r, t)
	}
	return r, nil
}

// readTile returns the parsed tile at the given tile-level and tile-index.
// If no complete tile exists at that location, it will attempt to find a
// partial tile for the given tree size at that location.
func (lrs *logResourceStorage) readTile(ctx context.Context, level, index uint64, p uint8) (*api.HashTile, error) {
	now := time.Now()

	t, err := lrs.ReadTile(ctx, level, index, p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// We'll signal to higher levels that it wasn't found by retuning a nil for this tile.
			return nil, nil
		}
		return nil, err
	}

	var tile api.HashTile
	if err := tile.UnmarshalText(t); err != nil {
		return nil, fmt.Errorf("failed to parse tile: %w", err)
	}

	posixOpsHistogram.Record(ctx, time.Since(now).Milliseconds(), metric.WithAttributes(opNameKey.String("readTile")))
	return &tile, nil
}

// storeTile writes a tile out to disk.
// Fully populated tiles are stored at the path corresponding to the level &
// index parameters, partially populated (i.e. right-hand edge) tiles are
// stored with a .xx suffix where xx is the number of "tile leaves" in hex.
func (lrs *logResourceStorage) storeTile(ctx context.Context, level, index, logSize uint64, tile *api.HashTile) error {
	tileSize := uint64(len(tile.Nodes))
	klog.V(2).Infof("StoreTile: level %d index %x ts: %x", level, index, tileSize)
	if tileSize == 0 || tileSize > layout.TileWidth {
		return fmt.Errorf("tileSize %d must be > 0 and <= %d", tileSize, layout.TileWidth)
	}
	t, err := tile.MarshalText()
	if err != nil {
		return fmt.Errorf("failed to marshal tile: %w", err)
	}

	return lrs.writeTile(ctx, level, index, layout.PartialTileSize(level, index, logSize), t)
}

func (lrs *logResourceStorage) writeTile(ctx context.Context, level, index uint64, partial uint8, t []byte) error {
	now := time.Now()

	tPath := layout.TilePath(level, index, partial)

	if err := lrs.s.createOverwrite(tPath, t); err != nil {
		return err
	}

	if partial == 0 {
		partials, err := filepath.Glob(fmt.Sprintf("%s.p/*", tPath))
		if err != nil {
			return fmt.Errorf("failed to list partial tiles for clean up; %w", err)
		}
		// Clean up old partial tiles by symlinking them to the new full tile.
		for _, p := range partials {
			klog.V(2).Infof("relink partial %s to %s", p, tPath)
			// We have to do a little dance here to get POSIX atomicity:
			// 1. Create a new temporary symlink to the full tile
			// 2. Rename the temporary symlink over the top of the old partial tile
			tmp := fmt.Sprintf("%s.link", tPath)
			_ = os.Remove(tmp)
			if err := os.Symlink(tPath, tmp); err != nil {
				return fmt.Errorf("failed to create temp link to full tile: %w", err)
			}
			if err := os.Rename(tmp, p); err != nil {
				return fmt.Errorf("failed to rename temp link over partial tile: %w", err)
			}
		}
	}

	posixOpsHistogram.Record(ctx, time.Since(now).Milliseconds(), metric.WithAttributes(opNameKey.String("writeTile")))
	return nil
}

// writeBundle takes care of writing out the serialised entry bundle file.
func (lrs *logResourceStorage) writeBundle(_ context.Context, index uint64, partial uint8, bundle []byte) error {
	bf := lrs.entriesPath(index, partial)
	if err := lrs.s.createOverwrite(bf, bundle); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	return nil
}

// initialise ensures that the storage location is valid by loading the checkpoint from this location, or
// creating a zero-sized one if it doesn't already exist.
func (a *appender) initialise(ctx context.Context) error {
	// Idempotent: If folder exists, nothing happens.
	if err := mkdirAll(filepath.Join(a.s.cfg.Path, stateDir), dirPerm); err != nil {
		return fmt.Errorf("failed to create log directory: %q", err)
	}
	// Double locking:
	// - The mutex `Lock()` ensures that multiple concurrent calls to this function within a task are serialised.
	// - The POSIX `lockFile()` ensures that distinct tasks are serialised.
	a.s.mu.Lock()
	unlock, err := a.s.lockFile(ctx, treeStateLock)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := unlock(); err != nil {
			panic(err)
		}
		a.s.mu.Unlock()
	}()

	if err := a.s.ensureVersion(compatibilityVersion); err != nil {
		return err
	}
	curSize, _, err := a.s.readTreeState(ctx)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to load checkpoint for log: %v", err)
		}
		// Create the directory structure and write out an empty checkpoint
		klog.Infof("Initializing directory for POSIX log at %q (this should only happen ONCE per log!)", a.s.cfg.Path)
		if err := a.s.writeTreeState(ctx, 0, rfc6962.DefaultHasher.EmptyRoot()); err != nil {
			return fmt.Errorf("failed to write tree-state checkpoint: %v", err)
		}
		if a.newCP != nil {
			if err := a.publishCheckpoint(ctx, 0, 0); err != nil {
				return fmt.Errorf("failed to publish checkpoint: %v", err)
			}
		}
		return nil
	}
	a.curSize = curSize

	return nil
}

type treeState struct {
	Size uint64 `json:"size"`
	Root []byte `json:"root"`
}

// ensureVersion will fail if the compatibility version stored in the state directory
// is not the expected version. If no file exists, then it is created with the expected version.
func (s *Storage) ensureVersion(version uint16) error {
	versionFile := filepath.Join(stateDir, "version")

	if _, err := s.stat(versionFile); errors.Is(err, os.ErrNotExist) {
		klog.V(1).Infof("No version file exists, creating")
		data := fmt.Appendf(nil, "%d", version)
		if err := s.createExclusive(versionFile, data); err != nil {
			return fmt.Errorf("failed to create version file: %v", err)
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("stat(%s): %v", versionFile, err)
	}

	data, err := s.readAll(versionFile)
	if err != nil {
		return fmt.Errorf("failed to read version file: %v", err)
	}
	parsed, err := strconv.ParseUint(string(data), 10, 16)
	if err != nil {
		return fmt.Errorf("failed to parse version: %v", err)
	}
	if got, want := uint16(parsed), version; got != want {
		return fmt.Errorf("wanted version %d but found %d", want, got)
	}
	return nil
}

// writeTreeState stores the current tree size and root hash on disk.
func (s *Storage) writeTreeState(ctx context.Context, size uint64, root []byte) error {
	now := time.Now()

	raw, err := json.Marshal(treeState{Size: size, Root: root})
	if err != nil {
		return fmt.Errorf("error in Marshal: %v", err)
	}

	if err := s.createOverwrite(filepath.Join(stateDir, treeStateFile), raw); err != nil {
		return fmt.Errorf("failed to create/overwrite private tree state file: %w", err)
	}

	posixOpsHistogram.Record(ctx, time.Since(now).Milliseconds(), metric.WithAttributes(opNameKey.String("writeTreeState")))
	return nil
}

// readTreeState reads and returns the currently stored tree state.
func (s *Storage) readTreeState(ctx context.Context) (uint64, []byte, error) {
	now := time.Now()

	p := filepath.Join(s.cfg.Path, stateDir, treeStateFile)
	raw, err := os.ReadFile(p)
	if err != nil {
		return 0, nil, fmt.Errorf("error in ReadFile(%q): %w", p, err)
	}
	ts := &treeState{}
	if err := json.Unmarshal(raw, ts); err != nil {
		return 0, nil, fmt.Errorf("error in Unmarshal: %v", err)
	}

	posixOpsHistogram.Record(ctx, time.Since(now).Milliseconds(), metric.WithAttributes(opNameKey.String("readTreeState")))
	return ts.Size, ts.Root, nil
}

// publishCheckpoint checks whether the currently published checkpoint (if any) is more than
// minStaleness old, and, if so, creates and published a fresh checkpoint from the current
// stored tree state.
func (a *appender) publishCheckpoint(ctx context.Context, minStalenessActive, minStalenessRepub time.Duration) error {
	now := time.Now()

	// Lock the destination "published" checkpoint location:
	unlock, err := a.s.lockFile(ctx, publishLock)
	if err != nil {
		return fmt.Errorf("lockFile(%s): %v", publishLock, err)
	}
	defer func() {
		if err := unlock(); err != nil {
			klog.Warningf("unlock(%s): %v", publishLock, err)
		}
	}()

	var publishedAge time.Duration
	var publishedSize uint64
	cpExists := true
	info, err := a.s.stat(layout.CheckpointPath)
	if errors.Is(err, os.ErrNotExist) {
		klog.V(1).Infof("No checkpoint exists, publishing")
		cpExists = false
	} else if err != nil {
		return fmt.Errorf("stat(%s): %v", layout.CheckpointPath, err)
	} else {
		publishedAge = time.Since(info.ModTime())
		if publishedAge < minStalenessActive {
			klog.V(1).Infof("publishCheckpoint: skipping publish because previous checkpoint published %v ago, less than %v", publishedAge, minStalenessActive)
			return nil
		}
		publishedSize, err = a.publishedSize(ctx)
		if err != nil {
			klog.V(1).Infof("publishCheckpoint: skipping publish because unable to determine previously published size: %v", err)
			return err
		}
	}

	size, root, err := a.s.readTreeState(ctx)
	if err != nil {
		return fmt.Errorf("readTreeState: %v", err)
	}
	if cpExists && size == publishedSize {
		if minStalenessRepub == 0 || publishedAge < minStalenessRepub {
			klog.V(1).Infof("publishCheckpoint: skipping publish because tree hasn't grown and previous checkpoint is too recent")
			return nil
		}
	}

	cpRaw, err := a.newCP(ctx, size, root)
	if err != nil {
		return fmt.Errorf("newCP: %v", err)
	}

	if err := a.s.createOverwrite(layout.CheckpointPath, cpRaw); err != nil {
		return fmt.Errorf("createOverwrite(%s): %v", layout.CheckpointPath, err)
	}

	klog.V(2).Infof("Published latest checkpoint: %d, %x", size, root)

	posixOpsHistogram.Record(ctx, time.Since(now).Milliseconds(), metric.WithAttributes(opNameKey.String("publishCheckpoint")))

	return nil
}

// publishedSize returns the size of tree that the currently published checkpoint, if any, commits to.
//
// If there is no currently published checkpoint zero will be returned without error.
func (a *appender) publishedSize(ctx context.Context) (uint64, error) {
	cp, err := a.logStorage.ReadCheckpoint(ctx)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read published checkpoint: %v", err)
	}
	_, pubSize, _, err := parse.CheckpointUnsafe(cp)
	if err != nil {
		return 0, fmt.Errorf("failed to parse published checkpoint: %v", err)
	}
	return pubSize, nil

}

// garbageCollectorJob is a long-running function which handles the removal of obsolete partial tiles
// and entry bundles.
// Blocks until ctx is done.
func (a *appender) garbageCollectorJob(ctx context.Context, i time.Duration) {
	t := time.NewTicker(i)
	defer t.Stop()

	// Entirely arbitrary number.
	maxBundlesPerRun := uint(100)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		// Figure out the size of the latest published checkpoint - we can't be removing partial tiles implied by
		// that checkpoint just because we've done an integration and know about a larger (but as yet unpublished)
		// checkpoint!
		pubSize, err := a.publishedSize(ctx)
		if err != nil {
			klog.Warningf("GarbageCollect: %v", err)
			continue
		}

		if err := a.s.garbageCollect(ctx, pubSize, maxBundlesPerRun); err != nil {
			klog.Warningf("GarbageCollect failed: %v", err)
			continue
		}
	}
}

// gcState represents a snapshot of how much of the log tree has been garbage collected.
// This state structure is serialized into a private (but not sensitive) file in the log's .state directory.
type gcState struct {
	FromSize uint64 `json:"fromSize"`
}

// writeGCState stores the high water mark below which garbage collection has successfully completed.
func (s *Storage) writeGCState(size uint64) error {
	raw, err := json.Marshal(gcState{FromSize: size})
	if err != nil {
		return fmt.Errorf("error in Marshal: %v", err)
	}

	if err := s.createOverwrite(filepath.Join(stateDir, gcStateFile), raw); err != nil {
		return fmt.Errorf("failed to create/overwrite private GC state file: %w", err)
	}
	return nil
}

// readGCState reads and returns the currently stored GC state, if any.
//
// If no GC state is stored, no GC run has completed successfully, so zero is returned to indicate
// that GC should start from the beginning of the log.
func (s *Storage) readGCState() (uint64, error) {
	p := filepath.Join(s.cfg.Path, stateDir, gcStateFile)
	raw, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// gcState file doesn't exist yet - we've probably just not completed a GC run before so start from index 0.
			return 0, nil
		}
		return 0, fmt.Errorf("error in ReadFile(%q): %w", p, err)
	}
	gs := &gcState{}
	if err := json.Unmarshal(raw, gs); err != nil {
		return 0, fmt.Errorf("error in Unmarshal: %v", err)
	}
	return gs.FromSize, nil
}

func (s *Storage) garbageCollect(ctx context.Context, treeSize uint64, maxBundles uint) error {
	// Lock the gc location:
	unlock, err := s.lockFile(ctx, gcStateLock)
	if err != nil {
		return fmt.Errorf("lockFile(%s): %v", gcStateLock, err)
	}
	defer func() {
		if err := unlock(); err != nil {
			klog.Warningf("unlock(%s): %v", gcStateLock, err)
		}
	}()

	fromSize, err := s.readGCState()
	if err != nil {
		return fmt.Errorf("readGCState: %v", err)
	}

	if fromSize == treeSize {
		// Nothing to do, nothing done.
		return nil
	}

	d := uint(0)
	// GC the tree in "vertical" chunks defined by entry bundles.
	for ri := range layout.Range(fromSize, treeSize-fromSize, treeSize) {
		// Only known-full bundles are in-scope for for GC, so exit if the current bundle is partial or
		// we've reached our limit of chunks.
		if ri.Partial > 0 || d > maxBundles {
			break
		}

		// GC any partial versions of the entry bundle itself and the tile which sits immediately above it.
		if err := s.removeDirAll(layout.EntriesPath(ri.Index, 0) + ".p/"); err != nil {
			return err
		}
		if err := s.removeDirAll(layout.TilePath(0, ri.Index, 0) + ".p/"); err != nil {
			return err
		}
		fromSize += uint64(ri.N)
		d++

		// Now consider (only) the part of the tree which sits above the bundle.
		// We'll walk up the parent tiles for as a long as we're tracing the right-hand
		// edge of a perfect subtree.
		// This gives the property we'll only visit each parent tile once, rather than up to 256 times.
		pL, pIdx := uint64(0), ri.Index
		for isLastLeafInParent(pIdx) {
			// Move our coordinates up to the parent
			pL, pIdx = pL+1, pIdx>>layout.TileHeight
			// GC any partial versions of the parent tile.
			if err := s.removeDirAll(layout.TilePath(pL, pIdx, 0) + ".p/"); err != nil {
				return err
			}

		}
	}
	if err := s.writeGCState(fromSize); err != nil {
		return fmt.Errorf("writeGCState: %v", err)
	}
	return nil
}

// isLastLeafInParent returns true if a tile with the provided index is the final child node of a
// (hypothetical) full parent tile.
func isLastLeafInParent(i uint64) bool {
	return i%layout.TileWidth == layout.TileWidth-1
}

// createExclusive atomically creates a file at the given path, relative to the root of the log, containing the provided data.
//
// It will error if a file already exists at the specified location, or it's unable to fully write the
// data & close the file.
func (s *Storage) createExclusive(p string, d []byte) error {
	return createEx(filepath.Join(s.cfg.Path, p), d)
}

// createOverwrite atomically creates or overwrites a file at the given path with the provided data.
func (s *Storage) createOverwrite(p string, d []byte) error {
	return overwrite(filepath.Join(s.cfg.Path, p), d)
}

func (s *Storage) readAll(p string) ([]byte, error) {
	p = filepath.Join(s.cfg.Path, p)
	return os.ReadFile(p)

}

// stat returns os.Stat info for the speficied file relative to the log root.
func (s *Storage) stat(p string) (os.FileInfo, error) {
	p = filepath.Join(s.cfg.Path, p)
	return os.Stat(p)
}

// removeDirAll removes the named directory and anything it contains.
// The provided path is interpreted relative to the log root.
func (s *Storage) removeDirAll(p string) error {
	p = filepath.Join(s.cfg.Path, p)
	klog.V(3).Infof("rm %s", p)
	if err := os.RemoveAll(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// MigrationWriter creates a new POSIX storage for the MigrationTarget lifecycle mode.
func (s *Storage) MigrationWriter(ctx context.Context, opts *tessera.MigrationOptions) (migrate.MigrationWriter, tessera.LogReader, error) {
	r := &MigrationStorage{
		s: s,
		logStorage: &logResourceStorage{
			entriesPath: opts.EntriesPath(),
			s:           s,
		},
		bundleHasher: opts.LeafHasher(),
	}
	if err := r.initialise(ctx); err != nil {
		return nil, nil, err
	}
	return r, r.logStorage, nil
}

// MigrationStorgage implements the tessera.MigrationTarget lifecycle contract.
type MigrationStorage struct {
	s            *Storage
	logStorage   *logResourceStorage
	bundleHasher func(entryBundle []byte) ([][]byte, error)
	curSize      uint64
}

var _ migrate.MigrationWriter = &MigrationStorage{}

func (m *MigrationStorage) AwaitIntegration(ctx context.Context, sourceSize uint64) ([]byte, error) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
		}
		if err := m.buildTree(ctx, sourceSize); err != nil {
			klog.Warningf("buildTree: %v", err)
		}
		s, r, err := m.s.readTreeState(ctx)
		if err != nil {
			klog.Warningf("readTreeState: %v", err)
		}
		if s == sourceSize {
			return r, nil
		}
	}
}

func (m *MigrationStorage) initialise(ctx context.Context) error {
	// Idempotent: If folder exists, nothing happens.
	if err := mkdirAll(filepath.Join(m.s.cfg.Path, stateDir), dirPerm); err != nil {
		return fmt.Errorf("failed to create log directory: %q", err)
	}
	// Double locking:
	// - The mutex `Lock()` ensures that multiple concurrent calls to this function within a task are serialised.
	// - The POSIX `lockFile()` ensures that distinct tasks are serialised.
	m.s.mu.Lock()
	unlock, err := m.s.lockFile(ctx, treeStateLock)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := unlock(); err != nil {
			panic(err)
		}
		m.s.mu.Unlock()
	}()

	if err := m.s.ensureVersion(compatibilityVersion); err != nil {
		return err
	}
	curSize, _, err := m.s.readTreeState(ctx)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to load checkpoint for log: %v", err)
		}
		// Create the directory structure and write out an empty checkpoint
		klog.Infof("Initializing directory for POSIX log at %q (this should only happen ONCE per log!)", m.s.cfg.Path)
		if err := m.s.writeTreeState(ctx, 0, rfc6962.DefaultHasher.EmptyRoot()); err != nil {
			return fmt.Errorf("failed to write tree-state checkpoint: %v", err)
		}
		return nil
	}
	m.curSize = curSize

	return nil
}

func (m *MigrationStorage) SetEntryBundle(ctx context.Context, index uint64, partial uint8, bundle []byte) error {
	return m.logStorage.writeBundle(ctx, index, partial, bundle)
}

func (m *MigrationStorage) IntegratedSize(ctx context.Context) (uint64, error) {
	sz, _, err := m.s.readTreeState(ctx)
	return sz, err
}

func (m *MigrationStorage) buildTree(ctx context.Context, targetSize uint64) error {
	// Double locking:
	// - The mutex `Lock()` ensures that multiple concurrent calls to this function within a task are serialised.
	// - The POSIX `lockFile()` ensures that distinct tasks are serialised.
	m.s.mu.Lock()
	unlock, err := m.s.lockFile(ctx, treeStateLock)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := unlock(); err != nil {
			panic(err)
		}
		m.s.mu.Unlock()
	}()

	size, _, err := m.s.readTreeState(ctx)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		size = 0
	}
	m.curSize = size
	klog.V(1).Infof("Building from %d", m.curSize)

	lh, err := m.fetchLeafHashes(ctx, size, targetSize, targetSize)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// We just don't have the bundle yet.
			// Bail quietly and the caller can retry.
			klog.V(1).Infof("fetchLeafHashes(%d, %d): %v", size, targetSize, err)
			return nil
		}
		return fmt.Errorf("fetchLeafHashes(%d, %d): %v", size, targetSize, err)
	}

	newSize, newRoot, err := doIntegrate(ctx, size, lh, m.logStorage)
	if err != nil {
		return fmt.Errorf("doIntegrate(%d, ...): %v", size, err)
	}
	if err := m.s.writeTreeState(ctx, newSize, newRoot); err != nil {
		return fmt.Errorf("failed to write new tree state: %v", err)
	}

	return nil
}

func (m *MigrationStorage) fetchLeafHashes(ctx context.Context, from, to, sourceSize uint64) ([][]byte, error) {
	const maxBundles = 300

	lh := make([][]byte, 0, maxBundles)
	n := 0
	for ri := range layout.Range(from, to, sourceSize) {
		b, err := m.logStorage.ReadEntryBundle(ctx, ri.Index, ri.Partial)
		if err != nil {
			return nil, fmt.Errorf("ReadEntryBundle(%d.%d): %w", ri.Index, ri.Partial, err)
		}

		bh, err := m.bundleHasher(b)
		if err != nil {
			return nil, fmt.Errorf("bundleHasherFunc for bundle index %d: %v", ri.Index, err)
		}
		lh = append(lh, bh[ri.First:ri.First+ri.N]...)
		n++
		if n >= maxBundles {
			break
		}
	}
	return lh, nil
}
