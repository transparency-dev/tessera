// Copyright 2025 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fsck

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sync/atomic"

	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

// Fetcher describes a struct which knows how to retrieve tlog-tiles artifacts from a log.
type Fetcher interface {
	ReadCheckpoint(ctx context.Context) ([]byte, error)
	ReadTile(ctx context.Context, l, i uint64, p uint8) ([]byte, error)
	ReadEntryBundle(ctx context.Context, i uint64, p uint8) ([]byte, error)
}

// Check performs an integrity check against a log via the provided fetcher, using the provided
// bundleHasher to parse and convert entries from the log's entry bundles into leaf hashes.
//
// The leaf hashes are used to:
// 1. re-construct the root hash of the log, and compare it against the value in the log's checkpoint
// 2. re-construct the internal tiles of the log, and compare them against the log's tile resources.
//
// The checking will use the provided N parameter to control the number of concurrent workers undertaking
// this process.
func Check(ctx context.Context, origin string, verifier note.Verifier, f Fetcher, N uint, bundleHasher func([]byte) ([][]byte, error)) error {
	cp, cpRaw, _, err := client.FetchCheckpoint(ctx, f.ReadCheckpoint, verifier, origin)
	if err != nil {
		klog.Exitf("Failed to fetch and verify checkpoint: %v", err)
	}
	klog.Infof("Fsck: checking log of size %d", cp.Size)

	klog.V(1).Infof("Fsck: checkpoint:\n%s", cpRaw)

	fTree := fsckTree{
		fetcher:           f,
		bundleHasher:      bundleHasher,
		tree:              (&compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}).NewEmptyRange(0),
		sourceSize:        cp.Size,
		pendingTiles:      make(map[compact.NodeID]*api.HashTile),
		expectedResources: make(chan resource, N),
	}

	// Set up a stream of entry bundles from the log to be checked.

	eg := errgroup.Group{}

	// Kick off resource comparing workers
	for range N {
		eg.Go(fTree.resourceCheckWorker(ctx))
	}

	getSize := func(_ context.Context) (uint64, error) { return cp.Size, nil }
	// Consume the stream of bundles to re-derive the other log resources.
	// TODO(al): consider chunking the log and doing each in parallel.
	for b, err := range client.EntryBundles(ctx, N, getSize, f.ReadEntryBundle, 0, cp.Size) {
		if err != nil {
			return fmt.Errorf("error while streaming bundles: %v", err)
		}
		if err := fTree.AppendBundle(b.RangeInfo, b.Data); err != nil {
			return fmt.Errorf("failure calling AppendBundle(%v): %v", b.RangeInfo, err)
		}
		if fTree.tree.End() >= cp.Size {
			break
		}
	}

	// Ensure we see process any partial tiles too.
	fTree.flushPartialTiles()
	// Signal that there will be no more resource checking jobs coming so workers can exit when the channel is drained.
	close(fTree.expectedResources)

	// Wait for all the work to be done.
	if err := eg.Wait(); err != nil {
		klog.Exitf("Failed: %v", err)
	}

	// Finally, check that the claimed root hash matches what we calculated.
	gotRoot, err := fTree.GetRootHash()
	switch {
	case err != nil:
		klog.Exitf("Failed to calculate root: %v", err)
	case !bytes.Equal(gotRoot, cp.Hash):
		klog.Exitf("Calculated root %x, but checkpoint claims %x", gotRoot, cp.Hash)
	default:
		klog.Infof("Successfully fsck'd log with size %d and root %s (%x)", cp.Size, base64.StdEncoding.EncodeToString(gotRoot), gotRoot)
	}

	return nil
}

// resource represents a single static tile resource on the log, and the derived content we expect it to contain.
type resource struct {
	level, index uint64
	partial      uint8
	content      []byte
}

// fsckTree represents the tree we're currently checking.
type fsckTree struct {
	// fetcher knows how to retrieve static tlog-tile resources.
	fetcher Fetcher
	// bundleHasher knows how to convert entry bundles into leaf hashes.
	bundleHasher func([]byte) ([][]byte, error)
	// tree contains the running state of the leaves we've appended so far.
	tree *compact.Range
	// sourceSize is the size of the source log we're checking.
	sourceSize uint64

	// pendingTiles holds tlog-tile structs which we are currently populating, but which we haven't yet
	// verified.
	// Entries are removed from this map once they either a) become fully populated, or b) flushPartialTiles is called.
	pendingTiles map[compact.NodeID]*api.HashTile

	// expectedResources is a channel of derived tlog resources which need to be verified against the source log's static resources.
	// Entries in this channel are consumed by the resoruceCheckWorker functions.
	expectedResources chan resource
}

// AppendBundle appends leaf hashes from the provided entry bundle.
func (f *fsckTree) AppendBundle(ri layout.RangeInfo, data []byte) error {
	if impliedSeq := ri.Index*layout.EntryBundleWidth + uint64(ri.First); impliedSeq != f.tree.End() {
		return fmt.Errorf("bundle with implied sequence number %d but expected %d", impliedSeq, f.tree.End())
	}

	hs, err := f.bundleHasher(data)
	if err != nil {
		return err
	}
	for i := ri.First; i < ri.First+ri.N; i++ {
		if err := f.tree.Append(hs[i], f.visit); err != nil {
			return err
		}
	}
	return nil
}

func (f *fsckTree) GetRootHash() ([]byte, error) {
	if f.tree.End() == 0 {
		return rfc6962.DefaultHasher.EmptyRoot(), nil
	}
	return f.tree.GetRootHash(nil)
}

// visit is used to populate the derived tiles as we consume entries from the log we're checking.
func (f *fsckTree) visit(id compact.NodeID, h []byte) {
	// We're only storing the lowest level of hash in the tiles, so early-out in other cases.
	if id.Level%layout.TileHeight != 0 {
		return
	}
	tLevel, tIdx, hIdx := id.Level/layout.TileHeight, id.Index/layout.EntryBundleWidth, id.Index%layout.EntryBundleWidth
	k := compact.NodeID{Level: tLevel, Index: tIdx}
	t, ok := f.pendingTiles[k]
	if !ok {
		t = &api.HashTile{}
		f.pendingTiles[k] = t
	}
	if hIdx != uint64(len(t.Nodes)) {
		klog.Exitf("LOGIC ERROR: got tile (l: %d, idx: %d) node index %d, for tile with %d nodes", tLevel, tIdx, hIdx, len(t.Nodes))
	}
	t.Nodes = append(t.Nodes, h)
	if len(t.Nodes) == layout.EntryBundleWidth {
		c, err := t.MarshalText()
		if err != nil {
			klog.Exitf("Failed to marshal tile: %v", err)
		}
		f.expectedResources <- resource{
			level:   uint64(tLevel),
			index:   tIdx,
			partial: uint8(len(t.Nodes)),
			content: c,
		}
		delete(f.pendingTiles, k)
	}
}

// flushPartialTiles ensures that any remaining derived tiles, which due to the size of the tree are partial, are also flushed to the
// expectedResources work queue.
func (f *fsckTree) flushPartialTiles() {
	for k, t := range f.pendingTiles {
		c, err := t.MarshalText()
		if err != nil {
			klog.Exitf("Failed to marshal tile: %v", err)
		}
		f.expectedResources <- resource{
			level:   uint64(k.Level),
			index:   k.Index,
			partial: uint8(len(t.Nodes)),
			content: c,
		}
		delete(f.pendingTiles, k)
	}
}

var resourceWorkerID atomic.Uint32

// resourceCheckWorker returns a func which will consume resource check jobs from the
// expectedResources channel.
func (f *fsckTree) resourceCheckWorker(ctx context.Context) func() error {
	id := fmt.Sprintf("rc-worker-%d", resourceWorkerID.Add(1))

	return func() error {
		for r := range f.expectedResources {
			data, err := f.fetcher.ReadTile(ctx, r.level, r.index, r.partial)
			if err != nil {
				return err
			}
			if l, e := uint(len(data)), uint(r.partial)*sha256.Size; r.partial != 0 && l > e {
				// We were likely given a full tile rather than a partial tile, so trim it to the expected size.
				data = data[:e]
			}
			p := layout.TilePath(r.level, r.index, r.partial)
			if !bytes.Equal(data, r.content) {
				return fmt.Errorf("%s: log has:\n%x\nexpected:\n%x", p, data, r.content)
			}
			klog.V(2).Infof("%s: %s ok", id, p)
		}
		return nil
	}
}
