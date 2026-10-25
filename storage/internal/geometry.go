// Copyright 2026 The Tessera authors. All Rights Reserved.
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

package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/fetcher"
)

// PartialResourceIO provides EnsureGeometry with raw access to a log's tiles
// and entry bundles.
type PartialResourceIO struct {
	// TileExists reports whether exactly the requested tile is present. It must
	// not fall back from a partial to the full tile: if it did, a missing partial
	// tile would never be created whenever the full tile exists.
	TileExists func(ctx context.Context, level, index uint64, p uint8) (bool, error)
	// ReadTile returns the requested tile, or an error wrapping os.ErrNotExist if
	// it's absent. Unlike TileExists, it may fall back from a partial to the full
	// tile, so tessera.LogReader.ReadTile is suitable.
	ReadTile func(ctx context.Context, level, index uint64, p uint8) ([]byte, error)
	// WriteTile stores a tile. It must succeed if identical data already exists.
	WriteTile func(ctx context.Context, level, index uint64, p uint8, data []byte) error
	// EntryBundleExists is the entry bundle equivalent of TileExists.
	EntryBundleExists func(ctx context.Context, index uint64, p uint8) (bool, error)
	// ReadEntryBundle is the entry bundle equivalent of ReadTile.
	ReadEntryBundle func(ctx context.Context, index uint64, p uint8) ([]byte, error)
	// WriteEntryBundle is the entry bundle equivalent of WriteTile.
	WriteEntryBundle func(ctx context.Context, index uint64, p uint8, data []byte) error
}

// EnsureGeometry ensures that the partial tiles and entry bundle implied by
// cpSize exist, deriving any missing ones from the resources implied by
// treeSize. It is idempotent, so it is safe to call from retried transactions.
//
// Storage implementations should call this before publishing a checkpoint of
// size cpSize: if their integrated tree, of size treeSize, has grown beyond
// cpSize, then the partial resources implied by cpSize may never have been
// written.
func EnsureGeometry(ctx context.Context, rw PartialResourceIO, cpSize, treeSize uint64) error {
	// If cpSize is zero then no tree exists.
	// If cpSize == treeSize the resources are guaranteed present by integration.
	// In both cases there's nothing to do.
	if cpSize == 0 || cpSize == treeSize {
		return nil
	}
	if cpSize > treeSize {
		return fmt.Errorf("new size %d is greater than integrated size %d", cpSize, treeSize)
	}
	ml := maxLevel(cpSize)
	idx := (cpSize - 1) >> layout.TileHeight
	for l := uint64(0); l <= uint64(ml); l, idx = l+1, idx>>layout.TileHeight {
		treeP := layout.PartialTileSize(l, idx, treeSize)
		cpP := layout.PartialTileSize(l, idx, cpSize)

		if cpP == treeP {
			// Nothing to be done at this level.
			continue
		}

		if l == 0 {
			if err := ensurePartialBundle(ctx, rw, idx, cpP, treeP); err != nil {
				return err
			}
		}
		if err := ensurePartialTile(ctx, rw, l, idx, cpP, treeP); err != nil {
			return err
		}
	}
	return nil
}

// maxLevel returns the maximum tile level for a tree of the given size.
func maxLevel(sz uint64) int {
	if sz == 0 {
		return 0
	}
	return (bits.Len64(sz) - 1) / layout.TileHeight
}

// ensurePartialBundle ensures that the partial entry bundle implied by the new size is
// present in the stored resources.
//
// If the implied partial entry bundle is not already present, this function will attempt to create
// it from the entry bundle implied by treeSize.
func ensurePartialBundle(ctx context.Context, rw PartialResourceIO, idx uint64, cpP, treeP uint8) error {
	if cpP == 0 {
		// layout.PartialTileSize returns 0 for both full and empty resources. We're only
		// called when cpP != treeP and treeSize > cpSize, so a zero here means that the
		// resource is empty at the new size: there's nothing to create.
		return nil
	}

	// Check if the entry bundle already exists.
	exists, err := rw.EntryBundleExists(ctx, idx, cpP)
	if err != nil {
		return fmt.Errorf("failed to check for entry bundle @%d.%d: %w", idx, cpP, err)
	}
	if exists {
		// Already exists, we're done!
		return nil
	}

	// Read bundle implied by the tree size, falling back to the full bundle in case the
	// tree has since grown and the partial bundle has been garbage collected.
	d, err := fetcher.PartialOrFullResource(ctx, treeP, func(ctx context.Context, p uint8) ([]byte, error) {
		return rw.ReadEntryBundle(ctx, idx, p)
	})
	if err != nil {
		return fmt.Errorf("failed to read entry bundle @%d.%d: %w", idx, treeP, err)
	}
	eb := &api.EntryBundle{}
	if err := eb.UnmarshalText(d); err != nil {
		return fmt.Errorf("failed to unmarshal entry bundle @%d.%d: %w", idx, treeP, err)
	}
	if n := len(eb.Entries); n < int(cpP) {
		return fmt.Errorf("entry bundle @%d.%d has %d entries, want at least %d", idx, treeP, n, cpP)
	}

	eb.Entries = eb.Entries[:cpP]
	d, err = marshalTlogEntryBundle(eb)
	if err != nil {
		return fmt.Errorf("failed to marshal entry bundle @%d.%d: %w", idx, cpP, err)
	}
	if err := rw.WriteEntryBundle(ctx, idx, cpP, d); err != nil {
		return fmt.Errorf("failed to write entry bundle @%d.%d: %w", idx, cpP, err)
	}
	return nil
}

// ensurePartialTile ensures that the partial tile implied by the new size is
// present in the stored resources.
//
// If the implied partial tile is not already present, this function will attempt to create
// it from the tile implied by treeSize.
func ensurePartialTile(ctx context.Context, rw PartialResourceIO, l uint64, idx uint64, cpP, treeP uint8) error {
	if cpP == 0 {
		// The tile is empty at the new size, see ensurePartialBundle.
		return nil
	}

	// Check if the tile already exists.
	exists, err := rw.TileExists(ctx, l, idx, cpP)
	if err != nil {
		return fmt.Errorf("failed to check for tile @%d/%d.%d: %w", l, idx, cpP, err)
	}
	if exists {
		// Already exists, we're done!
		return nil
	}

	// Read tile implied by the tree size, falling back to the full tile in case the
	// tree has since grown and the partial tile has been garbage collected.
	d, err := fetcher.PartialOrFullResource(ctx, treeP, func(ctx context.Context, p uint8) ([]byte, error) {
		return rw.ReadTile(ctx, l, idx, p)
	})
	if err != nil {
		return fmt.Errorf("failed to read tile @%d/%d.%d: %w", l, idx, treeP, err)
	}
	t := &api.HashTile{}
	if err := t.UnmarshalText(d); err != nil {
		return fmt.Errorf("failed to unmarshal tile @%d/%d.%d: %w", l, idx, treeP, err)
	}
	if n := len(t.Nodes); n < int(cpP) {
		return fmt.Errorf("tile @%d/%d.%d has %d nodes, want at least %d", l, idx, treeP, n, cpP)
	}

	t.Nodes = t.Nodes[:cpP]
	d, err = t.MarshalText()
	if err != nil {
		return fmt.Errorf("failed to marshal tile @%d/%d.%d: %w", l, idx, cpP, err)
	}
	if err := rw.WriteTile(ctx, l, idx, cpP, d); err != nil {
		return fmt.Errorf("failed to write tile @%d/%d.%d: %w", l, idx, cpP, err)
	}
	return nil
}

// marshalTlogEntryBundle returns a tlog-tiles compatible serialization of the provided entry bundle.
func marshalTlogEntryBundle(b *api.EntryBundle) ([]byte, error) {
	total := 0
	for i, e := range b.Entries {
		l := len(e)
		if l >= 1<<16 {
			return nil, fmt.Errorf("entry #%d has length %d >= 1<<16", i, l)
		}
		total += 2 + l
	}
	data := make([]byte, 0, total)
	for _, e := range b.Entries {
		data = binary.BigEndian.AppendUint16(data, uint16(len(e)))
		data = append(data, e...)
	}
	return data, nil
}
