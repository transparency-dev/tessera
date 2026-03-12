// Copyright 2024 Google LLC. All Rights Reserved.
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
	"fmt"
	"reflect"
	"sync"
	"testing"

	"log/slog"

	"github.com/google/go-cmp/cmp"
	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
)

func TestNewRangeFetchesTiles(t *testing.T) {
	ctx := context.Background()
	m := newMemTileStore[api.HashTile]()
	tb := newTreeBuilder(m.getTiles)

	treeSize := uint64(0x102030)
	wantIDs := []TileID{
		{Level: 0, Index: 0x1020},
		{Level: 1, Index: 0x10},
		{Level: 2, Index: 0x0},
	}

	for _, id := range wantIDs {
		if err := m.setTile(ctx, id, treeSize, zeroTile(layout.TileWidth)); err != nil {
			t.Fatalf("setTile: %v", err)
		}
	}

	_, err := tb.newRange(ctx, treeSize)
	if err != nil {
		t.Fatalf("newRange(%d): %v", treeSize, err)
	}
}

func TestTileVisit(t *testing.T) {
	ctx := context.Background()
	m := newMemTileStore[populatedTile]()
	treeSize := uint64(0x102030)

	for _, test := range []struct {
		name      string
		visits    map[compact.NodeID][]byte
		wantTiles map[TileID]*api.HashTile
	}{
		{
			name: "ok - single tile",
			visits: map[compact.NodeID][]byte{
				{Level: 0, Index: 0}: {0},
				{Level: 0, Index: 1}: {1},
				{Level: 1, Index: 1}: {2},
			},
			wantTiles: map[TileID]*api.HashTile{
				{Level: 0, Index: 0}: {Nodes: [][]byte{{0}, {1}}},
			},
		},
		{
			name: "ok - multiple tiles",
			visits: map[compact.NodeID][]byte{
				{Level: 0, Index: 0}:                    {0},
				{Level: 0, Index: 1 * layout.TileWidth}: {1},
				{Level: 8, Index: 2 * layout.TileWidth}: {2},
			},
			wantTiles: map[TileID]*api.HashTile{
				{Level: 0, Index: 0}: {Nodes: [][]byte{{0}}},
				{Level: 0, Index: 1}: {Nodes: [][]byte{{1}}},
				{Level: 1, Index: 2}: {Nodes: [][]byte{{2}}},
			},
		},
	} {
		twc := newTileWriteCache(treeSize, m.getTile)
		v := twc.Visitor(ctx)
		for id, k := range test.visits {
			v(id, k)
		}
		if err := twc.Err(); err != nil {
			t.Fatalf("Got err: %v", err)
		}
		gotTiles := twc.Tiles()
		for id, wantTile := range test.wantTiles {
			gotTile, ok := gotTiles[id]
			if !ok {
				t.Errorf("Missing tile %v", id)
				continue
			}
			if !reflect.DeepEqual(gotTile, wantTile) {
				t.Errorf("Got tile with unexpected data at %v:\ngot:\n%x\nwant:\n%x", id, gotTile, wantTile)
			}
			delete(gotTiles, id)
			delete(test.wantTiles, id)
		}
		if l := len(gotTiles); l > 0 {
			t.Errorf("got unexpected tiles: %v", gotTiles)
		}
		if l := len(test.wantTiles); l > 0 {
			t.Errorf("did not get expected tiles: %v", test.wantTiles)
		}
	}
}

func TestIntegrate(t *testing.T) {
	ctx := context.Background()
	m := newMemTileStore[api.HashTile]()

	cr := (&compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}).NewEmptyRange(0)

	chunkSize := 200
	numChunks := 1000
	seq := uint64(0)
	for chunk := range numChunks {
		oldSeq := seq
		c := make([][]byte, chunkSize)
		for i := range c {
			leaf := []byte{byte(seq)}
			entry := tessera.NewEntry(leaf)
			c[i] = entry.LeafHash()
			if err := cr.Append(rfc6962.DefaultHasher.HashLeaf(leaf), nil); err != nil {
				t.Fatalf("compact Append: %v", err)
			}
			seq++
		}
		wantRoot, err := cr.GetRootHash(nil)
		if err != nil {
			t.Fatalf("[%d] compactRange: %v", chunk, err)
		}
		gotSize, gotRoot, gotTiles, err := Integrate(ctx, m.getTiles, oldSeq, c)
		if err != nil {
			t.Fatalf("[%d] Integrate: %v", chunk, err)
		}
		if wantSize := seq; gotSize != wantSize {
			t.Errorf("[%d] Got size %d, want %d", chunk, gotSize, wantSize)
		}
		if !cmp.Equal(gotRoot, wantRoot) {
			t.Errorf("[%d] Got root %x, want %x", chunk, gotRoot, wantRoot)
		}
		for k, tile := range gotTiles {
			if err := m.setTile(ctx, k, seq, tile); err != nil {
				t.Fatalf("setTile: %v", err)
			}
		}
	}
}

func BenchmarkIntegrate(b *testing.B) {
	ctx := context.Background()
	m := newMemTileStore[api.HashTile]()

	chunkSize := 200
	seq := uint64(0)
	for chunk := 0; b.Loop(); chunk++ {
		oldSeq := seq
		c := make([][]byte, chunkSize)
		for i := range c {
			leaf := []byte{byte(seq)}
			entry := tessera.NewEntry(leaf)
			c[i] = entry.LeafHash()
			seq++
		}
		_, _, gotTiles, err := Integrate(ctx, m.getTiles, oldSeq, c)
		if err != nil {
			b.Fatalf("[%d] Integrate: %v", chunk, err)
		}
		for k, tile := range gotTiles {
			if err := m.setTile(ctx, k, seq, tile); err != nil {
				b.Fatalf("setTile: %v", err)
			}
		}
	}
}

// zerotile creates a new api.HashTile of the provided size, whose leaves are all a single zero byte.
func zeroTile(size uint64) *api.HashTile {
	r := &api.HashTile{
		Nodes: make([][]byte, size),
	}
	for i := range r.Nodes {
		r.Nodes[i] = []byte{0}
	}
	return r
}

type memTileStore[T any] struct {
	sync.RWMutex
	mem map[string]*T
}

func newMemTileStore[T any]() *memTileStore[T] {
	return &memTileStore[T]{
		mem: make(map[string]*T),
	}
}

func (m *memTileStore[T]) getTile(_ context.Context, id TileID, treeSize uint64) (*T, error) {
	m.RLock()
	defer m.RUnlock()

	k := layout.TilePath(id.Level, id.Index, layout.PartialTileSize(id.Level, id.Index, treeSize))
	d := m.mem[k]
	return d, nil
}

func (m *memTileStore[T]) getTiles(_ context.Context, ids []TileID, treeSize uint64) ([]*T, error) {
	m.RLock()
	defer m.RUnlock()

	r := make([]*T, len(ids))
	for i, id := range ids {
		k := layout.TilePath(id.Level, id.Index, layout.PartialTileSize(id.Level, id.Index, treeSize))
		slog.Debug("mem.getTile", slog.String("k", k), slog.Uint64("treesize", treeSize))
		d, ok := m.mem[k]
		if !ok {
			continue
		}
		r[i] = d
	}
	return r, nil
}

func (m *memTileStore[T]) setTile(_ context.Context, id TileID, treeSize uint64, t *T) error {
	m.Lock()
	defer m.Unlock()

	k := layout.TilePath(id.Level, id.Index, layout.PartialTileSize(id.Level, id.Index, treeSize))
	slog.Debug("mem.setTile", slog.String("k", k), slog.Uint64("treesize", treeSize))
	_, ok := m.mem[k]
	if ok {
		return fmt.Errorf("%q is already present", k)
	}
	d := *t
	m.mem[k] = &d
	return nil
}
