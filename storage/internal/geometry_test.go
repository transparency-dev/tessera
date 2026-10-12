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
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
)

// testTreeSize is the size of the integrated tree used by the EnsureGeometry tests.
// It spans three tile levels, and has more than one tile at level 1.
const testTreeSize = 66000

func TestEnsureGeometry(t *testing.T) {
	type object struct {
		path string
		data []byte
	}
	tile := func(level, index uint64, p uint8) object {
		return object{path: layout.TilePath(level, index, p), data: testTile(level, index, p)}
	}
	bundle := func(index uint64, p uint8) object {
		return object{path: testBundlePath(index, p), data: testBundle(t, index, p)}
	}

	for _, test := range []struct {
		name   string
		cpSize uint64
		// wantCreated holds the objects which EnsureGeometry should create.
		wantCreated []object
	}{
		{
			name:   "size_0",
			cpSize: 0,
		}, {
			name:        "size_1",
			cpSize:      1,
			wantCreated: []object{bundle(0, 1), tile(0, 0, 1)},
		}, {
			name:        "size_129",
			cpSize:      129,
			wantCreated: []object{bundle(0, 129), tile(0, 0, 129)},
		}, {
			// The level 0 tile is full, so only the level 1 tile is partial.
			name:        "size_256",
			cpSize:      256,
			wantCreated: []object{tile(1, 0, 1)},
		}, {
			name:        "size_300",
			cpSize:      300,
			wantCreated: []object{bundle(1, 44), tile(0, 1, 44), tile(1, 0, 1)},
		}, {
			name:        "size_600",
			cpSize:      600,
			wantCreated: []object{bundle(2, 88), tile(0, 2, 88), tile(1, 0, 2)},
		}, {
			// The only partial tile (level 2, with 1 node) is the same as the tree's.
			name:   "size_65536",
			cpSize: 65536,
		}, {
			// Level 1 tile 1 is empty at this size, so it must not be created.
			name:        "size_65537",
			cpSize:      65537,
			wantCreated: []object{bundle(256, 1), tile(0, 256, 1)},
		}, {
			// Derived from the tree's partial resources, rather than full ones.
			name:        "size_65800",
			cpSize:      65800,
			wantCreated: []object{bundle(257, 8), tile(0, 257, 8)},
		}, {
			name:   "size_66000",
			cpSize: testTreeSize,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newFakeGeometryStore(t, testTreeSize)
			before := maps.Clone(s.objects)

			if err := EnsureGeometry(t.Context(), s.io(), test.cpSize, testTreeSize); err != nil {
				t.Fatalf("EnsureGeometry(%d, %d) = %v, want nil", test.cpSize, testTreeSize, err)
			}

			want := make(map[string][]byte)
			for _, o := range test.wantCreated {
				want[o.path] = o.data
			}
			got := make(map[string][]byte)
			for path, data := range s.objects {
				if _, ok := before[path]; !ok {
					got[path] = data
				}
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("EnsureGeometry(%d, %d) created unexpected objects (-want +got):\n%s", test.cpSize, testTreeSize, diff)
			}
		})
	}
}

func TestEnsureGeometryNoOp(t *testing.T) {
	for _, cpSize := range []uint64{0, testTreeSize} {
		t.Run(fmt.Sprintf("size_%d", cpSize), func(t *testing.T) {
			s := newFakeGeometryStore(t, testTreeSize)
			if err := EnsureGeometry(t.Context(), s.io(), cpSize, testTreeSize); err != nil {
				t.Fatalf("EnsureGeometry(%d, %d) = %v, want nil", cpSize, testTreeSize, err)
			}
			if s.reads != 0 || s.writes != 0 {
				t.Errorf("EnsureGeometry(%d, %d) did %d reads and %d writes, want none", cpSize, testTreeSize, s.reads, s.writes)
			}
		})
	}
}

func TestEnsureGeometryIdempotent(t *testing.T) {
	const cpSize = 300
	s := newFakeGeometryStore(t, testTreeSize)
	if err := EnsureGeometry(t.Context(), s.io(), cpSize, testTreeSize); err != nil {
		t.Fatalf("EnsureGeometry(%d, %d) = %v, want nil", cpSize, testTreeSize, err)
	}

	s.writes = 0
	if err := EnsureGeometry(t.Context(), s.io(), cpSize, testTreeSize); err != nil {
		t.Fatalf("Repeated EnsureGeometry(%d, %d) = %v, want nil", cpSize, testTreeSize, err)
	}
	if s.writes != 0 {
		t.Errorf("Repeated EnsureGeometry(%d, %d) did %d writes, want 0", cpSize, testTreeSize, s.writes)
	}
}

func TestEnsureGeometryErrors(t *testing.T) {
	errRead := errors.New("read failed")
	for _, test := range []struct {
		name   string
		cpSize uint64
		// modify, if set, alters the store before EnsureGeometry is called.
		modify func(t *testing.T, s *fakeGeometryStore)
		// wantErrIs, if set, must be in the returned error's chain.
		wantErrIs error
	}{
		{
			name:   "size_greater_than_tree",
			cpSize: testTreeSize + 1,
		}, {
			name:      "read_error",
			cpSize:    300,
			modify:    func(_ *testing.T, s *fakeGeometryStore) { s.readErr = errRead },
			wantErrIs: errRead,
		}, {
			name:   "missing_source_bundle",
			cpSize: 300,
			modify: func(_ *testing.T, s *fakeGeometryStore) {
				delete(s.objects, testBundlePath(1, 0))
			},
			wantErrIs: os.ErrNotExist,
		}, {
			name:   "missing_source_tile",
			cpSize: 300,
			modify: func(_ *testing.T, s *fakeGeometryStore) {
				delete(s.objects, layout.TilePath(0, 1, 0))
			},
			wantErrIs: os.ErrNotExist,
		}, {
			name:   "short_source_bundle",
			cpSize: 300,
			modify: func(t *testing.T, s *fakeGeometryStore) {
				s.objects[testBundlePath(1, 0)] = testBundle(t, 1, 10)
			},
		}, {
			name:   "short_source_tile",
			cpSize: 300,
			modify: func(_ *testing.T, s *fakeGeometryStore) {
				s.objects[layout.TilePath(0, 1, 0)] = testTile(0, 1, 10)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newFakeGeometryStore(t, testTreeSize)
			if test.modify != nil {
				test.modify(t, s)
			}

			err := EnsureGeometry(t.Context(), s.io(), test.cpSize, testTreeSize)
			if err == nil {
				t.Fatalf("EnsureGeometry(%d, %d) = nil, want error", test.cpSize, testTreeSize)
			}
			if test.wantErrIs != nil && !errors.Is(err, test.wantErrIs) {
				t.Errorf("EnsureGeometry(%d, %d) = %v, want error wrapping %v", test.cpSize, testTreeSize, err, test.wantErrIs)
			}
		})
	}
}

func TestMaxLevel(t *testing.T) {
	for _, tc := range []struct {
		size uint64
		want int
	}{
		{0, 0},
		{1, 0},
		{256, 1},
		{257, 1},
		{32769, 1},
		{65535, 1},
		{65536, 2},
	} {
		t.Run(fmt.Sprintf("size_%d", tc.size), func(t *testing.T) {
			if got := maxLevel(tc.size); got != tc.want {
				t.Errorf("maxLevel(%d) = %d, want %d", tc.size, got, tc.want)
			}
		})
	}
}

func TestMarshalTlogEntryBundle(t *testing.T) {
	maxEntry := bytes.Repeat([]byte{'x'}, 1<<16-1)
	for _, test := range []struct {
		name    string
		entries [][]byte
		want    []byte
		wantErr bool
	}{
		{
			name:    "no_entries",
			entries: [][]byte{},
			want:    []byte{},
		}, {
			name:    "empty_entry",
			entries: [][]byte{{}},
			want:    []byte{0x00, 0x00},
		}, {
			name:    "two_entries",
			entries: [][]byte{[]byte("a"), []byte("bc")},
			want:    []byte{0x00, 0x01, 'a', 0x00, 0x02, 'b', 'c'},
		}, {
			name:    "max_length_entry",
			entries: [][]byte{maxEntry},
			want:    append([]byte{0xff, 0xff}, maxEntry...),
		}, {
			name:    "too_long_entry",
			entries: [][]byte{make([]byte, 1<<16)},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := MarshalTlogEntryBundle(&api.EntryBundle{Entries: test.entries})
			if err != nil {
				if !test.wantErr {
					t.Fatalf("MarshalTlogEntryBundle() = %v, want no error", err)
				}
				return
			}
			if test.wantErr {
				t.Fatal("MarshalTlogEntryBundle() succeeded, want error")
			}
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("MarshalTlogEntryBundle() mismatch (-want +got):\n%s", diff)
			}

			// The serialization must round-trip through api.EntryBundle.
			eb := &api.EntryBundle{}
			if err := eb.UnmarshalText(got); err != nil {
				t.Fatalf("UnmarshalText(MarshalTlogEntryBundle()) = %v, want nil", err)
			}
			if diff := cmp.Diff(test.entries, eb.Entries); diff != "" {
				t.Errorf("UnmarshalText(MarshalTlogEntryBundle()) mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// fakeGeometryStore is an in-memory backend for PartialResourceIO.
type fakeGeometryStore struct {
	objects map[string][]byte
	// readErr, if set, is returned by every read.
	readErr       error
	reads, writes int
}

// newFakeGeometryStore returns a store holding all the tiles and entry bundles
// of a tree of the given size.
func newFakeGeometryStore(t *testing.T, treeSize uint64) *fakeGeometryStore {
	t.Helper()
	s := &fakeGeometryStore{objects: make(map[string][]byte)}
	for level := uint64(0); treeSize>>(level*layout.TileHeight) > 0; level++ {
		nodes := treeSize >> (level * layout.TileHeight)
		for index := uint64(0); index*layout.TileWidth < nodes; index++ {
			p := layout.PartialTileSize(level, index, treeSize)
			s.objects[layout.TilePath(level, index, p)] = testTile(level, index, p)
			if level == 0 {
				s.objects[testBundlePath(index, p)] = testBundle(t, index, p)
			}
		}
	}
	return s
}

func (s *fakeGeometryStore) io() PartialResourceIO {
	return PartialResourceIO{
		ReadTile: func(_ context.Context, level, index uint64, p uint8) ([]byte, error) {
			return s.read(layout.TilePath(level, index, p))
		},
		WriteTile: func(_ context.Context, level, index uint64, p uint8, data []byte) error {
			return s.write(layout.TilePath(level, index, p), data)
		},
		ReadEntryBundle: func(_ context.Context, index uint64, p uint8) ([]byte, error) {
			return s.read(testBundlePath(index, p))
		},
		WriteEntryBundle: func(_ context.Context, index uint64, p uint8, data []byte) error {
			return s.write(testBundlePath(index, p), data)
		},
	}
}

// read returns exactly the named object: there's no partial to full fallback.
func (s *fakeGeometryStore) read(path string) ([]byte, error) {
	s.reads++
	if s.readErr != nil {
		return nil, s.readErr
	}
	d, ok := s.objects[path]
	if !ok {
		return nil, fmt.Errorf("%s: %w", path, os.ErrNotExist)
	}
	return d, nil
}

// write is idempotent: it only fails if the object exists with different data.
func (s *fakeGeometryStore) write(path string, data []byte) error {
	s.writes++
	if d, ok := s.objects[path]; ok && !bytes.Equal(d, data) {
		return fmt.Errorf("%s already exists with different data", path)
	}
	s.objects[path] = data
	return nil
}

// testBundlePath returns the path at which fakeGeometryStore stores an entry bundle.
// It's deliberately not layout.EntriesPath: EnsureGeometry must leave entry bundle
// paths to the storage implementation.
func testBundlePath(index uint64, p uint8) string {
	return fmt.Sprintf("bundle/%d.p/%d", index, p)
}

// testTile returns a tile whose node hashes are derived from their position in
// the tree, so that a partial tile is a prefix of all larger versions of it.
// A p of zero means a full tile.
func testTile(level, index uint64, p uint8) []byte {
	n := uint64(p)
	if n == 0 {
		n = layout.TileWidth
	}
	d := make([]byte, 0, n*sha256.Size)
	for i := range n {
		h := sha256.Sum256(fmt.Appendf(nil, "node %d/%d", level, index*layout.TileWidth+i))
		d = append(d, h[:]...)
	}
	return d
}

// testBundle is the entry bundle equivalent of testTile.
func testBundle(t *testing.T, index uint64, p uint8) []byte {
	t.Helper()
	n := uint64(p)
	if n == 0 {
		n = layout.EntryBundleWidth
	}
	b := &api.EntryBundle{}
	for i := range n {
		b.Entries = append(b.Entries, fmt.Appendf(nil, "entry %d", index*layout.EntryBundleWidth+i))
	}
	d, err := MarshalTlogEntryBundle(b)
	if err != nil {
		t.Fatalf("MarshalTlogEntryBundle() = %v, want no error", err)
	}
	return d
}
