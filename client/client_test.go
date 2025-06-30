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

package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"golang.org/x/mod/sumdb/note"
)

var (
	testOrigin      = "example.com/log/testdata"
	testLogVerifier = mustMakeVerifier("example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx")
	// Built using testdata/build_log.sh
	testRawCheckpoints, testCheckpoints = mustLoadTestCheckpoints()
)

func mustMakeVerifier(vs string) note.Verifier {
	v, err := note.NewVerifier(vs)
	if err != nil {
		panic(fmt.Errorf("NewVerifier(%q): %v", vs, err))
	}
	return v
}

func mustLoadTestCheckpoints() ([][]byte, []log.Checkpoint) {
	raws, cps := make([][]byte, 0), make([]log.Checkpoint, 0)
	for i := 0; ; i++ {
		cpName := fmt.Sprintf("checkpoint.%d", i)
		r, err := testLogFetcher(context.Background(), cpName)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// Probably just no more checkpoints left
				break
			}
			panic(err)
		}
		cp, _, _, err := log.ParseCheckpoint(r, testOrigin, testLogVerifier)
		if err != nil {
			panic(fmt.Errorf("ParseCheckpoint(%s): %v", cpName, err))
		}
		raws, cps = append(raws, r), append(cps, *cp)
	}
	if len(raws) == 0 {
		panic("no checkpoints loaded")
	}
	return raws, cps
}

// testLogFetcher is a fetcher which reads from the checked-in golden test log
// data stored in ../testdata/log
func testLogFetcher(_ context.Context, p string) ([]byte, error) {
	path := filepath.Join("../testdata/log", p)
	return os.ReadFile(path)
}

func testLogTileFetcher(ctx context.Context, l, i uint64, p uint8) ([]byte, error) {
	return testLogFetcher(ctx, layout.TilePath(l, i, p))
}

// fetchCheckpointShim allows fetcher requests for checkpoints to be intercepted.
type fetchCheckpointShim struct {
	// Checkpoints holds raw checkpoints to be returned when the fetcher is asked to retrieve a checkpoint path.
	// The zero-th entry will be returned until Advance is called.
	Checkpoints [][]byte
}

// Fetcher intercepts requests for the checkpoint file, returning the zero-th
// entry in the Checkpoints field. All other requests are passed through
// to the delegate fetcher.
func (f *fetchCheckpointShim) FetchCheckpoint(ctx context.Context) ([]byte, error) {
	if len(f.Checkpoints) == 0 {
		return nil, os.ErrNotExist
	}
	r := f.Checkpoints[0]
	return r, nil
}

// Advance causes subsequent intercepted checkpoint requests to return
// the next entry in the Checkpoints slice.
func (f *fetchCheckpointShim) Advance() {
	f.Checkpoints = f.Checkpoints[1:]
}

func TestCheckLogStateTracker(t *testing.T) {
	ctx := context.Background()

	for _, test := range []struct {
		desc       string
		cpRaws     [][]byte
		wantCpRaws [][]byte
	}{
		{
			desc: "Consistent",
			cpRaws: [][]byte{
				testRawCheckpoints[0],
				testRawCheckpoints[2],
				testRawCheckpoints[3],
				testRawCheckpoints[5],
				testRawCheckpoints[6],
				testRawCheckpoints[10],
			},
			wantCpRaws: [][]byte{
				testRawCheckpoints[0],
				testRawCheckpoints[2],
				testRawCheckpoints[3],
				testRawCheckpoints[5],
				testRawCheckpoints[6],
				testRawCheckpoints[10],
			},
		}, {
			desc: "Identical CP",
			cpRaws: [][]byte{
				testRawCheckpoints[0],
				testRawCheckpoints[0],
				testRawCheckpoints[0],
				testRawCheckpoints[0],
			},
			wantCpRaws: [][]byte{
				testRawCheckpoints[0],
				testRawCheckpoints[0],
				testRawCheckpoints[0],
				testRawCheckpoints[0],
			},
		}, {
			desc: "Identical CP pairs",
			cpRaws: [][]byte{
				testRawCheckpoints[0],
				testRawCheckpoints[0],
				testRawCheckpoints[5],
				testRawCheckpoints[5],
			},
			wantCpRaws: [][]byte{
				testRawCheckpoints[0],
				testRawCheckpoints[0],
				testRawCheckpoints[5],
				testRawCheckpoints[5],
			},
		}, {
			desc: "Out of order",
			cpRaws: [][]byte{
				testRawCheckpoints[5],
				testRawCheckpoints[2],
				testRawCheckpoints[0],
				testRawCheckpoints[3],
			},
			wantCpRaws: [][]byte{
				testRawCheckpoints[5],
				testRawCheckpoints[5],
				testRawCheckpoints[5],
				testRawCheckpoints[5],
			},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			shim := fetchCheckpointShim{Checkpoints: test.cpRaws}
			lst, err := NewLogStateTracker(ctx, testLogTileFetcher, testRawCheckpoints[0], testLogVerifier, testOrigin, UnilateralConsensus(shim.FetchCheckpoint))
			if err != nil {
				t.Fatalf("NewLogStateTracker: %v", err)
			}

			for i := range test.cpRaws {
				_, _, newCP, err := lst.Update(ctx)
				if err != nil {
					t.Errorf("Update %d: %v", i, err)
				}
				if got, want := newCP, test.wantCpRaws[i]; !bytes.Equal(got, want) {
					t.Errorf("Update moved to:\n%s\nwant:\n%s", string(got), string(want))
				}

				shim.Advance()
			}
		})
	}
}

func TestNodeCacheHandlesInvalidRequest(t *testing.T) {
	ctx := context.Background()
	wantBytes := []byte("0123456789ABCDEF0123456789ABCDEF")
	f := func(_ context.Context, _, _ uint64, _ uint8) ([]byte, error) {
		h := &api.HashTile{
			Nodes: [][]byte{wantBytes},
		}
		return h.MarshalText()
	}

	// Large tree, but we're emulating skew since f, above, will return a tile which only knows about 1
	// leaf.
	nc := newNodeCache(f, 10)

	if got, err := nc.GetNode(ctx, compact.NewNodeID(0, 0)); err != nil {
		t.Errorf("got %v, want no error", err)
	} else if !bytes.Equal(got, wantBytes) {
		t.Errorf("got %v, want %v", got, wantBytes)
	}

	if _, err := nc.GetNode(ctx, compact.NewNodeID(0, 1)); err == nil {
		t.Error("got no error, want error because ID is out of range")
	}
}

func TestHandleZeroRoot(t *testing.T) {
	zeroCP := testCheckpoints[0]
	if zeroCP.Size != 0 {
		t.Fatal("BadData: checkpoint has non-zero size")
	}
	if len(zeroCP.Hash) == 0 {
		t.Fatal("BadTestData: checkpoint.0 has empty root hash")
	}
	if _, err := NewProofBuilder(context.Background(), zeroCP.Size, testLogTileFetcher); err != nil {
		t.Fatalf("NewProofBuilder: %v", err)
	}
}

func TestGetEntryBundleAddressing(t *testing.T) {
	for _, test := range []struct {
		name                string
		idx                 uint64
		clientLogSize       uint64
		actualLogSize       uint64
		wantPartialTileSize uint8
	}{
		{
			name:                "works - partial tile",
			idx:                 0,
			clientLogSize:       34,
			actualLogSize:       34,
			wantPartialTileSize: 34,
		},
		{
			name:                "works - full tile",
			idx:                 1,
			clientLogSize:       layout.TileWidth*2 + 45,
			actualLogSize:       layout.TileWidth*2 + 45,
			wantPartialTileSize: 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotIdx := uint64(0)
			gotTileSize := uint8(0)
			f := func(_ context.Context, i uint64, sz uint8) ([]byte, error) {
				gotIdx = i
				gotTileSize = sz
				p := layout.PartialTileSize(0, i, test.actualLogSize)
				if p != sz {
					return nil, os.ErrNotExist
				}
				return []byte{}, nil
			}
			_, err := GetEntryBundle(context.Background(), f, test.idx, test.clientLogSize)
			if err != nil {
				t.Fatalf("GetEntryBundle: %v", err)
			}
			if gotIdx != test.idx {
				t.Errorf("f got idx %d, want %d", gotIdx, test.idx)
			}
			if gotTileSize != test.wantPartialTileSize {
				t.Errorf("f got tileSize %d, want %d", gotTileSize, test.wantPartialTileSize)
			}
		})
	}
}

func TestNodeFetcherAddressing(t *testing.T) {
	for _, test := range []struct {
		name                string
		nodeLevel           uint
		nodeIdx             uint64
		clientLogSize       uint64
		actualLogSize       uint64
		wantPartialTileSize uint8
	}{
		{
			name:                "works - partial tile",
			nodeIdx:             0,
			clientLogSize:       34,
			actualLogSize:       34,
			wantPartialTileSize: 34,
		},
		{
			name:                "works - full tile",
			nodeIdx:             56,
			clientLogSize:       layout.TileWidth*2 + 45,
			actualLogSize:       layout.TileWidth*2 + 45,
			wantPartialTileSize: 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotLevel, gotIdx, gotTileSize := uint(0), uint64(0), uint8(0)
			f := func(_ context.Context, l, i uint64, sz uint8) ([]byte, error) {
				gotLevel = uint(l)
				gotIdx = i
				gotTileSize = sz
				p := layout.PartialTileSize(l, i, test.actualLogSize)
				if p != sz {
					return nil, os.ErrNotExist
				}
				r := api.HashTile{}
				s := int(sz)
				if s == 0 {
					s = layout.TileWidth
				}
				for x := range s {
					h := sha256.Sum256(fmt.Appendf(nil, "node at %d/%d", l, i+uint64(x)))
					r.Nodes = append(r.Nodes, h[:])
				}
				return r.MarshalText()
			}
			pb, err := NewProofBuilder(t.Context(), test.clientLogSize, f)
			if err != nil {
				t.Fatalf("NewProofBuilder: %v", err)
			}
			_, err = pb.fetchNodes(t.Context(), proof.Nodes{IDs: []compact.NodeID{compact.NewNodeID(test.nodeLevel, test.nodeIdx)}})
			if err != nil {
				t.Fatalf("fetchNodes: %v", err)
			}
			if wantLevel := test.nodeLevel >> layout.TileHeight; gotLevel != wantLevel {
				t.Errorf("f got level %d, want %d", gotLevel, wantLevel)
			}
			if wantIdx := test.nodeIdx >> layout.TileHeight; gotIdx != wantIdx {
				t.Errorf("f got idx %d, want %d", gotIdx, wantIdx)
			}
			if gotTileSize != test.wantPartialTileSize {
				t.Errorf("f got tileSize %d, want %d", gotTileSize, test.wantPartialTileSize)
			}
		})
	}
}
