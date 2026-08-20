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

package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
)

func mockCheckpoint(origin string, size uint64) []byte {
	return []byte(fmt.Sprintf("%s\n%d\nAAAA\n", origin, size))
}

func TestNewReader(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		readCP   func(context.Context) ([]byte, error)
		wantSize uint64
		wantErr  bool
	}{
		{
			name: "successful initial read",
			readCP: func(_ context.Context) ([]byte, error) {
				return mockCheckpoint("test.log", 100), nil
			},
			wantSize: 100,
		},
		{
			name: "successful initial read with size 0",
			readCP: func(_ context.Context) ([]byte, error) {
				return mockCheckpoint("test.log", 0), nil
			},
			wantSize: 0,
		},
		{
			name:    "nil readCheckpoint function",
			readCP:  nil,
			wantErr: true,
		},
		{
			name: "storage error on initial read",
			readCP: func(_ context.Context) ([]byte, error) {
				return nil, errors.New("network error")
			},
			wantErr: true,
		},
		{
			name: "malformed initial checkpoint",
			readCP: func(_ context.Context) ([]byte, error) {
				return []byte("not-a-valid-checkpoint"), nil
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewReader(ctx, tc.readCP)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewReader() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			size := r.LatestSize()
			if size != tc.wantSize {
				t.Errorf("LatestSize() = %d, want %d", size, tc.wantSize)
			}
		})
	}
}

func TestReader_Checkpoint(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		readCP   func(context.Context) ([]byte, error)
		wantCP   []byte
		wantSize uint64
		wantErr  bool
	}{
		{
			name: "successful read and update size",
			readCP: func(_ context.Context) ([]byte, error) {
				return mockCheckpoint("test.log", 200), nil
			},
			wantCP:   mockCheckpoint("test.log", 200),
			wantSize: 200,
		},
		{
			name: "storage error does not update size",
			readCP: func(_ context.Context) ([]byte, error) {
				return nil, errors.New("network error")
			},
			wantSize: 100,
			wantErr:  true,
		},
		{
			name: "malformed checkpoint does not update size",
			readCP: func(_ context.Context) ([]byte, error) {
				return []byte("not-a-valid-checkpoint"), nil
			},
			wantSize: 100,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			currentCP := mockCheckpoint("test.log", 100)
			r, err := NewReader(ctx, func(_ context.Context) ([]byte, error) {
				return currentCP, nil
			})
			if err != nil {
				t.Fatalf("NewReader() error: %v", err)
			}

			// Switch read function for subsequent Checkpoint call
			r.readCheckpoint = tc.readCP

			cp, err := r.Checkpoint(ctx)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Checkpoint() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && !bytes.Equal(cp, tc.wantCP) {
				t.Errorf("Checkpoint() = %q, want %q", cp, tc.wantCP)
			}

			size := r.LatestSize()
			if size != tc.wantSize {
				t.Errorf("LatestSize() = %d, want %d", size, tc.wantSize)
			}
		})
	}
}

func TestReader_LatestSize(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		initialSize   uint64
		updateSize    uint64 // if > 0, updates checkpoint to updateSize
		wantSize      uint64
		wantCallCount int
	}{
		{
			name:          "initial read cached without extra storage calls",
			initialSize:   100,
			wantSize:      100,
			wantCallCount: 1,
		},
		{
			name:          "initial size 0 cached without extra storage calls",
			initialSize:   0,
			wantSize:      0,
			wantCallCount: 1,
		},
		{
			name:          "size updated by checkpoint poll",
			initialSize:   100,
			updateSize:    200,
			wantSize:      200,
			wantCallCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			callCount := 0
			currentSize := tc.initialSize
			readCP := func(_ context.Context) ([]byte, error) {
				callCount++
				return mockCheckpoint("test.log", currentSize), nil
			}

			r, err := NewReader(ctx, readCP)
			if err != nil {
				t.Fatalf("NewReader() unexpected error: %v", err)
			}

			if tc.updateSize > 0 {
				currentSize = tc.updateSize
				if _, err := r.Checkpoint(ctx); err != nil {
					t.Fatalf("Checkpoint() unexpected error: %v", err)
				}
			}

			size := r.LatestSize()
			if size != tc.wantSize {
				t.Errorf("LatestSize() = %d, want %d", size, tc.wantSize)
			}

			// Subsequent LatestSize call returns cached size without triggering additional storage reads
			size = r.LatestSize()
			if size != tc.wantSize {
				t.Errorf("subsequent LatestSize() = %d, want %d", size, tc.wantSize)
			}
			if callCount != tc.wantCallCount {
				t.Errorf("subsequent callCount = %d, want %d", callCount, tc.wantCallCount)
			}
		})
	}
}

func TestReader_SubtreeForIndex(t *testing.T) {
	ctx := context.Background()
	currentSize := uint64(0)

	reader := func(_ context.Context) ([]byte, error) {
		return mockCheckpoint("test.log", currentSize), nil
	}

	r, err := NewReader(ctx, reader)
	if err != nil {
		t.Fatalf("NewReader() error: %v", err)
	}

	// Add checkpoints: 50, 120, 200
	// Subtrees created:
	// [0, 50) -> [0, 32) and [32, 50)
	// [50, 120) -> [48, 64) and [64, 120) (FindSubtrees expands left to power-of-2 48)
	// [120, 200) -> [120, 128) and [128, 200)
	for _, sz := range []uint64{50, 120, 200} {
		currentSize = sz
		if _, err := r.Checkpoint(ctx); err != nil {
			t.Fatalf("Checkpoint(%d) error: %v", sz, err)
		}
	}

	tests := []struct {
		name      string
		index     uint64
		wantStart uint64
		wantEnd   uint64
		wantErr   bool
	}{
		{name: "first index of first subtree [0, 32)", index: 0, wantStart: 0, wantEnd: 32, wantErr: false},
		{name: "middle of first subtree [0, 32)", index: 15, wantStart: 0, wantEnd: 32, wantErr: false},
		{name: "last index of first subtree [0, 32)", index: 31, wantStart: 0, wantEnd: 32, wantErr: false},
		{name: "first index of second subtree [32, 50)", index: 32, wantStart: 32, wantEnd: 50, wantErr: false},
		{name: "middle of second subtree [32, 50)", index: 40, wantStart: 32, wantEnd: 50, wantErr: false},
		{name: "overlapping index 48 in [48, 64)", index: 48, wantStart: 48, wantEnd: 64, wantErr: false},
		{name: "overlapping index 49 in [48, 64)", index: 49, wantStart: 48, wantEnd: 64, wantErr: false},
		{name: "first index of third subtree [48, 64)", index: 50, wantStart: 48, wantEnd: 64, wantErr: false},
		{name: "last index of third subtree [48, 64)", index: 63, wantStart: 48, wantEnd: 64, wantErr: false},
		{name: "first index of fourth subtree [64, 120)", index: 64, wantStart: 64, wantEnd: 120, wantErr: false},
		{name: "middle of fourth subtree [64, 120)", index: 100, wantStart: 64, wantEnd: 120, wantErr: false},
		{name: "last index of fourth subtree [64, 120)", index: 119, wantStart: 64, wantEnd: 120, wantErr: false},
		{name: "first index of fifth subtree [120, 128)", index: 120, wantStart: 120, wantEnd: 128, wantErr: false},
		{name: "last index of fifth subtree [120, 128)", index: 127, wantStart: 120, wantEnd: 128, wantErr: false},
		{name: "first index of sixth subtree [128, 200)", index: 128, wantStart: 128, wantEnd: 200, wantErr: false},
		{name: "last index of sixth subtree [128, 200)", index: 199, wantStart: 128, wantEnd: 200, wantErr: false},
		{name: "index equal to latest checkpoint size", index: 200, wantErr: true},
		{name: "future uncheckpointed index", index: 500, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start, end, err := r.SubtreeForIndex(tc.index)
			if (err != nil) != tc.wantErr {
				t.Fatalf("SubtreeForIndex(%d) error = %v, wantErr %v", tc.index, err, tc.wantErr)
			}
			if !tc.wantErr && (start != tc.wantStart || end != tc.wantEnd) {
				t.Errorf("SubtreeForIndex(%d) = [%d, %d), want [%d, %d)", tc.index, start, end, tc.wantStart, tc.wantEnd)
			}
		})
	}
}

func TestReader_CapacityPruning(t *testing.T) {
	ctx := context.Background()
	currentSize := uint64(0)

	reader := func(_ context.Context) ([]byte, error) {
		return mockCheckpoint("test.log", currentSize), nil
	}

	r, err := NewReader(ctx, reader)
	if err != nil {
		t.Fatalf("NewReader() error: %v", err)
	}

	numCheckpoints := maxSubtrees + 4
	// Add checkpoints until capacity (maxSubtrees) is exceeded and pruning occurs
	for i := uint64(1); i <= uint64(numCheckpoints); i++ {
		currentSize += 50
		if _, err := r.Checkpoint(ctx); err != nil {
			t.Fatalf("Checkpoint(%d) error: %v", currentSize, err)
		}
	}

	// Verify subtrees are capped at maxSubtrees
	if len(r.subtrees) != maxSubtrees {
		t.Fatalf("len(subtrees) = %d, want maxSubtrees %d", len(r.subtrees), maxSubtrees)
	}

	// The first subtree always starts at 0 and expands as older delta subtrees are pruned.
	if r.subtrees[0].start != 0 {
		t.Fatalf("subtrees[0].start = %d, want 0", r.subtrees[0].start)
	}
	if r.subtrees[0].end == 0 {
		t.Fatalf("subtrees[0].end should have expanded after %d checkpoints", numCheckpoints)
	}

	// Verify every subtree has no gaps with the next (subtrees may overlap due to FindSubtrees power-of-2 alignment).
	for i := 0; i < len(r.subtrees)-1; i++ {
		if r.subtrees[i].end < r.subtrees[i+1].start {
			t.Errorf("gap detected: subtrees[%d].end (%d) < subtrees[%d].start (%d)", i, r.subtrees[i].end, i+1, r.subtrees[i+1].start)
		}
	}

	// Verify SubtreeForIndex returns the first subtree for index 0 and index within subtrees[0].
	start, end, err := r.SubtreeForIndex(0)
	if err != nil || start != 0 || end != r.subtrees[0].end {
		t.Errorf("SubtreeForIndex(0) = [%d, %d), err=%v; want [0, %d), err=nil", start, end, err, r.subtrees[0].end)
	}
	start, end, err = r.SubtreeForIndex(r.subtrees[0].end - 1)
	if err != nil || start != 0 || end != r.subtrees[0].end {
		t.Errorf("SubtreeForIndex(%d) = [%d, %d), err=%v; want [0, %d), err=nil", r.subtrees[0].end-1, start, end, err, r.subtrees[0].end)
	}

	// Latest subtree is valid
	latest := r.subtrees[len(r.subtrees)-1]
	start, end, err = r.SubtreeForIndex(latest.start)
	if err != nil || start != latest.start || end != latest.end {
		t.Errorf("SubtreeForIndex(%d) = [%d, %d), err=%v; want [%d, %d), err=nil", latest.start, start, end, err, latest.start, latest.end)
	}

	// Future index beyond latest size returns error
	if _, _, err := r.SubtreeForIndex(latest.end); err == nil {
		t.Errorf("SubtreeForIndex(%d) expected error, got nil", latest.end)
	}
}
