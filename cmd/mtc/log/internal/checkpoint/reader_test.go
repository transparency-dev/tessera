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
