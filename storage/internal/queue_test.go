// Copyright 2024 The Tessera authors. All Rights Reserved.
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

package storage_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/transparency-dev/tessera"
	storage "github.com/transparency-dev/tessera/storage/internal"
)

func TestQueue(t *testing.T) {
	for _, test := range []struct {
		name       string
		numItems   uint64
		maxEntries int
		maxWait    time.Duration
	}{
		{
			name:       "small",
			numItems:   100,
			maxEntries: 200,
			maxWait:    time.Second,
		}, {
			name:       "more items than queue space",
			numItems:   100,
			maxEntries: 20,
			maxWait:    time.Second,
		}, {
			name:       "much flushing",
			numItems:   100,
			maxEntries: 100,
			maxWait:    time.Microsecond,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			assignMu := sync.Mutex{}
			assignedItems := make([]*tessera.Entry, test.numItems)
			assignedIndex := uint64(0)

			// flushFunc mimics sequencing storage - it takes entries, assigns them to
			// positions in assignedItems.
			flushFunc := func(_ context.Context, entries []*tessera.Entry) error {
				assignMu.Lock()
				defer assignMu.Unlock()

				for _, e := range entries {
					_ = e.MarshalBundleData(assignedIndex)
					assignedItems[assignedIndex] = e
					assignedIndex++
				}
				return nil
			}

			// Create the Queue
			q := storage.NewQueue(ctx, test.maxWait, uint(test.maxEntries), flushFunc)

			// Now submit a bunch of entries
			adds := make([]tessera.IndexFuture, test.numItems)
			wantEntries := make([]*tessera.Entry, test.numItems)
			for i := uint64(0); i < test.numItems; i++ {
				d := fmt.Appendf(nil, "item %d", i)
				wantEntries[i] = tessera.NewEntry(d)
				adds[i] = q.Add(ctx, wantEntries[i])
			}

			for i, r := range adds {
				N, err := r()
				if err != nil {
					t.Errorf("Add: %v", err)
					return
				}
				if got, want := assignedItems[N.Index].Data(), wantEntries[i].Data(); !reflect.DeepEqual(got, want) {
					t.Errorf("Got item@%d %v, want %v", N.Index, got, want)
				}
			}
		})
	}
}

func TestNotify(t *testing.T) {
	for _, test := range []struct {
		name    string
		setIdx  int64
		setErr  error
		wantErr bool
	}{
		{
			name:    "just idx, no error",
			setIdx:  200,
			setErr:  nil,
			wantErr: false,
		}, {
			name:    "just error",
			setIdx:  -1,
			setErr:  errors.New("expected error"),
			wantErr: true,
		}, {
			name:    "error and idx",
			setIdx:  200,
			setErr:  errors.New("expected error"),
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()

			// flushFunc mimics sequencing storage - it takes entries, assigns them to
			// positions in assignedItems.
			flushFunc := func(_ context.Context, entries []*tessera.Entry) error {
				if got := len(entries); got != 1 {
					t.Fatalf("expected 1 entry but got %d", got)
				}
				if test.setIdx >= 0 {
					_ = entries[0].MarshalBundleData(uint64(test.setIdx))
				}
				return test.setErr
			}

			// Create the Queue
			q := storage.NewQueue(ctx, time.Second, uint(1), flushFunc)

			// Now submit the entry
			added := q.Add(ctx, tessera.NewEntry([]byte(test.name)))
			_, err := added()
			if gotErr, wantErr := err != nil, test.wantErr; gotErr != wantErr {
				t.Errorf("gotErr != wantErr (%t != %t): %v", gotErr, wantErr, err)
			}
		})
	}
}

func BenchmarkQueue(b *testing.B) {
	ctx := b.Context()
	const count = 1024
	// Outer loop is for benchmark calibration, inside here is each individual run of the benchmark
	for b.Loop() {
		flushFn := func(_ context.Context, entries []*tessera.Entry) error {
			for _, e := range entries {
				_ = e.MarshalBundleData(0)
			}
			return nil
		}
		q := storage.NewQueue(ctx, time.Second, 256, flushFn)

		adds := make([]tessera.IndexFuture, 0, count)
		for leafIndex := range count {
			f := q.Add(ctx, tessera.NewEntry([]byte{byte(leafIndex)}))
			adds = append(adds, f)
		}

		for _, r := range adds {
			_, err := r()
			if err != nil {
				b.Errorf("Add: %v", err)
				return
			}
		}
	}
}
