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

package tessera

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestDedup(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		desc     string
		newValue string
		wantIdx  uint64
		wantDup  bool
	}{
		{
			desc:     "first element",
			newValue: "foo",
			wantIdx:  1,
			wantDup:  true,
		},
		{
			desc:     "third element",
			newValue: "baz",
			wantIdx:  3,
			wantDup:  true,
		},
		{
			desc:     "new element",
			newValue: "omega",
			wantIdx:  4,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			idx := uint64(1)
			delegate := func(ctx context.Context, e *Entry) IndexFuture {
				thisIdx := idx
				idx++
				return func() (Index, error) {
					return Index{Index: thisIdx}, nil
				}
			}
			dedupAdd := newInMemoryDedup(256)(delegate)

			// Add foo, bar, baz to prime the cache to make things interesting
			for _, s := range []string{"foo", "bar", "baz"} {
				if _, err := dedupAdd(ctx, NewEntry([]byte(s)))(); err != nil {
					t.Fatalf("dedupAdd(%q): %v", s, err)
				}
			}

			i, err := dedupAdd(ctx, NewEntry([]byte(tC.newValue)))()
			if err != nil {
				t.Fatalf("dedupAdd(%q): %v", tC.newValue, err)
			}
			if i.Index != tC.wantIdx {
				t.Errorf("got Index != want Index (%d != %d)", i.Index, tC.wantIdx)
			}
			if i.IsDup != tC.wantDup {
				t.Errorf("got IsDup != want IsDup(%t != %t)", i.IsDup, tC.wantDup)

			}
		})
	}
}

func TestDedupDoesNotCacheError(t *testing.T) {
	idx := uint64(0)
	rErr := true

	// This delegate will return an error the first time it's called, but all further
	// calls will succeed.
	// When an error is returned, no entry will be "added" to the tree.
	delegate := func(ctx context.Context, e *Entry) IndexFuture {
		thisIdx := idx
		// Don't add an entry if we're returning an error
		if !rErr {
			idx++
		}
		return func() (Index, error) {
			var err error
			// Return an error just the first time we're called
			if rErr {
				err = errors.New("bad thing happened")
				rErr = false
			}
			return Index{Index: thisIdx}, err
		}
	}
	dedupAdd := newInMemoryDedup(256)(delegate)

	k := "foo"
	for i := range 10 {
		idx, err := dedupAdd(t.Context(), NewEntry([]byte(k)))()

		// We expect an error from the delegate the first time.
		if i == 0 && err == nil {
			t.Errorf("dedupAdd(%q)@%d: was successful, want error", k, i)
			continue
		}
		// But the 2nd call should work.
		if i > 0 && err != nil {
			t.Errorf("dedupAdd(%q)@%d: got %v, want no error", k, i, err)
			continue
		}

		// After which, all subsequent adds should dedup to that successful add.
		if i > 1 && !idx.IsDup {
			t.Errorf("got IsDup=false, want isDup=true")
			continue
		}
		if idx.Index != 0 {
			t.Errorf("got index=%d, want %d", idx.Index, 1)
		}
	}
}

func BenchmarkDedup(b *testing.B) {
	ctx := context.Background()
	// Outer loop is for benchmark calibration, inside here is each individual run of the benchmark
	for b.Loop() {
		idx := uint64(1)
		delegate := func(ctx context.Context, e *Entry) IndexFuture {
			thisIdx := idx
			idx++
			return func() (Index, error) {
				return Index{Index: thisIdx}, nil
			}
		}
		dedupAdd := newInMemoryDedup(256)(delegate)
		wg := &sync.WaitGroup{}
		// Loop to create a bunch of leaves in parallel to test lock contention
		for leafIndex := range 1024 {
			wg.Add(1)
			go func(index int) {
				_, err := dedupAdd(ctx, NewEntry(fmt.Appendf(nil, "leaf with value %d", index%sha256.Size)))()
				if err != nil {
					b.Error(err)
				}
				wg.Done()
			}(leafIndex)
		}
		wg.Wait()
	}
}
