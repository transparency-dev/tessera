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

package client

import (
	"testing"
)

func TestStreamEntries(t *testing.T) {
	/*
		ctx := context.Background()

		logSize1 := 12345
		logSize2 := 100045

			var logSize atomic.Uint64
			logSize.Store(uint64(logSize1))

			s := &LogReader{
				lrs: logResourceStore{
					objStore:    m,
					entriesPath: layout.EntriesPath,
				},
				integratedSize: func(context.Context) (uint64, error) { return logSize.Load(), nil },
			}

			// Populate entry bundles:
			// first to logSize1 (so we're sure we've got the partial bundle)
			for r, idx := logSize1, uint64(0); r > 0; idx++ {
				sz := min(r, layout.EntryBundleWidth)
				b := makeBundle(t, idx, sz)
				if err := s.lrs.setEntryBundle(ctx, idx, uint8(sz), b); err != nil {
					t.Fatalf("setEntryBundle(%d): %v", idx, err)
				}
				r -= sz
			}
			// Then on to logSize2
			for r, idx := logSize2, uint64(0); r > 0; idx++ {
				sz := min(r, layout.EntryBundleWidth)
				b := makeBundle(t, idx, sz)
				if err := s.lrs.setEntryBundle(ctx, idx, uint8(sz), b); err != nil {
					t.Fatalf("setEntryBundle(%d): %v", idx, err)
				}
				r -= sz
			}

			// Finally, try to stream all the bundles back.
			// We'll first try to stream up to logSize1, then when we reach it we'll
			// make the tree appear to grow to logSize2 to test resuming.
			seenEntries := uint64(0)

			for gotEntry, gotErr := range s.StreamEntries(ctx, 0, uint64(logSize2)) {
				if gotErr != nil {
					t.Fatalf("gotErr after %d: %v", seenEntries, gotErr)
				}
				if e := gotEntry.RangeInfo.Index*layout.EntryBundleWidth + uint64(gotEntry.RangeInfo.First); e != seenEntries {
					t.Fatalf("got idx %d, want %d", e, seenEntries)
				}
				seenEntries += uint64(gotEntry.RangeInfo.N)
				t.Logf("got RI %d / %d", gotEntry.RangeInfo.Index, seenEntries)

				switch seenEntries {
				case uint64(logSize1):
					// We've fetched all the entries from the original tree size, now we'll make
					// the tree appear to have grown to the final size.
					// The stream should start returning bundles again until we've consumed them all.
					t.Log("Reached logSize, growing tree")
					logSize.Store(uint64(logSize2))
					time.Sleep(time.Second)
				}
			}
	*/
}
