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

package client_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/transparency-dev/tessera"

	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/testonly"
)

func TestEntryBundles(t *testing.T) {
	ctx := t.Context()

	logSize1 := uint64(12345)
	logSize2 := uint64(100045)

	tl, done := testonly.NewTestLog(t, tessera.NewAppendOptions().WithBatching(30000, time.Second).WithCheckpointInterval(time.Second))
	defer func() {
		if err := done(ctx); err != nil {
			t.Fatalf("done: %v", err)
		}
	}()

	if _, err := populateEntries(ctx, tl, logSize1, "first"); err != nil {
		t.Fatalf("populateEntries(first): %v", err)
	}
	if _, err := populateEntries(ctx, tl, logSize2-logSize1, "second"); err != nil {
		t.Fatalf("populateEntries(second): %v", err)
	}

	var logSize atomic.Uint64
	logSize.Store(uint64(logSize1))
	size := func(ctx context.Context) (uint64, error) {
		return logSize.Load(), nil
	}

	// Finally, try to stream all the bundles back.
	// We'll first try to stream up to logSize1, then when we reach it we'll
	// make the tree appear to grow to logSize2 to test resuming.
	seenEntries := uint64(0)

	for gotEntry, gotErr := range client.EntryBundles(ctx, 2, size, tl.LogReader.ReadEntryBundle, 0, uint64(logSize2)) {
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
}

func TestEntries(t *testing.T) {
	ctx := t.Context()

	logSize := uint64(1234)

	tl, done := testonly.NewTestLog(t, tessera.NewAppendOptions().WithBatching(uint(logSize), time.Second).WithCheckpointInterval(time.Second))
	defer func() {
		if err := done(ctx); err != nil {
			t.Fatalf("done: %v", err)
		}
	}()

	// Put some entries into a log.
	es, err := populateEntries(ctx, tl, logSize, "first")
	if err != nil {
		t.Fatalf("populateEntries(): %v", err)
	}
	wantEntries := make(map[string]struct{})
	for _, e := range es {
		wantEntries[string(e)] = struct{}{}
	}

	unbundle := func(bundle []byte) ([][]byte, error) {
		eb := &api.EntryBundle{}
		if err := eb.UnmarshalText(bundle); err != nil {
			return nil, err
		}
		return eb.Entries, nil
	}

	// Now stream back entries and check that we saw all the entries we added above.
	eCh := make(chan []byte)
	size := func(ctx context.Context) (uint64, error) {
		return logSize, nil
	}
	go func() {
		defer close(eCh)
		for gotEntry, gotErr := range client.Entries(client.EntryBundles(ctx, 2, size, tl.LogReader.ReadEntryBundle, 0, logSize), unbundle) {
			if gotErr != nil {
				t.Errorf("gotErr: %v", gotErr)
			}
			eCh <- gotEntry.Entry
		}
	}()

	for e := range eCh {
		k := string(e)
		if _, ok := wantEntries[k]; !ok {
			t.Errorf("Expected missing entry %q - already seen?", k)
		}
		delete(wantEntries, k)
	}

	if l := len(wantEntries); l > 0 {
		t.Fatalf("Did not see %d expected entries", l)
	}
}

func populateEntries(ctx context.Context, tl *testonly.TestLog, N uint64, ep string) ([][]byte, error) {
	es := make([][]byte, 0, N)
	fs := make([]tessera.IndexFuture, 0, N)
	for i := range N {
		e := fmt.Appendf(nil, "%s-%d", ep, i)
		es = append(es, e)
		fs = append(fs, tl.Appender.Add(ctx, tessera.NewEntry(e)))
	}

	a := tessera.NewPublicationAwaiter(ctx, tl.LogReader.ReadCheckpoint, time.Second)
	for _, f := range fs {
		if _, _, err := a.Await(ctx, f); err != nil {
			return nil, err
		}
	}
	return es, nil
}
