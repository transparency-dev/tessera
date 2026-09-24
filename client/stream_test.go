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
	"errors"
	"fmt"
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

	logSize := uint64(12345)

	tl, done := testonly.NewTestLog(t, tessera.NewAppendOptions().WithBatching(30000, time.Second).WithCheckpointInterval(time.Second))
	defer func() {
		if err := done(ctx); err != nil {
			t.Fatalf("done: %v", err)
		}
	}()

	if _, err := populateEntries(t, tl, logSize, "first"); err != nil {
		t.Fatalf("populateEntries(first): %v", err)
	}

	size := func(ctx context.Context) (uint64, error) {
		return logSize, nil
	}

	// Finally, try to stream all the bundles back.
	seenEntries := uint64(0)

	for gotEntry, gotErr := range client.EntryBundles(ctx, 2, size, tl.LogReader.ReadEntryBundle, 0, logSize) {
		if gotErr != nil {
			t.Fatalf("gotErr after %d: %v", seenEntries, gotErr)
		}
		if e := gotEntry.RangeInfo.Index*layout.EntryBundleWidth + uint64(gotEntry.RangeInfo.First); e != seenEntries {
			t.Fatalf("got idx %d, want %d", e, seenEntries)
		}
		seenEntries += uint64(gotEntry.RangeInfo.N)
		t.Logf("got RI %d / %d", gotEntry.RangeInfo.Index, seenEntries)
	}
	if seenEntries != logSize {
		t.Fatalf("got seenEntries %d, want %d", seenEntries, logSize)
	}
}

// syntheticLog returns a getSize/getBundle pair over a synthetic log of numBundles bundles.
// The returned bundle data is not parseable; these tests only care about the streaming machinery.
func syntheticLog(numBundles uint64) (client.TreeSizeFunc, client.EntryBundleFetcherFunc) {
	size := numBundles * layout.EntryBundleWidth
	return func(context.Context) (uint64, error) { return size, nil },
		func(_ context.Context, idx uint64, _ uint8) ([]byte, error) {
			return fmt.Appendf(nil, "bundle-%d", idx), nil
		}
}

func TestEntryBundlesStopCancelsInFlightFetches(t *testing.T) {
	const numBundles = 20
	const numWorkers = 4

	getSize, _ := syntheticLog(numBundles)

	// The first bundle is served immediately so that the consumer below gets something to yield.
	// Every subsequent fetch, i.e. all the in-flight read-aheads, blocks until its context is
	// cancelled, and reports the fact that it was.
	blocking := make(chan struct{}, numBundles)
	cancelled := make(chan struct{}, numBundles)
	getBundle := func(ctx context.Context, idx uint64, _ uint8) ([]byte, error) {
		if idx == 0 {
			return []byte("bundle-0"), nil
		}
		blocking <- struct{}{}
		<-ctx.Done()
		cancelled <- struct{}{}
		return nil, ctx.Err()
	}

	for _, err := range client.EntryBundles(t.Context(), numWorkers, getSize, getBundle, 0, numBundles*layout.EntryBundleWidth) {
		if err != nil {
			t.Fatalf("Failed to iterate: %v", err)
		}
		// Wait until the read-ahead definitely has a fetch in flight before we stop,
		// so that there's something for the stop to cancel.
		select {
		case <-blocking:
		case <-t.Context().Done():
			t.Fatalf("timed out")
		}

		// Now break out of the iterator, which should cancel the in-flight fetches.
		break
	}

	select {
	case <-cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight getBundle calls were not cancelled when iteration stopped early")
	}
}

func TestEntryBundlesReportsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	getSize, getBundle := syntheticLog(1000)
	n, gotErr := 0, error(nil)
	for _, err := range client.EntryBundles(ctx, 2, getSize, getBundle, 0, 1000*layout.EntryBundleWidth) {
		gotErr = err
		if err != nil {
			break
		}
		if n++; n == 3 {
			cancel()
		}
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("got err %v after %d bundles, want context.Canceled", gotErr, n)
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
	es, err := populateEntries(t, tl, logSize, "first")
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

func populateEntries(t *testing.T, tl *testonly.TestLog, N uint64, ep string) ([][]byte, error) {
	t.Helper()

	es := make([][]byte, 0, N)
	fs := make([]tessera.IndexFuture, 0, N)
	for i := range N {
		e := fmt.Appendf(nil, "%s-%d", ep, i)
		es = append(es, e)
		fs = append(fs, tl.Appender.Add(t.Context(), tessera.NewEntry(e)))
	}
	t.Logf("Added %d entries", N)

	a := tessera.NewPublicationAwaiter(t.Context(), tl.LogReader.ReadCheckpoint, time.Second)
	for _, f := range fs {
		if _, _, err := a.Await(t.Context(), f); err != nil {
			return nil, err
		}
	}
	return es, nil
}
