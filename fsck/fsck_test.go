// Copyright 2025 The Tessera authors. All Rights Reserved.
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

package fsck

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/tessera/api/layout"
	"golang.org/x/mod/sumdb/note"
)

func TestTrimFullToPartial(t *testing.T) {
	for _, test := range []struct {
		name       string
		r          resource
		storedTile []byte
		wantErr    bool
	}{
		{
			name: "partial request, partial exists",
			r: resource{
				partial: 10,
				content: makeTile(10),
			},
			storedTile: makeTile(10),
		},
		{
			name: "partial request, full exists",
			r: resource{
				partial: 10,
				content: makeTile(10),
			},
			storedTile: makeTile(256),
		},
		{
			name: "invalid stored data",
			r: resource{
				content: makeTile(256),
			},
			storedTile: makeTile(2),
			wantErr:    true,
		},
		{
			name: "full request, full exists",
			r: resource{
				content: makeTile(256),
			},
			storedTile: makeTile(256),
		},
	} {
		t.Run(test.name, func(t *testing.T) {

			f := &fsckTree{
				expectedResources: make(chan resource, 1),
				fetcher: newCountingFetcher(&fakeFetcher{
					tile: test.storedTile,
				}),
				rangeTracker: newRangeTracker(1),
			}

			f.expectedResources <- test.r
			close(f.expectedResources)

			err := f.resourceCheckWorker(t.Context())()
			if gotErr := err != nil; gotErr != test.wantErr {
				t.Fatalf("resourceCheckWorker: %v want err %t", err, test.wantErr)
			}
		})
	}
}

type fakeFetcher struct {
	tile []byte
}

func (f *fakeFetcher) ReadCheckpoint(_ context.Context) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeFetcher) ReadTile(_ context.Context, _, _ uint64, _ uint8) ([]byte, error) {
	return f.tile, nil
}

func (f *fakeFetcher) ReadEntryBundle(_ context.Context, _ uint64, _ uint8) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func makeTile(n int) []byte {
	r := make([]byte, 0, n*sha256.Size)
	for i := range n {
		h := sha256.Sum256(fmt.Appendf(nil, "%50d", i))
		r = append(r, h[:]...)
	}
	return r
}

// fakeLog is a log with a valid signed checkpoint and entry bundles, but whose tiles all fail to fetch or mismatch.
type fakeLog struct {
	cp      []byte
	bundles map[uint64][]byte
	tile    func() ([]byte, error)
}

func newFakeLog(t *testing.T, size uint64, v *note.Verifier) *fakeLog {
	t.Helper()
	skey, vkey, err := note.GenerateKey(nil, "fsck-test")
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	s, err := note.NewSigner(skey)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	if *v, err = note.NewVerifier(vkey); err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	cp, err := note.Sign(&note.Note{Text: string((&log.Checkpoint{Origin: "fsck-test", Size: size, Hash: make([]byte, sha256.Size)}).Marshal())}, s)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	l := &fakeLog{cp: cp, bundles: make(map[uint64][]byte)}
	for i := range size {
		h := sha256.Sum256(fmt.Appendf(nil, "leaf %d", i))
		l.bundles[i/layout.EntryBundleWidth] = append(l.bundles[i/layout.EntryBundleWidth], h[:]...)
	}
	return l
}

func (l *fakeLog) ReadCheckpoint(_ context.Context) ([]byte, error) { return l.cp, nil }

func (l *fakeLog) ReadTile(_ context.Context, _, _ uint64, _ uint8) ([]byte, error) { return l.tile() }

func (l *fakeLog) ReadEntryBundle(_ context.Context, i uint64, _ uint8) ([]byte, error) {
	return l.bundles[i], nil
}

// splitHashes is a bundle hasher for fakeLog's bundles, which are just concatenated leaf hashes.
func splitHashes(b []byte) ([][]byte, error) {
	var r [][]byte
	for ; len(b) >= sha256.Size; b = b[sha256.Size:] {
		r = append(r, b[:sha256.Size])
	}
	return r, nil
}

// TestCheckResourceFailure checks that Check returns an error, rather than deadlocking, when resource checks fail
// while there are still many more resources to be queued.
func TestCheckResourceFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		n    uint
		tile func() ([]byte, error)
	}{
		{name: "fetch error", n: 1, tile: func() ([]byte, error) { return nil, errors.New("boom") }},
		{name: "mismatch", n: 1, tile: func() ([]byte, error) { return makeTile(256), nil }},
		{name: "all workers fail", n: 4, tile: func() ([]byte, error) { return nil, errors.New("boom") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var v note.Verifier
			// Many more full tiles than the work queue can hold.
			l := newFakeLog(t, 256*32, &v)
			l.tile = test.tile
			f := New("fsck-test", v, l, splitHashes, Opts{N: test.n})

			done := make(chan error, 1)
			go func() { done <- f.Check(t.Context()) }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("Check succeeded, want error")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Check deadlocked after resource check failure")
			}
		})
	}
}

// TestCheckCancelled checks that Check returns promptly when the context is cancelled while it is blocked queueing work.
func TestCheckCancelled(t *testing.T) {
	var v note.Verifier
	l := newFakeLog(t, 256*32, &v)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	l.tile = func() ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f := New("fsck-test", v, l, splitHashes, Opts{N: 1})

	done := make(chan error, 1)
	go func() { done <- f.Check(ctx) }()
	time.AfterFunc(100*time.Millisecond, cancel)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Check succeeded, want error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Check did not return after context cancellation")
	}
}
