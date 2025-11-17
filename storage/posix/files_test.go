// Copyright 2025 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package posix

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/fsck"
	"golang.org/x/mod/sumdb/note"
)

func TestGarbageCollect(t *testing.T) {
	ctx := t.Context()
	batchSize := uint64(60000)
	integrateEvery := uint64(31343)

	s := &Storage{
		cfg: Config{
			HTTPClient: http.DefaultClient,
			Path:       t.TempDir(),
		},
	}
	sk, vk := mustGenerateKeys(t)

	opts := tessera.NewAppendOptions().
		WithCheckpointInterval(1200*time.Millisecond).
		WithBatching(uint(batchSize), 100*time.Millisecond).
		// Disable GC so we can manually invoke below.
		WithGarbageCollectionInterval(time.Duration(0)).
		WithCheckpointSigner(sk)
	logStorage := &logResourceStorage{
		s:           s,
		entriesPath: opts.EntriesPath(),
	}
	appender, lr, err := s.newAppender(ctx, logStorage, opts)
	if err != nil {
		t.Fatalf("Appender: %v", err)
	}
	if err := appender.publishCheckpoint(ctx, 0, 0); err != nil {
		t.Fatalf("publishCheckpoint: %v", err)
	}

	// Build a reasonably-sized tree with a bunch of partial resouces present, and wait for
	// it to be published.
	treeSize := uint64(256 * 384)

	a := tessera.NewPublicationAwaiter(ctx, lr.ReadCheckpoint, 100*time.Millisecond)

	// grow and garbage collect the tree several times to check continued correct operation over lifetime of the log
	for size := uint64(0); size < treeSize; {
		t.Logf("Adding entries from %d", size)
		for range batchSize {
			f := appender.Add(ctx, tessera.NewEntry(fmt.Appendf(nil, "entry %d", size)))
			if size%integrateEvery == 0 {
				t.Logf("Awaiting entry  %d", size)
				if _, _, err := a.Await(ctx, f); err != nil {
					t.Fatalf("Await: %v", err)
				}
			}
			size++
		}
		t.Logf("Awaiting tree at size  %d", size)
		if _, _, err := a.Await(ctx, func() (tessera.Index, error) { return tessera.Index{Index: size - 1}, nil }); err != nil {
			t.Fatalf("Await final tree: %v", err)
		}

		t.Logf("Running GC at size  %d", size)
		if err := s.garbageCollect(ctx, size, 1000); err != nil {
			t.Fatalf("garbageCollect: %v", err)
		}

		// Compare any remaining partial resources to the list of places
		// we'd expect them to be, given the tree size.
		wantPartialPrefixes := make(map[string]struct{})
		for _, p := range expectedPartialPrefixes(size) {
			wantPartialPrefixes[p] = struct{}{}
		}
		allPartialDirs, err := findAllPartialDirs(t, s.cfg.Path)
		if err != nil {
			t.Fatalf("findAllPartials: %v", err)
		}
		for _, k := range allPartialDirs {
			if _, ok := wantPartialPrefixes[k]; !ok {
				t.Errorf("Found unwanted partial: %s", k)
			}
		}
	}

	// And finally, for good measure, assert that all the resources implied by the log's checkpoint
	// are present.
	f := fsck.New(vk.Name(), vk, lr, defaultMerkleLeafHasher, fsck.Opts{N: 1})
	if err := f.Check(ctx); err != nil {
		t.Fatalf("FSCK failed: %v", err)
	}
}

func TestPublishTree(t *testing.T) {
	for _, test := range []struct {
		name              string
		publishInterval   time.Duration
		republishInterval time.Duration
		attempts          []time.Duration
		growTree          bool
		wantUpdates       int
	}{
		{
			name:            "publish: works ok",
			publishInterval: 100 * time.Millisecond,
			attempts:        []time.Duration{1 * time.Second},
			growTree:        true,
			wantUpdates:     1,
		}, {
			name:            "publish: too soon, skip update",
			publishInterval: 10 * time.Second,
			growTree:        true,
			attempts:        []time.Duration{100 * time.Millisecond},
			wantUpdates:     0,
		}, {
			name:            "publish: too soon, skip update, but recovers",
			publishInterval: 2 * time.Second,
			growTree:        true,
			attempts:        []time.Duration{100 * time.Millisecond, 2 * time.Second},
			wantUpdates:     1,
		}, {
			name:            "publish: many attempts, eventually one succeeds",
			publishInterval: 1 * time.Second,
			growTree:        true,
			attempts:        []time.Duration{300 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond},
			wantUpdates:     1,
		}, {
			name:              "republish: works ok",
			publishInterval:   minCheckpointInterval,
			republishInterval: 100 * time.Millisecond,
			attempts:          []time.Duration{1 * time.Second},
			wantUpdates:       1,
		}, {
			name:              "republish: too soon, skip update",
			publishInterval:   minCheckpointInterval,
			republishInterval: 10 * time.Second,
			attempts:          []time.Duration{100 * time.Millisecond},
			wantUpdates:       0,
		}, {
			name:              "republish: too soon, skip update, but recovers",
			publishInterval:   minCheckpointInterval,
			republishInterval: 2 * time.Second,
			attempts:          []time.Duration{100 * time.Millisecond, 2 * time.Second},
			wantUpdates:       1,
		}, {
			name:              "republish: many attempts, eventually one succeeds",
			publishInterval:   minCheckpointInterval,
			republishInterval: 1 * time.Second,
			attempts:          []time.Duration{300 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond},
			wantUpdates:       1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			s := &Storage{
				cfg: Config{
					HTTPClient: http.DefaultClient,
					Path:       t.TempDir(),
				},
			}
			sk, _ := mustGenerateKeys(t)
			opts := tessera.NewAppendOptions().
				WithCheckpointInterval(10*time.Minute). // Prevent tessera from publishing checkpoints on our behalf
				WithBatching(1, minCheckpointInterval).
				WithCheckpointSigner(sk)

			logStorage := &logResourceStorage{
				s:           s,
				entriesPath: opts.EntriesPath(),
			}
			appender, lr, err := s.newAppender(ctx, logStorage, opts)
			if err != nil {
				t.Fatalf("Appender: %v", err)
			}

			// Add time as an extension line on the checkpoint so we can easily tell when it's been updated.
			appender.newCP = func(_ context.Context, size uint64, hash []byte) ([]byte, error) {
				return fmt.Appendf(nil, "origin\n%d\n%x\n%d\n,", size, hash, time.Now().Unix()), nil
			}

			if err := appender.publishCheckpoint(ctx, test.publishInterval, test.republishInterval); err != nil {
				t.Fatalf("publishTree: %v", err)
			}

			updatesSeen := 0
			cpOld, err := lr.ReadCheckpoint(ctx)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("ReadCheckpoint: %v", err)
			}

			if test.growTree {
				// Fake the tree growing here - we don't want Tessera creating a new checkpoint for us, as we'll do that
				// manually below.
				if err := appender.s.writeTreeState(ctx, 1, []byte("root)")); err != nil {
					t.Fatalf("writeTreeState: %v", err)
				}
			}

			for _, d := range test.attempts {
				time.Sleep(d)
				if err := appender.publishCheckpoint(ctx, test.publishInterval, test.republishInterval); err != nil {
					t.Fatalf("publishTree: %v", err)
				}
				cpNew, err := lr.ReadCheckpoint(ctx)
				if err != nil {
					t.Fatalf("ReadCheckpoint: %v", err)
				}
				if !bytes.Equal(cpOld, cpNew) {
					updatesSeen++
					cpOld = cpNew
				}
			}
			if updatesSeen != test.wantUpdates {
				t.Fatalf("Saw %d updates, want %d", updatesSeen, test.wantUpdates)
			}
		})
	}
}

func findAllPartialDirs(t *testing.T, root string) ([]string, error) {
	t.Helper()
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}

	dirs := make([]string, 0)
	f := func(path string, d os.DirEntry, err error) error {
		if d.IsDir() && strings.Contains(d.Name(), ".p") {
			dirs = append(dirs, strings.TrimPrefix(path, root))
		}
		return nil
	}
	return dirs, filepath.WalkDir(root, f)
}

// expectedPartialPrefixes returns a slice containing resource prefixes where it's acceptable for a
// tree of the provided size to have partial resources.
//
// These are really just the right-hand tiles/entry bundle in the tree.
func expectedPartialPrefixes(size uint64) []string {
	r := []string{}
	for l, c := uint64(0), size; c > 0; l, c = l+1, c>>8 {
		idx, p := c/256, c%256
		if p != 0 {
			if l == 0 {
				r = append(r, layout.EntriesPath(idx, 0)+".p")
			}
			r = append(r, layout.TilePath(l, idx, 0)+".p")
		}
	}
	return r
}

func mustGenerateKeys(t *testing.T) (note.Signer, note.Verifier) {
	sk, vk, err := note.GenerateKey(nil, "testlog")
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	s, err := note.NewSigner(sk)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	v, err := note.NewVerifier(vk)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	return s, v
}

// defaultMerkleLeafHasher parses a C2SP tlog-tile bundle and returns the Merkle leaf hashes of each entry it contains.
func defaultMerkleLeafHasher(bundle []byte) ([][]byte, error) {
	eb := &api.EntryBundle{}
	if err := eb.UnmarshalText(bundle); err != nil {
		return nil, fmt.Errorf("unmarshal: %v", err)
	}
	r := make([][]byte, 0, len(eb.Entries))
	for _, e := range eb.Entries {
		h := rfc6962.DefaultHasher.HashLeaf(e)
		r = append(r, h[:])
	}
	return r, nil
}
