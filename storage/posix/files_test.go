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
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
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
	if _, err := appender.publishCheckpoint(ctx, 0, 0); err != nil {
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
		if err := s.garbageCollect(ctx, size, 1000, appender.logStorage.entriesPath); err != nil {
			t.Fatalf("garbageCollect: %v", err)
		}

		// Compare any remaining partial resources to the list of places
		// we'd expect them to be, given the tree size.
		wantPartialPrefixes := make(map[string]struct{})
		for _, p := range expectedPartialPrefixes(size, appender.logStorage.entriesPath) {
			wantPartialPrefixes[p] = struct{}{}
		}
		allPartialDirs, err := findAllPartialDirs(t, s.cfg.Path)
		if err != nil {
			t.Fatalf("findAllPartials: %v", err)
		}
		for k := range allPartialDirs {
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

func TestGarbageCollectOption(t *testing.T) {
	batchSize := uint64(60000)
	integrateEvery := uint64(31343)
	garbageCollectionInterval := 100 * time.Millisecond

	for _, test := range []struct {
		name                          string
		withCTLayout                  bool
		withGarbageCollectionInterval time.Duration
	}{
		{
			name:                          "on",
			withGarbageCollectionInterval: garbageCollectionInterval,
			withCTLayout:                  false,
		},
		{
			name:                          "on-ct",
			withGarbageCollectionInterval: garbageCollectionInterval,
			withCTLayout:                  true,
		},
		{
			name:                          "off",
			withGarbageCollectionInterval: time.Duration(0),
			withCTLayout:                  false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()

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
				WithGarbageCollectionInterval(test.withGarbageCollectionInterval).
				WithCheckpointSigner(sk)

			if test.withCTLayout {
				opts.WithCTLayout()
			}

			logStorage := &logResourceStorage{
				s:           s,
				entriesPath: opts.EntriesPath(),
			}
			appender, lr, err := s.newAppender(ctx, logStorage, opts)
			if err != nil {
				t.Fatalf("Appender: %v", err)
			}
			if _, err := appender.publishCheckpoint(ctx, 0, 0); err != nil {
				t.Fatalf("publishCheckpoint: %v", err)
			}

			// Build a reasonably-sized tree with a bunch of partial resouces present, and wait for
			// it to be published.
			treeSize := uint64(256 * 384)

			a := tessera.NewPublicationAwaiter(ctx, lr.ReadCheckpoint, 100*time.Millisecond)
			wantPartialPrefixes := make(map[string]struct{})

			// Grow the tree several times to check continued correct operation over lifetime of the log.
			// Let garbage collection happen in the background.
			for size := uint64(0); size < treeSize; {
				t.Logf("Adding entries from %d", size)
				for range batchSize {
					f := appender.Add(ctx, tessera.NewEntry(fmt.Appendf(nil, "entry %d", size)))
					if size%integrateEvery == 0 {
						t.Logf("Awaiting entry  %d", size)
						if _, _, err := a.Await(ctx, f); err != nil {
							t.Fatalf("Await: %v", err)
						}
						// If garbage collection is off, we want partial tiles and bundles to stick around.
						if test.withGarbageCollectionInterval == time.Duration(0) {
							for _, p := range expectedPartialPrefixes(size, appender.logStorage.entriesPath) {
								wantPartialPrefixes[p] = struct{}{}
							}
						}
					}
					size++
				}
				t.Logf("Awaiting tree at size  %d", size)
				if _, _, err := a.Await(ctx, func() (tessera.Index, error) { return tessera.Index{Index: size - 1}, nil }); err != nil {
					t.Fatalf("Await final tree: %v", err)
				}

				// Leave a bit of time for Garbage Collection to run.
				time.Sleep(3 * garbageCollectionInterval)

				// Compare any remaining partial resources to the list of places
				// we'd expect them to be, given the tree size.

				// Regardless of whether garbage collection is on, partial tiles corresponding to the last
				// checkpoint should alway be here.
				for _, p := range expectedPartialPrefixes(size, appender.logStorage.entriesPath) {
					wantPartialPrefixes[p] = struct{}{}
				}
				allPartialDirs, err := findAllPartialDirs(t, s.cfg.Path)
				if err != nil {
					t.Fatalf("findAllPartials: %v", err)
				}
				// If gargabe collection is on, no partial tiles other than the ones we expect should be
				// present.
				for k := range allPartialDirs {
					if _, ok := wantPartialPrefixes[k]; !ok && test.withGarbageCollectionInterval > 0 {
						t.Errorf("Found unwanted partial: %s", k)
					}
					delete(wantPartialPrefixes, k)
				}
				for k := range wantPartialPrefixes {
					t.Errorf("Did not find expected partial: %s", k)
				}
			}

			// And finally, for good measure, assert that all the resources implied by the log's checkpoint
			// are present.
			f := fsck.New(vk.Name(), vk, lr, defaultMerkleLeafHasher, fsck.Opts{N: 1})
			if err := f.Check(ctx); err != nil {
				t.Fatalf("FSCK failed: %v", err)
			}
		})
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
			name:              "publish: no growth; republish: disabled",
			publishInterval:   100 * time.Millisecond,
			republishInterval: 0,
			growTree:          false,
			attempts:          []time.Duration{100 * time.Millisecond, 100 * time.Millisecond},
			wantUpdates:       0,
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
			pubAt := time.Now() // Good approximation of the checkpoint's future modTime.
			appender, lr, err := s.newAppender(ctx, logStorage, opts)
			if err != nil {
				t.Fatalf("Appender: %v", err)
			}

			// Add time as an extension line on the checkpoint so we can easily tell when it's been updated.
			appender.newCP = func(_ context.Context, size uint64, hash []byte) ([]byte, error) {
				return fmt.Appendf(nil, "origin\n%d\n%x\n%d\n,", size, hash, time.Now().Unix()), nil
			}

			if _, err := appender.publishCheckpoint(ctx, test.publishInterval, test.republishInterval); err != nil {
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
				nextPubAt, err := appender.publishCheckpoint(ctx, test.publishInterval, test.republishInterval)
				notAfter := time.Now().Add(test.publishInterval)
				if err != nil {
					t.Fatalf("publishTree: %v", err)
				}
				if nextPubAt.Before(pubAt.Add(test.publishInterval)) {
					t.Errorf("nextPubAt = %v, want larger than %v", nextPubAt, pubAt.Add(test.publishInterval))
				}
				if nextPubAt.After(notAfter) {
					t.Errorf("nextPubAt = %v, want smaller than %v", nextPubAt, notAfter)
				}
				cpNew, err := lr.ReadCheckpoint(ctx)
				if err != nil {
					t.Fatalf("ReadCheckpoint: %v", err)
				}
				if !bytes.Equal(cpOld, cpNew) {
					updatesSeen++
					pubAt = time.Now()
					cpOld = cpNew
				}
			}
			if updatesSeen != test.wantUpdates {
				t.Fatalf("Saw %d updates, want %d", updatesSeen, test.wantUpdates)
			}
		})
	}
}

func findAllPartialDirs(t *testing.T, root string) (map[string]struct{}, error) {
	t.Helper()
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}

	dirs := make(map[string]struct{})
	f := func(path string, d os.DirEntry, err error) error {
		if d.IsDir() && strings.Contains(d.Name(), ".p") {
			dirs[strings.TrimPrefix(path, root)] = struct{}{}
		}
		return nil
	}
	return dirs, filepath.WalkDir(root, f)
}

// expectedPartialPrefixes returns a slice containing resource prefixes where it's acceptable for a
// tree of the provided size to have partial resources.
//
// These are really just the right-hand tiles/entry bundle in the tree.
func expectedPartialPrefixes(size uint64, entriesPath func(uint64, uint8) string) []string {
	r := []string{}
	for l, c := uint64(0), size; c > 0; l, c = l+1, c>>8 {
		idx, p := c/256, c%256
		if p != 0 {
			if l == 0 {
				r = append(r, entriesPath(idx, 0)+".p")
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

func TestWriteTileNoSymlinks(t *testing.T) {
	ctx := t.Context()

	// Helper to check if a file path is a symlink.
	isSymlink := func(p string) bool {
		fi, err := os.Lstat(p)
		if err != nil {
			return false
		}
		return fi.Mode()&os.ModeSymlink != 0
	}

	tempDir := t.TempDir()
	s := &Storage{
		cfg: Config{
			HTTPClient: http.DefaultClient,
			Path:       tempDir,
		},
	}
	lrs := &logResourceStorage{
		s:           s,
		entriesPath: tessera.NewAppendOptions().EntriesPath(),
	}

	// 1. Write a partial tile
	if err := lrs.writeTile(ctx, 0, 0, 1, []byte("partial")); err != nil {
		t.Fatalf("writeTile (partial): %v", err)
	}

	// 2. Write the full tile
	if err := lrs.writeTile(ctx, 0, 0, 0, []byte("full")); err != nil {
		t.Fatalf("writeTile (full): %v", err)
	}

	partialPath := filepath.Join(tempDir, layout.TilePath(0, 0, 1))
	if isSymlink(partialPath) {
		t.Fatalf("partial tile was converted to a symlink; expected it to remain a regular file")
	}

	// Try reading the file contents.
	content, err := os.ReadFile(partialPath)
	if err != nil {
		t.Fatalf("failed to read partial tile: %v", err)
	}
	if string(content) != "partial" {
		t.Fatalf("expected partial tile content to remain 'partial', got %q", string(content))
	}
}

func TestCreateTemp_ErrorReturns(t *testing.T) {
	tempDir := t.TempDir()
	nonExistentDir := filepath.Join(tempDir, "nonexistent-subdir")
	prefix := filepath.Join(nonExistentDir, "prefix")

	done := make(chan struct{})
	var err error
	var name string

	go func() {
		name, err = createTemp(prefix, []byte("test data"))
		close(done)
	}()

	select {
	case <-done:
		if err == nil {
			t.Errorf("expected error opening file in non-existent directory, got nil (file: %s)", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out: createTemp is likely in an infinite loop")
	}
}

func TestFlockLockingCorrectness(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	// Create the state directory where locks are placed
	if err := os.MkdirAll(filepath.Join(dir, ".state"), 0755); err != nil {
		t.Fatal(err)
	}

	s1 := &Storage{
		cfg: Config{
			HTTPClient: http.DefaultClient,
			Path:       dir,
		},
	}
	s2 := &Storage{
		cfg: Config{
			HTTPClient: http.DefaultClient,
			Path:       dir,
		},
	}

	lockName := "test.lock"
	outputPath := filepath.Join(dir, "output.txt")

	startedA := make(chan struct{})
	proceedA := make(chan struct{})
	tryingB := make(chan struct{})
	doneB := make(chan struct{})

	// Worker A
	go func() {
		unlock1, err := s1.lockFile(ctx, lockName)
		if err != nil {
			t.Errorf("s1.lockFile failed: %v", err)
			return
		}
		close(startedA)
		<-proceedA

		// Append "A"
		f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			t.Errorf("Worker A failed to open output file: %v", err)
			_ = unlock1()
			return
		}
		if _, err := f.WriteString("A\n"); err != nil {
			t.Errorf("Worker A failed to write output: %v", err)
		}
		_ = f.Close()
		_ = unlock1()
	}()

	// Worker B
	go func() {
		<-startedA
		close(tryingB)
		unlock2, err := s2.lockFile(ctx, lockName)
		if err != nil {
			t.Errorf("s2.lockFile failed: %v", err)
			return
		}
		defer func() {
			_ = unlock2()
			close(doneB)
		}()

		// Append "B"
		f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			t.Errorf("Worker B failed to open output file: %v", err)
			return
		}
		if _, err := f.WriteString("B\n"); err != nil {
			t.Errorf("Worker B failed to write output: %v", err)
		}
		_ = f.Close()
	}()

	// Coordinator
	select {
	case <-startedA:
	case <-time.After(2 * time.Second):
		t.Fatal("Worker A failed to start and lock")
	}

	select {
	case <-tryingB:
	case <-time.After(2 * time.Second):
		t.Fatal("Worker B failed to start")
	}

	// Sleep briefly to allow Worker B to reach the lockFile call.
	// Under BSD flock, Worker B MUST block and should not finish yet.
	time.Sleep(100 * time.Millisecond)

	select {
	case <-doneB:
		t.Fatal("Worker B finished prematurely; lock did not block same-process concurrent access")
	default:
		// Expected: Worker B is blocked waiting for Worker A to release the lock.
	}

	// Let Worker A proceed and unlock
	close(proceedA)

	// Worker B should now unblock, acquire the lock, write "B", and finish.
	select {
	case <-doneB:
	case <-time.After(2 * time.Second):
		t.Fatal("Worker B timed out waiting to acquire lock")
	}

	// Verify the final write sequence was correct: A then B
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	expected := "A\nB\n"
	if string(content) != expected {
		t.Fatalf("expected output file to contain %q, got %q (serialization failed)", expected, string(content))
	}
}

func TestOverwrite_CleanupOnRenameFailure(t *testing.T) {
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "target_dir")
	if err := os.Mkdir(targetDir, 0755); err != nil {
		t.Fatal(err)
	}
	err := overwrite(targetDir, []byte("some data"))
	if err == nil {
		t.Fatal("expected overwrite to fail when target is a directory")
	}
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != "target_dir" {
			t.Errorf("found unexpected file/dir after failure: %s", entry.Name())
		}
	}
}

func TestMirrorWriter_IntegrateBundles(t *testing.T) {
	baseBundle := &api.EntryBundle{
		Entries: [][]byte{[]byte("entry1"), []byte("entry2")},
	}

	bundlesFunc := func(b *api.EntryBundle) func(yield func(*api.EntryBundle, error) bool) {
		return func(yield func(*api.EntryBundle, error) bool) {
			yield(b, nil)
		}
	}

	for _, tc := range []struct {
		name         string
		bundleIdx    uint64
		bundle       *api.EntryBundle
		expectedSize uint64
		wantErr      bool
		errSubstring string
	}{
		{
			name:         "integrate bundle with common prefix (extend)",
			bundleIdx:    0,
			bundle:       &api.EntryBundle{Entries: append(append([][]byte{}, baseBundle.Entries...), []byte("entry3"))},
			expectedSize: 3,
		},
		{
			name:         "re-integrate base bundle (idempotency)",
			bundleIdx:    0,
			bundle:       baseBundle,
			expectedSize: 2,
		},
		{
			name:         "re-integrate modified basebundle (conflict error)",
			bundleIdx:    0,
			bundle:       &api.EntryBundle{Entries: append(append([][]byte{}, baseBundle.Entries[:len(baseBundle.Entries)-1]...), []byte("entry2_modified"))},
			wantErr:      true,
			errSubstring: "different contents",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Storage{
				cfg: Config{
					HTTPClient: http.DefaultClient,
					Path:       t.TempDir(),
				},
			}
			opts := tessera.NewMirrorOptions()
			ctx := t.Context()
			mw, _, err := s.MirrorWriter(ctx, opts)
			if err != nil {
				t.Fatalf("MirrorWriter: %v", err)
			}

			// Set up the storage with the base bundle.
			if size, _, err := mw.IntegrateBundles(ctx, 0, bundlesFunc(baseBundle)); err != nil {
				t.Fatalf("Failed to set up test, IntegrateBundles: %v", err)
			} else if size != uint64(len(baseBundle.Entries)) {
				t.Fatalf("Failed to set up test, expected size %d, got %d", len(baseBundle.Entries), size)
			}

			// Now run the test case.
			size, _, err := mw.IntegrateBundles(ctx, tc.bundleIdx, bundlesFunc(tc.bundle))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.errSubstring)
				}
				if !strings.Contains(err.Error(), tc.errSubstring) {
					t.Fatalf("expected error containing %q, got %v", tc.errSubstring, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != tc.expectedSize {
				t.Fatalf("got size %d, want %d", size, tc.expectedSize)
			}
		})
	}
}

func TestMaxLevel(t *testing.T) {
	for _, tc := range []struct {
		size uint64
		want int
	}{
		{0, 0},
		{1, 0},
		{256, 1},
		{257, 1},
		{32769, 1},
		{65535, 1},
		{65536, 2},
	} {
		t.Run(fmt.Sprintf("size_%d", tc.size), func(t *testing.T) {
			if got := maxLevel(tc.size); got != tc.want {
				t.Fatalf("maxLevel(%d): got %d, want %d", tc.size, got, tc.want)
			}
		})
	}
}

func TestMirrorWriter_UpdateCheckpointGeometry(t *testing.T) {
	ctx := t.Context()
	s := &Storage{
		cfg: Config{
			Path: t.TempDir(),
		},
	}
	opts := tessera.NewMirrorOptions()
	mw, lr, err := s.MirrorWriter(ctx, opts)
	if err != nil {
		t.Fatalf("MirrorWriter: %v", err)
	}

	// Create 600 entries to span multiple tile levels.
	entries := make([][]byte, 600)
	for i := range 600 {
		entries[i] = fmt.Appendf(nil, "entry %d", i)
	}

	bundles := []*api.EntryBundle{
		{Entries: entries[0:256]},
		{Entries: entries[256:512]},
		{Entries: entries[512:600]},
	}
	bundlesIter := func(yield func(*api.EntryBundle, error) bool) {
		for _, b := range bundles {
			if !yield(b, nil) {
				return
			}
		}
	}

	// Integrate to size 600.
	// Tiles layout should then be:
	// Level 1: [2]
	// Level 0: [256] [256] [88]
	// Entries: [256] [256] [88]
	size, _, err := mw.IntegrateBundles(ctx, 0, bundlesIter)
	if err != nil {
		t.Fatalf("IntegrateBundles: %v", err)
	}
	if size != 600 {
		t.Fatalf("expected integrated size 600, got %d", size)
	}

	for _, test := range []struct {
		size        uint64
		wantRHSizes []int
	}{
		{0, []int{}},
		{1, []int{1}},
		{129, []int{129}},
		{256, []int{256, 1}},
		{300, []int{44, 2}},
		{600, []int{88, 2}},
	} {
		t.Run(fmt.Sprintf("size_%d", test.size), func(t *testing.T) {
			h := make([]byte, 32)
			cpStr1 := fmt.Sprintf("origin\n%d\n%s\nsig\n", test.size, base64.StdEncoding.EncodeToString(h))
			if err := mw.UpdateCheckpoint(ctx, func(old []byte) ([]byte, error) {
				return []byte(cpStr1), nil
			}); err != nil {
				t.Fatalf("UpdateCheckpoint (size %d): %v", test.size, err)
			}

			if test.size == 0 {
				return
			}

			idx := (test.size - 1) / layout.EntryBundleWidth
			p := uint8(test.wantRHSizes[0])
			eb := mustReadEntryBundle(t, lr, idx, p)
			if l := len(eb.Entries); l != test.wantRHSizes[0] {
				t.Fatalf("expected %d entries in partial bundle %d.%d, got %d", test.wantRHSizes[0], idx, p, l)
			}
			if got, want := eb.Entries, bundles[idx].Entries[:test.wantRHSizes[0]]; !slices.EqualFunc(got, want, bytes.Equal) {
				t.Errorf("entrybundle doesn't match:\ngot %v\nwant %v", got, want)
			}
			// Verify the existence/correctness of the partial tiles for the given size.
			for level, p := range test.wantRHSizes {
				tile := mustReadTile(t, lr, uint64(level), idx, uint8(p))
				if len(tile.Nodes) != p {
					t.Errorf("expected %d nodes in partial tile %d/%d.%d, got %d", p, level, idx, p, len(tile.Nodes))
				}
				idx >>= layout.TileHeight
			}
		})
	}
}

func mustReadTile(t *testing.T, lr tessera.LogReader, level, idx uint64, p uint8) *api.HashTile {
	t.Helper()
	d, err := lr.ReadTile(t.Context(), level, idx, p)
	if err != nil {
		t.Fatalf("failed to read tile: %v", err)
	}
	tile := &api.HashTile{}
	if err := tile.UnmarshalText(d); err != nil {
		t.Fatalf("failed to unmarshal tile: %v", err)
	}
	return tile
}

func mustReadEntryBundle(t *testing.T, lr tessera.LogReader, idx uint64, p uint8) *api.EntryBundle {
	t.Helper()
	d, err := lr.ReadEntryBundle(t.Context(), idx, p)
	if err != nil {
		t.Fatalf("failed to read entry bundle: %v", err)
	}
	eb := &api.EntryBundle{}
	if err := eb.UnmarshalText(d); err != nil {
		t.Fatalf("failed to unmarshal entry bundle: %v", err)
	}
	return eb
}
