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

package badger

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/testonly"
	"k8s.io/klog/v2"
)

type testLookup struct {
	entryHash    []byte
	wantNotFound bool
}

func TestAntispamStorage(t *testing.T) {

	for _, test := range []struct {
		name          string
		opts          AntispamOpts
		logEntries    [][]byte
		lookupEntries []testLookup
	}{
		{
			name: "roundtrip",
			logEntries: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
			lookupEntries: []testLookup{
				{
					entryHash: testIDHash([]byte("one")),
				}, {
					entryHash: testIDHash([]byte("two")),
				}, {
					entryHash: testIDHash([]byte("three")),
				}, {
					entryHash:    testIDHash([]byte("nowhere to be found")),
					wantNotFound: true,
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			as, err := NewAntispam(t.Context(), t.TempDir(), test.opts)
			if err != nil {
				t.Fatalf("NewAntispam: %v", err)
			}

			fl, shutdown := testonly.NewTestLog(t, tessera.NewAppendOptions().WithCheckpointInterval(time.Second))
			defer func() {
				if err := shutdown(t.Context()); err != nil {
					t.Logf("shutdown: %v", err)
				}
			}()

			f := as.Follower(testBundleHasher)

			go f.Follow(t.Context(), fl.LogReader)

			entryIndex := make(map[string]uint64)
			a := tessera.NewPublicationAwaiter(t.Context(), fl.LogReader.ReadCheckpoint, 100*time.Millisecond)
			for i, e := range test.logEntries {
				entry := tessera.NewEntry(e)
				f := fl.Appender.Add(t.Context(), entry)
				idx, _, err := a.Await(t.Context(), f)
				if err != nil {
					t.Fatalf("Await(%d): %v", i, err)
				}
				klog.Infof("%d == %x", i, entry.Identity())
				entryIndex[string(testIDHash(e))] = idx.Index
			}

			for {
				time.Sleep(time.Second)
				pos, err := f.EntriesProcessed(t.Context())
				if err != nil {
					t.Logf("EntriesProcessed: %v", err)
					continue
				}
				sz, err := fl.LogReader.IntegratedSize(t.Context())
				if err != nil {
					t.Logf("IntegratedSize: %v", err)
					continue
				}
				klog.Infof("Wait for follower (%d) to catch up with tree (%d)", pos, sz)
				if pos >= sz {
					break
				}
			}

			for _, e := range test.lookupEntries {
				gotIndex, err := as.index(t.Context(), e.entryHash)
				if err != nil {
					t.Errorf("error looking up hash %x: %v", e.entryHash, err)
				}
				wantIndex := entryIndex[string(e.entryHash)]
				if gotIndex == nil {
					if !e.wantNotFound {
						t.Errorf("no index for hash %x, but expected index %d", e.entryHash, wantIndex)
					}
					continue
				}
				if *gotIndex != wantIndex {
					t.Errorf("got index %d, want %d from looking up hash %x", gotIndex, wantIndex, e.entryHash)
				}
			}
		})
	}
}

func TestAntispamPushbackRecovers(t *testing.T) {
	for _, test := range []struct {
		name       string
		opts       AntispamOpts
		logEntries [][]byte
	}{
		{
			name: "pushback",
			opts: AntispamOpts{
				PushbackThreshold: 1,
			},
			logEntries: [][]byte{
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			as, err := NewAntispam(t.Context(), t.TempDir(), test.opts)
			if err != nil {
				t.Fatalf("NewAntispam: %v", err)
			}

			fl, shutdown := testonly.NewTestLog(t, tessera.NewAppendOptions().WithCheckpointInterval(time.Second))
			defer func() {
				if err := shutdown(t.Context()); err != nil {
					t.Logf("shutdown: %v", err)
				}
			}()

			f := as.Follower(testBundleHasher)

			entryIndex := make(map[string]uint64)
			a := tessera.NewPublicationAwaiter(t.Context(), fl.LogReader.ReadCheckpoint, 100*time.Millisecond)
			for i, e := range test.logEntries {
				entry := tessera.NewEntry(e)
				f := fl.Appender.Add(t.Context(), entry)
				idx, _, err := a.Await(t.Context(), f)
				if err != nil {
					t.Fatalf("Await(%d): %v", i, err)
				}
				klog.Infof("%d == %x", i, entry.Identity())
				entryIndex[string(testIDHash(e))] = idx.Index
			}

			// Wait for entries te be integrated before we start the follower, so we know we'll hit the pushback condition
			go f.Follow(t.Context(), fl.LogReader)

			for {
				time.Sleep(time.Second)
				pos, err := f.EntriesProcessed(t.Context())
				if err != nil {
					t.Logf("EntriesProcessed: %v", err)
					continue
				}
				sz, err := fl.LogReader.IntegratedSize(t.Context())
				if err != nil {
					t.Logf("IntegratedSize: %v", err)
					continue
				}
				klog.Infof("Wait for follower (%d) to catch up with tree (%d)", pos, sz)
				if pos >= sz {
					break
				}
			}

			// Ensure that the follower gets itself _out_ of pushback mode once it's caught up.
			// We'll give the follower some time to do its thing and notice.
			// It runs onces a second, so this should be plenty of time.
			for i := range 5 {
				time.Sleep(time.Second)
				if !as.pushBack.Load() {
					t.Logf("Antispam caught up and out of pushback in %ds", i)
					return
				}
			}
			t.Fatalf("pushBack remains true after 5 seconds despite being caught up!")
		})
	}
}

func testIDHash(d []byte) []byte {
	r := sha256.Sum256(d)
	return r[:]
}

func testBundleHasher(b []byte) ([][]byte, error) {
	bun := &api.EntryBundle{}
	err := bun.UnmarshalText(b)
	if err != nil {
		return nil, err
	}
	r := make([][]byte, len(bun.Entries))
	for i, e := range bun.Entries {
		r[i] = testIDHash(e)
	}
	return r, err
}
