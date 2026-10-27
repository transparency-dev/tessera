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

package gcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"iter"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spannertest"
	gcs "cloud.google.com/go/storage"
	"github.com/google/go-cmp/cmp"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/fsck"
	storage "github.com/transparency-dev/tessera/storage/internal"
	"golang.org/x/mod/sumdb/note"
)

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

func newSpannerDB(t *testing.T) (*spanner.Client, func()) {
	t.Helper()
	return newSpannerDBWithPrefix(t, "")
}

// prefixTable returns a function which returns the provided table name with prefix prepended.
func prefixTable(prefix string) func(string) string {
	return func(table string) string {
		return prefix + table
	}
}

func newSpannerDBWithPrefix(t *testing.T, tablePrefix string) (*spanner.Client, func()) {
	t.Helper()
	srv, err := spannertest.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("Failed to set up test spanner: %v", err)
	}
	if err := os.Setenv("SPANNER_EMULATOR_HOST", srv.Addr); err != nil {
		t.Fatalf("Setenv: %v", err)
	}

	id := "projects/p/instances/i/databases/d"
	if err := initDB(t.Context(), id, prefixTable(tablePrefix)); err != nil {
		t.Fatalf("initDB: %v", err)
	}

	c, err := spanner.NewClient(t.Context(), id)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c, srv.Close

}

func TestSpannerSequencerAssignEntries(t *testing.T) {
	ctx := t.Context()
	db, close := newSpannerDB(t)
	defer close()

	seq, err := newSpannerCoordinator(ctx, db, prefixTable(""), 1000)
	if err != nil {
		t.Fatalf("newSpannerCoordinator: %v", err)
	}

	want := uint64(0)
	for chunks := range 10 {
		entries := []*tessera.Entry{}
		for i := range 10 + chunks {
			entries = append(entries, tessera.NewEntry(fmt.Appendf(nil, "item %d/%d", chunks, i)))
		}
		if err := seq.assignEntries(ctx, entries); err != nil {
			t.Fatalf("assignEntries: %v", err)
		}
		for i, e := range entries {
			if got := *e.Index(); got != want {
				t.Errorf("Chunk %d entry %d got seq %d, want %d", chunks, i, got, want)
			}
			want++
		}
	}
}

func TestSpannerTablePrefix(t *testing.T) {
	ctx := t.Context()
	const prefix = "Tenant1_"
	db, close := newSpannerDBWithPrefix(t, prefix)
	defer close()

	seq, err := newSpannerCoordinator(ctx, db, prefixTable(prefix), 1000)
	if err != nil {
		t.Fatalf("newSpannerCoordinator: %v", err)
	}

	entries := []*tessera.Entry{}
	for i := range 10 {
		entries = append(entries, tessera.NewEntry(fmt.Appendf(nil, "item %d", i)))
	}
	if err := seq.assignEntries(ctx, entries); err != nil {
		t.Fatalf("assignEntries: %v", err)
	}
	next, err := seq.nextIndex(ctx)
	if err != nil {
		t.Fatalf("nextIndex: %v", err)
	}
	if want := uint64(len(entries)); next != want {
		t.Errorf("nextIndex: got %d, want %d", next, want)
	}

	// The state should live in the prefixed tables, and the unprefixed tables should not exist.
	if _, err := db.Single().ReadRow(ctx, prefix+"SeqCoord", spanner.Key{0}, []string{"next"}); err != nil {
		t.Errorf("failed to read from prefixed SeqCoord table: %v", err)
	}
	if _, err := db.Single().ReadRow(ctx, "SeqCoord", spanner.Key{0}, []string{"next"}); err == nil {
		t.Error("read from unprefixed SeqCoord table succeeded, want error as table should not exist")
	}
}

func TestSpannerTablePrefixValidation(t *testing.T) {
	ctx := t.Context()
	for _, test := range []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{name: "empty is valid", prefix: ""},
		{name: "simple prefix is valid", prefix: "Tenant1_"},
		{name: "letters digits underscores are valid", prefix: "log1_x"},
		{name: "64 chars is valid", prefix: strings.Repeat("a", 64)},
		{name: "65 chars is invalid", prefix: strings.Repeat("a", 65), wantErr: true},
		{name: "leading digit is invalid", prefix: "1log", wantErr: true},
		{name: "leading underscore is invalid", prefix: "_log", wantErr: true},
		{name: "space is invalid", prefix: "a b", wantErr: true},
		{name: "hyphen is invalid", prefix: "a-b", wantErr: true},
		{name: "DDL metacharacters are invalid", prefix: "Evil (id INT64) PRIMARY KEY (id); DROP TABLE Tessera;--", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(ctx, Config{
				Bucket:             "bucket",
				Spanner:            "projects/p/instances/i/databases/d",
				SpannerTablePrefix: test.prefix,
			})
			if gotErr := err != nil; gotErr != test.wantErr {
				t.Errorf("New with prefix %q: got err %v, want err %t", test.prefix, err, test.wantErr)
			}
		})
	}
}

func TestSpannerSequencerPushback(t *testing.T) {
	ctx := t.Context()

	for _, test := range []struct {
		name           string
		threshold      uint64
		initialEntries int
		wantPushback   bool
	}{
		{
			name:           "no pushback: num < threshold",
			threshold:      10,
			initialEntries: 5,
		},
		{
			name:           "no pushback: num = threshold",
			threshold:      10,
			initialEntries: 10,
		},
		{
			name:           "pushback: initial > threshold",
			threshold:      10,
			initialEntries: 15,
			wantPushback:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, close := newSpannerDB(t)
			defer close()

			seq, err := newSpannerCoordinator(ctx, db, prefixTable(""), test.threshold)
			if err != nil {
				t.Fatalf("newSpannerCoordinator: %v", err)
			}
			// Set up the test scenario with the configured number of initial outstanding entries
			entries := []*tessera.Entry{}
			for i := range test.initialEntries {
				entries = append(entries, tessera.NewEntry(fmt.Appendf(nil, "initial item %d", i)))
			}
			if err := seq.assignEntries(ctx, entries); err != nil {
				t.Fatalf("initial assignEntries: %v", err)
			}

			// Now perform the test with a single additional entry to check for pushback
			entries = []*tessera.Entry{tessera.NewEntry([]byte("additional"))}
			err = seq.assignEntries(ctx, entries)
			if gotPushback := errors.Is(err, tessera.ErrPushback); gotPushback != test.wantPushback {
				t.Fatalf("assignEntries: got pushback %t (%v), want pushback: %t", gotPushback, err, test.wantPushback)
			} else if !gotPushback && err != nil {
				t.Fatalf("assignEntries: %v", err)
			}
		})
	}
}

func TestSpannerSequencerRoundTrip(t *testing.T) {
	ctx := t.Context()
	db, close := newSpannerDB(t)
	defer close()

	s, err := newSpannerCoordinator(ctx, db, prefixTable(""), 1000)
	if err != nil {
		t.Fatalf("newSpannerCoordinator: %v", err)
	}

	seq := 0
	wantEntries := []storage.SequencedEntry{}
	for chunks := range 10 {
		entries := []*tessera.Entry{}
		for range 10 + chunks {
			e := tessera.NewEntry(fmt.Appendf(nil, "item %d", seq))
			entries = append(entries, e)
			wantEntries = append(wantEntries, storage.SequencedEntry{
				BundleData: e.MarshalBundleData(uint64(seq)),
				LeafHash:   e.LeafHash(),
			})
			seq++
		}
		if err := s.assignEntries(ctx, entries); err != nil {
			t.Fatalf("assignEntries: %v", err)
		}
	}

	seenIdx := uint64(0)
	f := func(_ context.Context, fromSeq uint64, entries []storage.SequencedEntry) ([]byte, error) {
		if fromSeq != seenIdx {
			return nil, fmt.Errorf("f called with fromSeq %d, want %d", fromSeq, seenIdx)
		}
		for i, e := range entries {

			if got, want := e, wantEntries[i]; !reflect.DeepEqual(got, want) {
				return nil, fmt.Errorf("entry %d+%d != %d", fromSeq, i, seenIdx)
			}
			seenIdx++
		}
		return fmt.Appendf(nil, "root<%d>", seenIdx), nil
	}

	more, err := s.consumeEntries(ctx, 7, f, false)
	if err != nil {
		t.Errorf("consumeEntries: %v", err)
	}
	if !more {
		t.Errorf("more: false, expected true")
	}
}

func TestCheckDataCompatibility(t *testing.T) {
	ctx := t.Context()
	db, close := newSpannerDB(t)
	defer close()

	s, err := newSpannerCoordinator(ctx, db, prefixTable(""), 1000)
	if err != nil {
		t.Fatalf("newSpannerCoordinator: %v", err)
	}

	for _, test := range []struct {
		desc    string
		dbV     int64
		wantErr bool
	}{
		{
			desc: "versions match",
			dbV:  SchemaCompatibilityVersion,
		},
		{
			desc:    "data < library",
			dbV:     SchemaCompatibilityVersion + 1,
			wantErr: true,
		},
		{
			desc:    "data > library",
			dbV:     SchemaCompatibilityVersion - 1,
			wantErr: true,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			if _, err := s.dbPool.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("Tessera", []string{"id", "compatibilityVersion"}, []any{0, test.dbV})}); err != nil {
				t.Fatalf("Failed for force schema version to %d: %v", test.dbV, err)
			}

			err := s.checkDataCompatibility(ctx)
			if gotErr := err != nil; test.wantErr != gotErr {
				t.Fatalf("checkDataCompatibility: %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func makeTile(t *testing.T, size uint64) *api.HashTile {
	t.Helper()
	r := &api.HashTile{Nodes: make([][]byte, size)}
	for i := uint64(0); i < size; i++ {
		h := sha256.Sum256(fmt.Appendf(nil, "%d", i))
		r.Nodes[i] = h[:]
	}
	return r
}

func TestTileRoundtrip(t *testing.T) {
	ctx := t.Context()
	m := newMemObjStore()
	s := &logResourceStore{
		objStore: m,
	}

	for _, test := range []struct {
		name     string
		level    uint64
		index    uint64
		logSize  uint64
		tileSize uint64
	}{
		{
			name:     "ok",
			level:    0,
			index:    3 * layout.TileWidth,
			logSize:  3*layout.TileWidth + 20,
			tileSize: 20,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			wantTile := makeTile(t, test.tileSize)
			tRaw, err := wantTile.MarshalText()
			if err != nil {
				t.Fatalf("Failed to marshal tile: %v", err)
			}
			if err := s.setTile(ctx, test.level, test.index, layout.PartialTileSize(test.level, test.index, test.logSize), tRaw); err != nil {
				t.Fatalf("setTile: %v", err)
			}

			expPath := layout.TilePath(test.level, test.index, layout.PartialTileSize(test.level, test.index, test.logSize))
			if _, _, err = m.getObject(t.Context(), expPath); err != nil {
				t.Fatalf("want tile at %v but got: %v", expPath, err)
			}

			got, err := s.getTiles(ctx, []storage.TileID{{Level: test.level, Index: test.index}}, test.logSize)
			if err != nil {
				t.Fatalf("getTile: %v", err)
			}
			if !cmp.Equal(got[0], wantTile) {
				t.Fatal("roundtrip returned different data")
			}
		})
	}
}

// TestReadTileNotExist checks that LogReader.ReadTile falls back to the full
// tile if the requested partial tile is absent (e.g. because it was garbage
// collected after the tree grew), and returns os.ErrNotExist if neither exists.
func TestReadTileNotExist(t *testing.T) {
	ctx := t.Context()
	s := &logResourceStore{
		objStore: newMemObjStore(),
	}
	lr := &LogReader{lrs: *s}

	const level, index = 0, 1
	wantTile := makeTile(t, layout.TileWidth)
	tRaw, err := wantTile.MarshalText()
	if err != nil {
		t.Fatalf("Failed to marshal tile: %v", err)
	}
	// Only the full tile is stored.
	if err := s.setTile(ctx, level, index, 0, tRaw); err != nil {
		t.Fatalf("setTile: %v", err)
	}

	for _, test := range []struct {
		name    string
		index   uint64
		p       uint8
		wantErr error
	}{
		{
			name:  "full tile present",
			index: index,
		},
		{
			name:  "partial tile absent, full tile present",
			index: index,
			p:     20,
		},
		{
			name:    "full tile absent",
			index:   index + 1,
			wantErr: os.ErrNotExist,
		},
		{
			name:    "partial and full tile absent",
			index:   index + 1,
			p:       20,
			wantErr: os.ErrNotExist,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := lr.ReadTile(ctx, level, test.index, test.p)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("ReadTile(%d, %d, %d) = %v, want %v", level, test.index, test.p, err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadTile(%d, %d, %d): %v", level, test.index, test.p, err)
			}
			if !bytes.Equal(got, tRaw) {
				t.Fatalf("ReadTile(%d, %d, %d) returned different data", level, test.index, test.p)
			}
		})
	}
}

func makeBundle(t *testing.T, idx uint64, size int) []byte {
	t.Helper()
	r := &bytes.Buffer{}
	if size == 0 {
		size = layout.EntryBundleWidth
	}
	for i := range size {
		e := tessera.NewEntry(fmt.Appendf(nil, "%d:%d", idx, i))
		if _, err := r.Write(e.MarshalBundleData(uint64(i))); err != nil {
			t.Fatalf("MarshalBundleEntry: %v", err)
		}
	}
	return r.Bytes()
}

func TestBundleRoundtrip(t *testing.T) {
	ctx := t.Context()
	m := newMemObjStore()
	s := &logResourceStore{
		objStore:    m,
		entriesPath: layout.EntriesPath,
	}

	for _, test := range []struct {
		name       string
		index      uint64
		logSize    uint64
		bundleSize int
	}{
		{
			name:       "ok",
			index:      3 * layout.EntryBundleWidth,
			logSize:    3*layout.EntryBundleWidth + 20,
			bundleSize: 20,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			wantBundle := makeBundle(t, test.index, test.bundleSize)
			if err := s.setEntryBundle(ctx, test.index, uint8(test.bundleSize), wantBundle); err != nil {
				t.Fatalf("setEntryBundle: %v", err)
			}

			expPath := layout.EntriesPath(test.index, layout.PartialTileSize(0, test.index, test.logSize))
			if _, _, err := m.getObject(t.Context(), expPath); err != nil {
				t.Fatalf("want tile at %v but got: %v", expPath, err)
			}

			got, err := s.getEntryBundle(ctx, test.index, layout.PartialTileSize(0, test.index, test.logSize))
			if err != nil {
				t.Fatalf("getEntryBundle: %v", err)
			}
			if !cmp.Equal(got, wantBundle) {
				t.Fatal("roundtrip returned different data")
			}
		})
	}
}

func TestSpannerSequencerAssignEntriesBatchSplitting(t *testing.T) {
	ctx := t.Context()

	db, closeDB := newSpannerDB(t)
	defer closeDB()

	// We want to create enough entries to exceed this limit.
	// Each entry will be ~10KB, so 200 entries will be ~2MiB.
	const (
		numEntries = 200
		entrySize  = 10 * 1024
	)

	seq, err := newSpannerCoordinator(ctx, db, prefixTable(""), uint64(numEntries+1))
	if err != nil {
		t.Fatalf("newSpannerCoordinator: %v", err)
	}

	seq.seqTableMaxBatchByteSize = 1 << 20 // 1 MiB, set low for testing due to grpc message size limits in the Spanner emulator.

	wantEntries := make(map[string]struct{})
	var entries []*tessera.Entry
	baseData := make([]byte, entrySize)
	for i := range numEntries {
		e := tessera.NewEntry(fmt.Appendf(baseData, "entry-%d", i))
		entries = append(entries, e)
		wantEntries[string(e.LeafHash())] = struct{}{}
	}

	if err := seq.assignEntries(ctx, entries); err != nil {
		t.Fatalf("assignEntries: %v", err)
	}

	t.Log("assignEntries done")

	// Verify that multiple rows were created in the Seq table.
	var rowCount int
	if err := db.Single().Read(ctx, "Seq", spanner.AllKeys(), []string{"seq"}).Do(func(row *spanner.Row) error {
		rowCount++
		return nil
	}); err != nil {
		t.Fatalf("Failed to read from Seq table: %v", err)
	}

	t.Logf("rowCount is %d", rowCount)

	if rowCount <= 1 {
		t.Errorf("expected more than 1 row in Seq table, got %d", rowCount)
	}

	// Verify that all entries are present and contiguous by consuming them.
	seenEntries := 0
	f := func(_ context.Context, fromSeq uint64, batch []storage.SequencedEntry) ([]byte, error) {
		t.Logf("consuming from %d, saw %d entries", fromSeq, len(batch))
		if fromSeq != uint64(seenEntries) {
			return nil, fmt.Errorf("f called with fromSeq %d, want %d", fromSeq, seenEntries)
		}
		seenEntries += len(batch)

		for _, e := range batch {
			lh := string(e.LeafHash)
			if _, ok := wantEntries[lh]; !ok {
				return nil, fmt.Errorf("saw unexpected entry with hash %x", e.LeafHash)
			}
			delete(wantEntries, lh)
		}

		return fmt.Appendf(nil, "root<%d>", seenEntries), nil
	}

	for seenEntries < numEntries {
		t.Logf("calling consumeEntries, seen: %d", seenEntries)
		more, err := seq.consumeEntries(ctx, 100, f, false)
		if err != nil {
			t.Fatalf("consumeEntries: %v", err)
		}
		if !more && seenEntries < numEntries {
			t.Fatalf("consumeEntries: more = false but only seen %d/%d entries", seenEntries, numEntries)
		}
	}

	if seenEntries != numEntries {
		t.Errorf("saw %d entries, want %d", seenEntries, numEntries)
	}
	if len(wantEntries) != 0 {
		t.Errorf("failed to consume %d entries", len(wantEntries))
	}
}

func TestPublishTree(t *testing.T) {
	ctx := t.Context()
	for _, test := range []struct {
		name              string
		publishInterval   time.Duration
		republishInterval time.Duration
		attempts          []time.Duration
		wantUpdates       int
	}{
		{
			name:              "works ok",
			publishInterval:   100 * time.Millisecond,
			republishInterval: 100 * time.Millisecond,
			attempts:          []time.Duration{1 * time.Second},
			wantUpdates:       1,
		}, {
			name:              "too soon, skip update",
			publishInterval:   10 * time.Second,
			republishInterval: 10 * time.Second,
			attempts:          []time.Duration{100 * time.Millisecond},
			wantUpdates:       0,
		}, {
			name:              "too soon, skip update, but recovers",
			publishInterval:   2 * time.Second,
			republishInterval: 2 * time.Second,
			attempts:          []time.Duration{100 * time.Millisecond, 2 * time.Second},
			wantUpdates:       1,
		}, {
			name:              "many attempts, eventually one succeeds",
			publishInterval:   1 * time.Second,
			republishInterval: 1 * time.Second,
			attempts:          []time.Duration{300 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond},
			wantUpdates:       1,
		}, {
			name:              "republish needed",
			publishInterval:   1 * time.Second,
			republishInterval: 2 * time.Second,
			attempts:          []time.Duration{1500 * time.Millisecond, 2500 * time.Millisecond},
			wantUpdates:       1,
		}, {
			name:              "publish: no growth; republish: disabled",
			publishInterval:   100 * time.Millisecond,
			republishInterval: 0,
			attempts:          []time.Duration{100 * time.Millisecond, 100 * time.Millisecond},
			wantUpdates:       0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, closeDB := newSpannerDB(t)
			defer closeDB()
			s, err := newSpannerCoordinator(ctx, db, prefixTable(""), 1000)
			if err != nil {
				t.Fatalf("newSpannerCoordinator: %v", err)
			}
			defer s.dbPool.Close()

			m := newMemObjStore()
			storage := &Appender{
				logStore: &logResourceStore{
					objStore:    m,
					entriesPath: layout.EntriesPath,
				},
				sequencer: s,
				newCP: func(_ context.Context, size uint64, hash []byte) ([]byte, error) {
					return fmt.Appendf(nil, "%d/%x,", size, hash), nil
				},
			}
			// Call init so we've got a zero-sized checkpoint to work with.
			if err := storage.init(ctx); err != nil {
				t.Fatalf("storage.init: %v", err)
			}

			pubAt := time.Now() // Good approximation of the checkpoint's future publishedAt.
			if _, err := s.publishCheckpoint(ctx, test.publishInterval, test.republishInterval, storage.updateCheckpoint); err != nil {
				t.Fatalf("publishTree: %v", err)
			}
			cpOld := []byte("bananas")
			if err := m.setObject(ctx, layout.CheckpointPath, cpOld, nil, "", ""); err != nil {
				t.Fatalf("setObject(bananas): %v", err)
			}
			updatesSeen := 0
			for _, d := range test.attempts {
				time.Sleep(d)
				nextPubAt, err := s.publishCheckpoint(ctx, test.publishInterval, test.republishInterval, storage.updateCheckpoint)
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
				cpNew, _, err := m.getObject(ctx, layout.CheckpointPath)
				if err != nil {
					t.Fatalf("getObject: %v", err)
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

func TestGarbageCollect(t *testing.T) {
	ctx := t.Context()
	batchSize := uint64(60000)
	integrateEvery := uint64(31234)

	db, closeDB := newSpannerDB(t)
	defer closeDB()

	s, err := newSpannerCoordinator(ctx, db, prefixTable(""), batchSize)
	if err != nil {
		t.Fatalf("newSpannerCoordinator: %v", err)
	}
	defer s.dbPool.Close()

	sk, vk := mustGenerateKeys(t)

	m := newMemObjStore()
	storage := &Storage{}

	opts := tessera.NewAppendOptions().
		WithCheckpointInterval(1200*time.Millisecond).
		WithBatching(uint(batchSize), 100*time.Millisecond).
		// Disable GC so we can manually invoke below.
		WithGarbageCollectionInterval(time.Duration(0)).
		WithCheckpointSigner(sk)
	appender, lr, err := storage.newAppender(ctx, m, s, opts)
	if err != nil {
		t.Fatalf("newAppender: %v", err)
	}
	if err := appender.updateCheckpoint(ctx, 0, []byte("")); err != nil {
		t.Fatalf("publishCheckpoint: %v", err)
	}

	// Build a reasonably-sized tree with a bunch of partial resources present, and wait for
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
		if err := s.garbageCollect(ctx, size, 1000, m.deleteObjectsWithPrefix, appender.logStore.entriesPath); err != nil {
			t.Fatalf("garbageCollect: %v", err)
		}

		// Compare any remaining partial resources to the list of places
		// we'd expect them to be, given the tree size.
		wantPartialPrefixes := make(map[string]struct{})
		for _, p := range expectedPartialPrefixes(size, appender.logStore.entriesPath) {
			wantPartialPrefixes[p] = struct{}{}
		}
		for _, k := range m.keys() {
			if strings.Contains(k, ".p/") {
				p := strings.SplitAfter(k, ".p/")[0]
				if _, ok := wantPartialPrefixes[p]; !ok {
					t.Errorf("Found unwanted partial: %s", k)
				}
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
	integrateEvery := uint64(31234)
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
			db, closeDB := newSpannerDB(t)
			defer closeDB()

			s, err := newSpannerCoordinator(ctx, db, prefixTable(""), batchSize)
			if err != nil {
				t.Fatalf("newSpannerCoordinator: %v", err)
			}
			defer s.dbPool.Close()

			sk, vk := mustGenerateKeys(t)

			m := newMemObjStore()
			storage := &Storage{}

			opts := tessera.NewAppendOptions().
				WithCheckpointInterval(1200*time.Millisecond).
				WithBatching(uint(batchSize), 100*time.Millisecond).
				// Disable GC so we can manually invoke below.
				WithGarbageCollectionInterval(test.withGarbageCollectionInterval).
				WithCheckpointSigner(sk)

			if test.withCTLayout {
				opts.WithCTLayout()
			}

			appender, lr, err := storage.newAppender(ctx, m, s, opts)
			if err != nil {
				t.Fatalf("newAppender: %v", err)
			}
			if err := appender.updateCheckpoint(ctx, 0, []byte("")); err != nil {
				t.Fatalf("publishCheckpoint: %v", err)
			}

			// Build a reasonably-sized tree with a bunch of partial resources present, and wait for
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
							for _, p := range expectedPartialPrefixes(size, appender.logStore.entriesPath) {
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
				// checkpoint should always be here.
				for _, p := range expectedPartialPrefixes(size, appender.logStore.entriesPath) {
					wantPartialPrefixes[p] = struct{}{}
				}
				allPartialDirs := make(map[string]struct{})
				for _, k := range m.keys() {
					if strings.Contains(k, ".p/") {
						allPartialDirs[strings.SplitAfter(k, ".p/")[0]] = struct{}{}
					}
				}
				// If garbage collection is on, no partial tiles other than the ones we expect should be
				// present.
				for p := range allPartialDirs {
					if _, ok := wantPartialPrefixes[p]; !ok && test.withGarbageCollectionInterval > 0 {
						t.Errorf("Found unwanted partial: %s", p)
					}
					delete(wantPartialPrefixes, p)
				}
				for p := range wantPartialPrefixes {
					t.Errorf("Did not find expected partial: %s", p)
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
				r = append(r, entriesPath(idx, 0)+".p/")
			}
			r = append(r, layout.TilePath(l, idx, 0)+".p/")
		}
	}
	return r
}

type memObjStore struct {
	sync.RWMutex
	mem map[string][]byte
}

func newMemObjStore() *memObjStore {
	return &memObjStore{
		mem: make(map[string][]byte),
	}
}

func (m *memObjStore) getObject(_ context.Context, obj string) ([]byte, *gcs.ReaderObjectAttrs, error) {
	m.RLock()
	defer m.RUnlock()

	d, ok := m.mem[obj]
	if !ok {
		return nil, nil, fmt.Errorf("obj %q not found: %w", obj, gcs.ErrObjectNotExist)
	}
	return d, &gcs.ReaderObjectAttrs{Generation: 1}, nil
}

// TODO(phboneff): add content type tests
func (m *memObjStore) setObject(_ context.Context, obj string, data []byte, cond *gcs.Conditions, _, _ string) error {
	m.Lock()
	defer m.Unlock()

	d, ok := m.mem[obj]
	if cond != nil {
		if ok && cond.DoesNotExist {
			if !bytes.Equal(d, data) {
				return errors.New("precondition failed and data not identical")
			}
			return nil
		}
	}
	m.mem[obj] = data
	return nil
}

func (m *memObjStore) keys() []string {
	m.RLock()
	defer m.RUnlock()
	r := make([]string, 0, len(m.mem))
	for k := range m.mem {
		r = append(r, k)
	}
	return r
}

func (m *memObjStore) deleteObjectsWithPrefix(_ context.Context, prefix string) error {
	m.Lock()
	defer m.Unlock()

	for k := range m.mem {
		if strings.HasPrefix(k, prefix) {
			log.Printf("DELETE: %s", k)
			delete(m.mem, k)
		}
	}
	return nil
}

// TestSetObjectBucketPrefixIdempotentRecovery exercises gcsStorage.setObject's
// precondition-failed recovery path with a non-empty BucketPrefix: the
// recovery getObject must read <prefix>/<name>, not <prefix>/<prefix>/<name>.
// setObject and getObject each apply bucketPrefix once, so setObject must
// hand getObject the unprefixed name. The fake server returns 412 on the
// write and serves identical bytes at the single-prefixed path, so setObject
// should return nil (idempotent success).
func TestSetObjectBucketPrefixIdempotentRecovery(t *testing.T) {
	const (
		bucket  = "test-bucket"
		prefix  = "some/prefix"
		objName = "tile/0/000"
	)
	data := []byte("tile-bytes")
	wantObj := prefix + "/" + objName

	// This fake pins the cloud.google.com/go/storage client's HTTP JSON-API
	// wire shape (multipart upload, 412 parsing, JSON media-read path). That
	// coupling is deliberate: the bug under test sits below the objStore
	// interface, so a real *gcs.Client is the only way to exercise it; the
	// cost is possible breakage on a client-library upgrade that reshapes
	// these requests.
	var mu sync.Mutex
	var gotReadObj string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/"):
			// Conditional write: object already exists, precondition fails.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = fmt.Fprint(w, `{"error":{"code":412,"message":"conditionNotMet"}}`)
		case r.Method == http.MethodGet:
			// JSON-API media read. Tolerate both path shapes the client
			// library has been observed to emit against an emulator endpoint:
			//   [/download]/storage/v1/b/<bucket>/o/<url-encoded object>?alt=media
			p := strings.TrimPrefix(r.URL.Path, "/download")
			got, _ := url.PathUnescape(strings.TrimPrefix(p, "/storage/v1/b/"+bucket+"/o/"))
			mu.Lock()
			gotReadObj = got
			mu.Unlock()
			if got != wantObj {
				http.Error(w, "object not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("X-Goog-Generation", "1")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			http.Error(w, "unexpected", http.StatusNotImplemented)
		}
	}))
	defer srv.Close()

	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(srv.URL, "http://"))
	ctx := context.Background()
	c, err := gcs.NewClient(ctx, gcs.WithJSONReads())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("Close: %v", err)
		}
	}()

	s := &gcsStorage{gcsClient: c, bucket: bucket, bucketPrefix: prefix}
	err = s.setObject(ctx, objName, data, &gcs.Conditions{DoesNotExist: true}, "application/octet-stream", "")
	mu.Lock()
	got := gotReadObj
	mu.Unlock()
	if err != nil {
		t.Fatalf("setObject: want idempotent success (nil), got %v (recovery read was for %q)", err, got)
	}
	if got != wantObj {
		t.Fatalf("recovery getObject read %q, want %q (double-prefix bug)", got, wantObj)
	}
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

// newTestMirrorWriter returns a mirrorWriter backed by an in-memory object store and a test Spanner
// instance, along with a LogReader wired up in the same way as Storage.MirrorWriter does.
func newTestMirrorWriter(t *testing.T, opts *tessera.MirrorOptions) (*mirrorWriter, tessera.LogReader, *memObjStore) {
	t.Helper()
	db, closeDB := newSpannerDB(t)
	t.Cleanup(closeDB)
	t.Cleanup(db.Close)

	seq, err := newSpannerCoordinator(t.Context(), db, prefixTable(""), 0)
	if err != nil {
		t.Fatalf("newSpannerCoordinator: %v", err)
	}
	m := newMemObjStore()
	mw := &mirrorWriter{
		seq: seq,
		logStore: &logResourceStore{
			objStore:    m,
			entriesPath: opts.EntriesPath(),
		},
	}
	lr := &LogReader{
		lrs:            *mw.logStore,
		integratedSize: mw.IntegratedSize,
		nextIndex:      mw.IntegratedSize,
	}
	return mw, lr, m
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
			errSubstring: "precondition failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := tessera.NewMirrorOptions()
			ctx := t.Context()
			mw, _, _ := newTestMirrorWriter(t, opts)

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

func TestMirrorWriter_UpdateCheckpoint(t *testing.T) {
	cp := func(size uint64) []byte {
		return fmt.Appendf(nil, "origin\n%d\n%s\nsig\n", size, base64.StdEncoding.EncodeToString(make([]byte, 32)))
	}
	errUpdate := errors.New("update failed")

	for _, test := range []struct {
		name string
		// publishedCP, if non-nil, is stored as the published checkpoint before calling UpdateCheckpoint.
		publishedCP []byte
		// fnErr, if non-nil, is returned by the update function instead of a new checkpoint.
		fnErr error
	}{
		{
			name: "no published checkpoint",
		},
		{
			name:        "published checkpoint",
			publishedCP: cp(1),
		},
		{
			name:  "update function error, no published checkpoint",
			fnErr: errUpdate,
		},
		{
			name:        "update function error, published checkpoint",
			publishedCP: cp(1),
			fnErr:       errUpdate,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			mw, lr, m := newTestMirrorWriter(t, tessera.NewMirrorOptions())

			// Integrate a couple of entries so that the new checkpoint doesn't commit to more entries than the tree contains.
			if _, _, err := mw.IntegrateBundles(ctx, 0, generateBundles(t, 2)); err != nil {
				t.Fatalf("IntegrateBundles: %v", err)
			}
			if test.publishedCP != nil {
				if err := m.setObject(ctx, layout.CheckpointPath, test.publishedCP, nil, "", ""); err != nil {
					t.Fatalf("setObject: %v", err)
				}
			}

			newCP := cp(2)
			var gotOldCP []byte
			err := mw.UpdateCheckpoint(ctx, func(oldCP []byte) ([]byte, error) {
				gotOldCP = oldCP
				if test.fnErr != nil {
					return nil, test.fnErr
				}
				return newCP, nil
			})
			switch {
			case test.fnErr == nil && err != nil:
				t.Fatalf("UpdateCheckpoint: %v", err)
			case test.fnErr != nil && !errors.Is(err, test.fnErr):
				t.Fatalf("UpdateCheckpoint: got error %v, want %v", err, test.fnErr)
			}

			// The witness relies on a nil (rather than merely empty) checkpoint to recognise that none has
			// been published yet, so check for that explicitly.
			if test.publishedCP == nil && gotOldCP != nil {
				t.Errorf("fn called with %q, want nil", gotOldCP)
			} else if !bytes.Equal(gotOldCP, test.publishedCP) {
				t.Errorf("fn called with %q, want %q", gotOldCP, test.publishedCP)
			}

			// If fn returned an error, the published checkpoint must be unchanged.
			wantCP := newCP
			if test.fnErr != nil {
				wantCP = test.publishedCP
			}
			gotCP, err := lr.ReadCheckpoint(ctx)
			switch {
			case wantCP == nil && !errors.Is(err, os.ErrNotExist):
				t.Errorf("ReadCheckpoint: got (%q, %v), want os.ErrNotExist", gotCP, err)
			case wantCP != nil && err != nil:
				t.Errorf("ReadCheckpoint: %v", err)
			case !bytes.Equal(gotCP, wantCP):
				t.Errorf("published checkpoint: got %q, want %q", gotCP, wantCP)
			}
		})
	}
}

func TestMirrorWriter_UpdateCheckpointGeometry(t *testing.T) {
	// TODO: Unskip once mirrorWriter.UpdateCheckpoint ensures the partial tiles and entry bundles
	// implied by the new checkpoint exist.
	t.Skip("mirrorWriter.UpdateCheckpoint does not yet ensure checkpoint geometry")

	ctx := t.Context()
	opts := tessera.NewMirrorOptions()
	mw, lr, m := newTestMirrorWriter(t, opts)

	// Create 66000 entries to span multiple tile levels and multiple tiles at level 1.
	const treeSize = 66000
	size, _, err := mw.IntegrateBundles(ctx, 0, generateBundles(t, treeSize))
	if err != nil {
		t.Fatalf("IntegrateBundles: %v", err)
	}
	if size != treeSize {
		t.Fatalf("expected integrated size %d, got %d", treeSize, size)
	}

	for _, test := range []struct {
		size uint64
		// wantRHSizes[l] is the expected node count for the rightmost tile at level l:
		//   - 1..255: partial tile with that many nodes (read via .p/N)
		//   - 256:    full tile (read via p=0, asserting 256 nodes)
		//   - 0:      no tile exists at this coordinate for this checkpoint (asserts full tile object is absent)
		wantRHSizes []int
	}{
		{0, []int{}},
		{1, []int{1}},
		{129, []int{129}},
		{256, []int{256, 1}},
		{300, []int{44, 1}},
		{600, []int{88, 2}},
		{65536, []int{256, 256, 1}},
		{65537, []int{1, 0, 1}},
		{66000, []int{208, 1, 1}},
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

			// Verify the existence/correctness of the partial tiles for the given size,
			// and ensure that full tiles whose range extends beyond the tree are not created.
			for level, wantNodes := range test.wantRHSizes {
				fullTilePath := layout.TilePath(uint64(level), idx, 0)
				treeP := layout.PartialTileSize(uint64(level), idx, treeSize)

				if wantNodes == 0 {
					// Tile does not exist at this checkpoint size.
					// A full tile _may_ legitimately exist in the tree if it's large enough.
					// However, since our treeSize is 66000 we know that one cannot be present for
					// these test cases, so we'll assert that it doesn't exist in order to verify that we
					// haven't incorrectly created one.
					if d, _, err := m.getObject(ctx, fullTilePath); err == nil {
						t.Fatalf("Level %d tile %d: full tile %s should not exist for size %d (found %d bytes)", level, idx, fullTilePath, test.size, len(d))
					} else if !errors.Is(err, gcs.ErrObjectNotExist) {
						t.Fatalf("Failed to get %s: %v", fullTilePath, err)
					}
				} else {
					tile := mustReadTile(t, lr, uint64(level), idx, uint8(wantNodes))
					if len(tile.Nodes) != wantNodes {
						t.Errorf("Expected %d nodes in partial tile %d/%d.%d, got %d", wantNodes, level, idx, uint8(wantNodes), len(tile.Nodes))
					}
					// If the integrated tree does not have a full tile at this position (treeP > 0),
					// no full tile object should have been written.
					if treeP > 0 {
						if d, _, err := m.getObject(ctx, fullTilePath); err == nil {
							t.Errorf("Level %d tile %d: full tile %s should not exist for size %d (found %d bytes)", level, idx, fullTilePath, test.size, len(d))
						} else if !errors.Is(err, gcs.ErrObjectNotExist) {
							t.Fatalf("Failed to get %s: %v", fullTilePath, err)
						}
					}
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

func BenchmarkMarshalEntryBundle(b *testing.B) {
	for _, entrySize := range []int{32, 128, 1024} {
		b.Run(fmt.Sprintf("entrySize=%dB", entrySize), func(b *testing.B) {
			bundle := &api.EntryBundle{
				Entries: make([][]byte, layout.EntryBundleWidth),
			}
			for i := range bundle.Entries {
				bundle.Entries[i] = bytes.Repeat([]byte("a"), entrySize)
			}
			b.ReportAllocs()
			b.SetBytes(int64(layout.EntryBundleWidth * entrySize))
			b.ResetTimer()
			for b.Loop() {
				res, err := marshalEntryBundle(bundle)
				if err != nil {
					b.Fatal(err)
				}
				_ = res
			}
		})
	}
}

func generateBundles(t *testing.T, totalEntries int) iter.Seq2[*api.EntryBundle, error] {
	t.Helper()

	return func(yield func(*api.EntryBundle, error) bool) {
		remaining := totalEntries

		N := 0
		for remaining > 0 {
			count := min(remaining, layout.EntryBundleWidth)
			entries := make([][]byte, count)
			for i := range entries {
				entries[i] = fmt.Appendf(nil, "entry %d", N+i)
			}
			if !yield(&api.EntryBundle{Entries: entries}, nil) {
				return
			}
			remaining -= count
			N += count
		}
	}
}

// TestLease exercises the singleton-jobs lease.
func TestLease(t *testing.T) {
	ctx := t.Context()
	db, closeDB := newSpannerDB(t)
	defer closeDB()

	// Note: we can't re-run initDB here to check idempotency on an existing DB, because spannertest
	// ignores "IF NOT EXISTS" in DDL. The seed Insert's AlreadyExists is ignored by createAndPrepareTables.

	a, err := newSpannerCoordinator(ctx, db, prefixTable(""), 1000)
	if err != nil {
		t.Fatalf("newSpannerCoordinator: %v", err)
	}
	b, err := newSpannerCoordinator(ctx, db, prefixTable(""), 1000)
	if err != nil {
		t.Fatalf("newSpannerCoordinator: %v", err)
	}
	if a.holderID == b.holderID {
		t.Fatalf("holder IDs not unique: %q", a.holderID)
	}

	a.tryAcquireOrRenewLease(ctx)
	b.tryAcquireOrRenewLease(ctx)
	if !a.hasLease() || b.hasLease() {
		t.Fatalf("after first acquire: a=%v b=%v, want a=true b=false", a.hasLease(), b.hasLease())
	}

	// Renewal by the holder keeps the lease; the other still can't take it.
	a.tryAcquireOrRenewLease(ctx)
	b.tryAcquireOrRenewLease(ctx)
	if !a.hasLease() || b.hasLease() {
		t.Fatalf("after renew: a=%v b=%v, want a=true b=false", a.hasLease(), b.hasLease())
	}

	// Simulate a's lease expiring (e.g. a died): b should take over, and a should notice it lost it.
	if _, err := db.Apply(ctx, []*spanner.Mutation{spanner.Update("LeaseCoord", []string{"id", "holder", "expiresAt"}, []any{0, a.holderID, time.Now().Add(-time.Second)})}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	b.tryAcquireOrRenewLease(ctx)
	a.tryAcquireOrRenewLease(ctx)
	if a.hasLease() || !b.hasLease() {
		t.Fatalf("after expiry: a=%v b=%v, want a=false b=true", a.hasLease(), b.hasLease())
	}
}
