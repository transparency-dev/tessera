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
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"log/slog"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/spannertest"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/testonly"
)

type testLookup struct {
	entryHash    []byte
	wantNotFound bool
}

func TestAntispamStorage(t *testing.T) {
	for _, test := range []struct {
		name          string
		opts          AntispamOpts
		sharedClient  bool
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
		{
			name:         "roundtrip with shared client",
			opts:         AntispamOpts{SpannerTablePrefix: "Shared1_"},
			sharedClient: true,
			logEntries: [][]byte{
				[]byte("one"),
				[]byte("two"),
			},
			lookupEntries: []testLookup{
				{
					entryHash: testIDHash([]byte("one")),
				}, {
					entryHash: testIDHash([]byte("two")),
				}, {
					entryHash:    testIDHash([]byte("nowhere to be found")),
					wantNotFound: true,
				},
			},
		},
		{
			name: "roundtrip with table prefix",
			opts: AntispamOpts{SpannerTablePrefix: "Tenant1_"},
			logEntries: [][]byte{
				[]byte("one"),
				[]byte("two"),
			},
			lookupEntries: []testLookup{
				{
					entryHash: testIDHash([]byte("one")),
				}, {
					entryHash: testIDHash([]byte("two")),
				}, {
					entryHash:    testIDHash([]byte("nowhere to be found")),
					wantNotFound: true,
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			closeDB := newSpannerDB(t)
			defer closeDB()
			if test.sharedClient {
				c, err := spanner.NewClient(t.Context(), testSpannerDB)
				if err != nil {
					t.Fatalf("spanner.NewClient: %v", err)
				}
				// As documented on AntispamOpts.SpannerClient, the caller retains
				// ownership of the client's lifecycle. Cleanup runs after this
				// function's defers have shut down the follower.
				t.Cleanup(c.Close)
				test.opts.SpannerClient = c
			}
			as, err := NewAntispam(t.Context(), testSpannerDB, test.opts)
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
				slog.InfoContext(context.Background(), "integrated entry", slog.Int("i", i), slog.String("identity", fmt.Sprintf("%x", entry.Identity())))
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
				slog.InfoContext(context.Background(), "Wait for follower to catch up with tree", slog.Uint64("pos", pos), slog.Uint64("sz", sz))
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

func TestAntispamSharedClientWrongDatabase(t *testing.T) {
	closeDB := newSpannerDB(t)
	defer closeDB()

	c, err := spanner.NewClient(t.Context(), "projects/p/instances/i/databases/other")
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}
	defer c.Close()

	if _, err := NewAntispam(t.Context(), testSpannerDB, AntispamOpts{SpannerClient: c}); err == nil {
		t.Error("NewAntispam accepted a SpannerClient connected to a different database, want error")
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
			closeDB := newSpannerDB(t)
			defer closeDB()
			as, err := NewAntispam(t.Context(), testSpannerDB, test.opts)
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
				slog.InfoContext(context.Background(), "integrated entry", slog.Int("i", i), slog.String("identity", fmt.Sprintf("%x", entry.Identity())))
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
				slog.InfoContext(context.Background(), "Wait for follower to catch up with tree", slog.Uint64("pos", pos), slog.Uint64("sz", sz))
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

func TestNewAntispamExistingSchema(t *testing.T) {
	ctx := t.Context()
	closeDB := newSpannerDB(t)
	defer closeDB()

	db, err := spanner.NewClient(ctx, testSpannerDB)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}
	defer db.Close()
	opts := AntispamOpts{SpannerTablePrefix: "Tenant1_", SpannerClient: db}

	if schemaInitialised(ctx, db, prefixTable(opts.SpannerTablePrefix)) {
		t.Fatal("schemaInitialised: got true on empty DB, want false")
	}
	if _, err := NewAntispam(ctx, testSpannerDB, opts); err != nil {
		t.Fatalf("NewAntispam on empty DB: %v", err)
	}
	if !schemaInitialised(ctx, db, prefixTable(opts.SpannerTablePrefix)) {
		t.Fatal("schemaInitialised: got false after NewAntispam, want true")
	}
	if _, err := db.Apply(ctx, []*spanner.Mutation{spanner.Update(opts.SpannerTablePrefix+"FollowCoord", []string{"id", "nextIdx"}, []any{0, 42})}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// spannertest rejects CREATE TABLE IF NOT EXISTS on an existing table, so a re-open that ran DDL
	// would fail below; guard against the emulator changing and this passing vacuously.
	if err := createAndPrepareTables(ctx, testSpannerDB, db, []string{"CREATE TABLE IF NOT EXISTS Tenant1_IDSeq (h BYTES(32) NOT NULL, idx INT64 NOT NULL) PRIMARY KEY (h)"}, nil); err == nil {
		t.Skip("spannertest now honours CREATE TABLE IF NOT EXISTS, so this test can no longer tell whether NewAntispam applied DDL")
	}

	// Re-opening must apply no DDL (see above) and leave existing state alone.
	as, err := NewAntispam(ctx, testSpannerDB, opts)
	if err != nil {
		t.Fatalf("NewAntispam on existing schema: %v", err)
	}
	f := as.Follower(testBundleHasher)
	if got, err := f.EntriesProcessed(ctx); err != nil || got != 42 {
		t.Fatalf("EntriesProcessed: got %d, %v, want 42, nil", got, err)
	}
}

func TestSchemaInitialised(t *testing.T) {
	for _, test := range []struct {
		name string
		// prep modifies a DB in which NewAntispam has created the unprefixed schema.
		prep func(ctx context.Context, t *testing.T, db *spanner.Client)
		// table defaults to unprefixed.
		table func(string) string
		want  bool
	}{
		{
			name: "initialised",
			want: true,
		}, {
			name:  "not initialised: no tables with this prefix",
			table: prefixTable("Other_"),
			want:  false,
		}, {
			name: "missing seed row",
			prep: func(ctx context.Context, t *testing.T, db *spanner.Client) {
				if _, err := db.Apply(ctx, []*spanner.Mutation{spanner.Delete("FollowCoord", spanner.Key{0})}); err != nil {
					t.Fatalf("Apply: %v", err)
				}
			},
			want: false,
		}, {
			name: "missing unseeded table",
			prep: func(ctx context.Context, t *testing.T, db *spanner.Client) {
				applyDDL(t, "DROP TABLE IDSeq")
			},
			want: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			closeDB := newSpannerDB(t)
			defer closeDB()
			db, err := spanner.NewClient(ctx, testSpannerDB)
			if err != nil {
				t.Fatalf("spanner.NewClient: %v", err)
			}
			defer db.Close()
			if _, err := NewAntispam(ctx, testSpannerDB, AntispamOpts{SpannerClient: db}); err != nil {
				t.Fatalf("NewAntispam: %v", err)
			}
			if test.prep != nil {
				test.prep(ctx, t, db)
			}
			if test.table == nil {
				test.table = prefixTable("")
			}
			if got := schemaInitialised(ctx, db, test.table); got != test.want {
				t.Fatalf("schemaInitialised: got %t, want %t", got, test.want)
			}
		})
	}
}

// testSpannerDB is the database served by the spannertest emulator.
const testSpannerDB = "projects/p/instances/i/databases/d"

// prefixTable returns a func which prepends prefix to a table name.
func prefixTable(prefix string) func(string) string {
	return func(table string) string {
		return prefix + table
	}
}

// applyDDL applies DDL directly to testSpannerDB.
func applyDDL(t *testing.T, statements ...string) {
	t.Helper()
	adminClient, err := database.NewDatabaseAdminClient(t.Context())
	if err != nil {
		t.Fatalf("NewDatabaseAdminClient: %v", err)
	}
	defer func() {
		if err := adminClient.Close(); err != nil {
			t.Logf("adminClient.Close: %v", err)
		}
	}()
	op, err := adminClient.UpdateDatabaseDdl(t.Context(), &adminpb.UpdateDatabaseDdlRequest{
		Database:   testSpannerDB,
		Statements: statements,
	})
	if err != nil {
		t.Fatalf("UpdateDatabaseDdl(%q): %v", statements, err)
	}
	if err := op.Wait(t.Context()); err != nil {
		t.Fatalf("UpdateDatabaseDdl(%q): %v", statements, err)
	}
}

func newSpannerDB(t *testing.T) func() {
	t.Helper()
	srv, err := spannertest.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("Failed to set up test spanner: %v", err)
	}
	if err := os.Setenv("SPANNER_EMULATOR_HOST", srv.Addr); err != nil {
		t.Fatalf("Setenv: %v", err)
	}
	return srv.Close
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
