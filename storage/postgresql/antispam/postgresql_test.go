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

package postgresql

import (
	"context"
	"crypto/sha256"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/testonly"
	"k8s.io/klog/v2"
)

var (
	pgURI            = flag.String("pg_uri", "postgres://postgres:root@localhost:5432/test_tessera?sslmode=disable", "Connection string for a PostgreSQL database")
	isPGTestOptional = flag.Bool("is_pg_test_optional", true, "Boolean value to control whether the PostgreSQL test is optional")
)

func TestMain(m *testing.M) {
	klog.InitFlags(nil)
	os.Exit(m.Run())
}

func TestAntispam(t *testing.T) {
	ctx := t.Context()
	if canSkipPGTest(t, ctx) {
		t.Skip("PostgreSQL not available, skipping test")
	}
	mustDropTables(t, ctx)
	as, err := NewAntispam(ctx, *pgURI, AntispamOpts{})
	if err != nil {
		t.Fatal(err)
	}
	fl, shutdown := testonly.NewTestLog(t, tessera.NewAppendOptions().WithCheckpointInterval(time.Second))
	defer func() {
		if err := shutdown(t.Context()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()
	addFn := as.Decorator()(fl.Appender.Add)
	follower := as.Follower(testBundleHasher)
	go follower.Follow(ctx, fl.LogReader)

	pos, err := follower.EntriesProcessed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Error("expected initial position to be 0")
	}

	a := tessera.NewPublicationAwaiter(t.Context(), fl.LogReader.ReadCheckpoint, time.Second)
	var idx1 tessera.Index
	idxf1 := addFn(ctx, tessera.NewEntry([]byte("one")))
	if _, _, err := a.Await(t.Context(), idxf1); err != nil {
		t.Fatalf("Await(1): %v", err)
	}

	idxf2 := addFn(ctx, tessera.NewEntry([]byte("two")))
	if _, _, err := a.Await(t.Context(), idxf2); err != nil {
		t.Fatalf("Await(2): %v", err)
	}

	if idx1, err = idxf1(); err != nil {
		t.Fatal(err)
	}
	if _, err := idxf2(); err != nil {
		t.Fatal(err)
	}
	for {
		if idx, err := follower.EntriesProcessed(ctx); err != nil {
			t.Fatal(err)
		} else if idx == 2 {
			break
		}
	}
	dupIdx, err := addFn(ctx, tessera.NewEntry([]byte("one")))()
	if err != nil {
		t.Error(err)
	}
	if !dupIdx.IsDup {
		t.Error("expected dupe but it wasn't marked as such")
	}
	if dupIdx.Index != idx1.Index {
		t.Errorf("expected idx %d but got %d", idx1.Index, dupIdx.Index)
	}
}

func TestAntispamPushbackRecovers(t *testing.T) {
	ctx := t.Context()
	if canSkipPGTest(t, ctx) {
		t.Skip("PostgreSQL not available, skipping test")
	}
	mustDropTables(t, ctx)
	as, err := NewAntispam(ctx, *pgURI, AntispamOpts{
		PushbackThreshold: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	fl, shutdown := testonly.NewTestLog(t, tessera.NewAppendOptions().WithCheckpointInterval(time.Second))
	defer func() {
		if err := shutdown(t.Context()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()
	addFn := as.Decorator()(fl.Appender.Add)
	follower := as.Follower(testBundleHasher)
	a := tessera.NewPublicationAwaiter(t.Context(), fl.LogReader.ReadCheckpoint, time.Second)
	idxf1 := addFn(ctx, tessera.NewEntry([]byte("one")))
	if _, _, err := a.Await(t.Context(), idxf1); err != nil {
		t.Fatalf("Await(1): %v", err)
	}

	idxf2 := addFn(ctx, tessera.NewEntry([]byte("two")))
	if _, _, err := a.Await(t.Context(), idxf2); err != nil {
		t.Fatalf("Await(2): %v", err)
	}

	go follower.Follow(ctx, fl.LogReader)

	for {
		time.Sleep(time.Second)
		if idx, err := follower.EntriesProcessed(ctx); err != nil {
			t.Fatal(err)
		} else if idx == 2 {
			break
		}
	}

	// Ensure that the follower gets itself _out_ of pushback mode once it's caught up.
	for i := range 5 {
		time.Sleep(time.Second)
		if !as.pushBack.Load() {
			t.Logf("Antispam caught up and out of pushback in %ds", i)
			return
		}
	}
	t.Fatalf("pushBack remains true after 5 seconds despite being caught up!")
}

func canSkipPGTest(t *testing.T, ctx context.Context) bool {
	t.Helper()

	pool, err := pgxpool.New(ctx, *pgURI)
	if err != nil {
		if *isPGTestOptional {
			return true
		}
		t.Fatalf("failed to create PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		if *isPGTestOptional {
			return true
		}
		t.Fatalf("failed to ping PostgreSQL test db: %v", err)
	}
	return false
}

func mustDropTables(t *testing.T, ctx context.Context) {
	t.Helper()

	pool, err := pgxpool.New(ctx, *pgURI)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS AntispamMeta, AntispamIDSeq, AntispamFollowCoord"); err != nil {
		t.Fatalf("failed to drop all tables: %v", err)
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
