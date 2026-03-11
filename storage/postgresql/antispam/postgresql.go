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

// Package postgresql contains a PostgreSQL-based antispam implementation for Tessera.
//
// A PostgreSQL database provides a mechanism for maintaining an index of
// hash --> log position for detecting duplicate submissions.
package postgresql

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/internal/otel"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/klog/v2"
)

const (
	DefaultMaxBatchSize      = 64
	DefaultPushbackThreshold = 2048

	// SchemaCompatibilityVersion represents the expected version of stored antispam data.
	SchemaCompatibilityVersion = 1

	defaultBatchTimeout = 10 * time.Second
)

// AntispamOpts allows configuration of some tunable options.
type AntispamOpts struct {
	// MaxBatchSize is the largest number of mutations permitted in a single write operation when
	// updating the antispam index.
	MaxBatchSize uint

	// PushbackThreshold allows configuration of when to start responding to Add requests with pushback due to
	// the antispam follower falling too far behind.
	PushbackThreshold uint

	// MaxConns sets the maximum number of connections in the pool.
	MaxConns int32
}

// AntispamStorage implements the tessera.Antispam interface using PostgreSQL.
type AntispamStorage struct {
	opts AntispamOpts
	pool *pgxpool.Pool

	pushBack   atomic.Bool
	numLookups atomic.Uint64
	numWrites  atomic.Uint64
	numHits    atomic.Uint64
}

// NewAntispam returns an antispam driver which uses PostgreSQL tables to maintain a mapping of
// previously seen entries and their assigned indices.
//
// Note that the storage for this mapping is entirely separate and unconnected to the storage used for
// maintaining the Merkle tree.
func NewAntispam(ctx context.Context, dsn string, opts AntispamOpts) (*AntispamStorage, error) {
	if opts.MaxBatchSize == 0 {
		opts.MaxBatchSize = DefaultMaxBatchSize
	}
	if opts.PushbackThreshold == 0 {
		opts.PushbackThreshold = DefaultPushbackThreshold
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PostgreSQL DSN: %v", err)
	}
	if opts.MaxConns > 0 {
		config.MaxConns = opts.MaxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL db: %v", err)
	}

	r := &AntispamStorage{
		opts: opts,
		pool: pool,
	}

	if err := r.initDB(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to initDB: %v", err)
	}
	if err := r.checkDataCompatibility(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("schema is not compatible with this version of the Tessera library: %v", err)
	}
	return r, nil
}

func (s *AntispamStorage) initDB(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS AntispamMeta (
			id SMALLINT NOT NULL PRIMARY KEY,
			compatibilityVersion BIGINT NOT NULL
		)`); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS AntispamIDSeq (
			h BYTEA NOT NULL PRIMARY KEY,
			idx BIGINT NOT NULL
		)`); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS AntispamFollowCoord (
			id SMALLINT NOT NULL PRIMARY KEY,
			nextIdx BIGINT NOT NULL
		)`); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO AntispamMeta (id, compatibilityVersion) VALUES (0, $1) ON CONFLICT (id) DO NOTHING`, SchemaCompatibilityVersion); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO AntispamFollowCoord (id, nextIdx) VALUES (0, 0) ON CONFLICT (id) DO NOTHING`); err != nil {
		return err
	}
	return nil
}

func (s *AntispamStorage) checkDataCompatibility(ctx context.Context) error {
	row := s.pool.QueryRow(ctx, "SELECT compatibilityVersion FROM AntispamMeta WHERE id = 0")
	var gotVersion uint64
	if err := row.Scan(&gotVersion); err != nil {
		return fmt.Errorf("failed to read schema compatibility version from DB: %v", err)
	}

	if gotVersion != SchemaCompatibilityVersion {
		return fmt.Errorf("schema compatibilityVersion (%d) != library compatibilityVersion (%d)", gotVersion, SchemaCompatibilityVersion)
	}
	return nil
}

// index returns the index (if any) previously associated with the provided hash.
func (d *AntispamStorage) index(ctx context.Context, h []byte) (*uint64, error) {
	d.numLookups.Add(1)
	row := d.pool.QueryRow(ctx, "SELECT idx FROM AntispamIDSeq WHERE h = $1", h)

	var idx uint64
	if err := row.Scan(&idx); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	d.numHits.Add(1)
	return &idx, nil
}

// Decorator returns a function which will wrap an underlying Add delegate with
// code to dedup against the stored data.
func (d *AntispamStorage) Decorator() func(f tessera.AddFn) tessera.AddFn {
	return func(delegate tessera.AddFn) tessera.AddFn {
		return func(ctx context.Context, e *tessera.Entry) tessera.IndexFuture {
			if d.pushBack.Load() {
				return func() (tessera.Index, error) { return tessera.Index{}, tessera.ErrPushbackAntispam }
			}
			idx, err := d.index(ctx, e.Identity())
			if err != nil {
				return func() (tessera.Index, error) { return tessera.Index{}, err }
			}
			if idx != nil {
				return func() (tessera.Index, error) { return tessera.Index{Index: *idx, IsDup: true}, nil }
			}

			return delegate(ctx, e)
		}
	}
}

// Follower returns a follower which knows how to populate the antispam index.
func (d *AntispamStorage) Follower(b func([]byte) ([][]byte, error)) tessera.Follower {
	return &follower{
		as:           d,
		bundleHasher: b,
	}
}

type follower struct {
	as           *AntispamStorage
	bundleHasher func([]byte) ([][]byte, error)
}

func (f *follower) Name() string {
	return "PostgreSQL antispam"
}

// Follow uses entry data from the log to populate the antispam storage.
func (f *follower) Follow(ctx context.Context, lr tessera.LogReader) {
	errOutOfSync := errors.New("out-of-sync")

	t := time.NewTicker(time.Second)
	var (
		next func() (client.Entry[[]byte], error, bool)
		stop func()
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		var logSize uint64

		for streamDone := false; !streamDone; {
			select {
			case <-ctx.Done():
				return
			default:
			}
			err := otel.TraceErr(ctx, "tessera.antispam.postgresql.FollowTask", tracer, func(ctx context.Context, span trace.Span) error {
				ctx, cancel := context.WithTimeout(ctx, defaultBatchTimeout)
				defer cancel()

				tx, err := f.as.pool.Begin(ctx)
				if err != nil {
					return err
				}

				defer func() {
					if tx != nil {
						_ = tx.Rollback(ctx)
					}
				}()

				row := tx.QueryRow(ctx, "SELECT nextIdx FROM AntispamFollowCoord WHERE id = 0 FOR UPDATE")

				var followFrom uint64
				if err := row.Scan(&followFrom); err != nil {
					return err
				}

				if followFrom >= logSize {
					logSize, err = lr.IntegratedSize(ctx)
					if err != nil {
						streamDone = true
						return fmt.Errorf("populate: IntegratedSize(): %v", err)
					}
					switch {
					case followFrom > logSize:
						streamDone = true
						return fmt.Errorf("followFrom %d > size %d", followFrom, logSize)
					case followFrom == logSize:
						streamDone = true
						f.as.pushBack.Store(false)
						return ctx.Err()
					default:
					}
				}

				f.as.pushBack.Store(logSize-followFrom > uint64(f.as.opts.PushbackThreshold))

				if next == nil {
					sizeFn := func(_ context.Context) (uint64, error) {
						return logSize, nil
					}
					numFetchers := uint(10)
					next, stop = iter.Pull2(client.Entries(client.EntryBundles(ctx, numFetchers, sizeFn, lr.ReadEntryBundle, followFrom, logSize-followFrom), f.bundleHasher))
				}

				bs := uint64(f.as.opts.MaxBatchSize)
				if r := logSize - followFrom; r < bs {
					bs = r
				}
				curEntries := make([][]byte, 0, bs)
				for i := range int(bs) {
					e, err, ok := next()
					if !ok {
						stop()
						next = nil
						break
					}
					if err != nil {
						return fmt.Errorf("entryReader.next: %v", err)
					}
					if wantIdx := followFrom + uint64(i); e.Index != wantIdx {
						return errOutOfSync
					}
					curEntries = append(curEntries, e.Entry)
				}

				if len(curEntries) == 0 {
					return ctx.Err()
				}

				klog.V(1).Infof("Inserting %d entries into antispam database (follow from %d of size %d)", len(curEntries), followFrom, logSize)

				batch := &pgx.Batch{}
				for i, e := range curEntries {
					batch.Queue("INSERT INTO AntispamIDSeq (h, idx) VALUES ($1, $2) ON CONFLICT DO NOTHING", e, followFrom+uint64(i))
				}
				br := tx.SendBatch(ctx, batch)
				for range len(curEntries) {
					if _, err := br.Exec(); err != nil {
						_ = br.Close()
						return fmt.Errorf("failed to insert into AntispamIDSeq: %v", err)
					}
				}
				if err := br.Close(); err != nil {
					return fmt.Errorf("failed to close batch: %v", err)
				}

				numAdded := uint64(len(curEntries))
				f.as.numWrites.Add(numAdded)
				nextIdx := followFrom + numAdded

				if _, err := tx.Exec(ctx, "UPDATE AntispamFollowCoord SET nextIdx=$1 WHERE id=0", nextIdx); err != nil {
					return fmt.Errorf("error updating AntispamFollowCoord: %v", err)
				}
				if err := tx.Commit(ctx); err != nil {
					return err
				}
				tx = nil
				return ctx.Err()
			})
			if err != nil {
				if err != errOutOfSync {
					klog.Errorf("Failed to commit antispam population tx: %v", err)
				}
				if next != nil {
					stop()
					next = nil
					stop = nil
				}
				streamDone = true
				continue
			}
		}
	}
}

// EntriesProcessed returns the total number of log entries processed.
func (f *follower) EntriesProcessed(ctx context.Context) (uint64, error) {
	row := f.as.pool.QueryRow(ctx, "SELECT nextIdx FROM AntispamFollowCoord WHERE id = 0")

	var idx uint64
	if err := row.Scan(&idx); err != nil {
		return 0, fmt.Errorf("failed to read follow coordination info: %v", err)
	}

	return idx, nil
}
