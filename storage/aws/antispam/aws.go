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

// Package aws contains an AWS-based antispam implementation for Tessera.
//
// A MySQL database provides a mechanism for maintaining an index of
// hash --> log position for detecting duplicate submissions.
package aws

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync/atomic"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"k8s.io/klog/v2"

	_ "github.com/go-sql-driver/mysql"
)

const (
	DefaultMaxBatchSize      = 64
	DefaultPushbackThreshold = 2048

	// SchemaCompatibilityVersion represents the expected version (e.g. layout & serialisation) of stored data.
	//
	// A binary built with a given version of the Tessera library is compatible with stored data created by a different version
	// of the library if and only if this value is the same as the compatibilityVersion stored in the Tessera table.
	//
	// NOTE: if changing this version, you need to consider whether end-users are going to update their schema instances to be
	// compatible with the new format, and provide a means to do it if so.
	SchemaCompatibilityVersion = 1
)

var errPushback = fmt.Errorf("antispam %w", tessera.ErrPushback)

// AntispamOpts allows configuration of some tunable options.
type AntispamOpts struct {
	// MaxBatchSize is the largest number of mutations permitted in a single write operation when
	// updating the antispam index.
	//
	// Larger batches can enable (up to a point) higher throughput, but care should be taken not to
	// overload the database instance.
	MaxBatchSize uint

	// PushbackThreshold allows configuration of when to start responding to Add requests with pushback due to
	// the antispam follower falling too far behind.
	//
	// When the antispam follower is at least this many entries behind the size of the locally integrated tree,
	// the antispam decorator will return a wrapped tessera.ErrPushback for every Add request.
	PushbackThreshold uint

	PushbackMaxOutstanding uint64
	MaxOpenConns           int
	MaxIdleConns           int
}

type AntispamStorage struct {
	opts AntispamOpts

	dbPool *sql.DB

	// pushBack is used to prevent the follower from getting too far underwater.
	// Populate dynamically will set this to true/false based on how far behind the follower is from the
	// currently integrated tree size.
	// When pushBack is true, the decorator will start returning a wrapped ErrPushback to all calls.
	pushBack atomic.Bool

	numLookups atomic.Uint64
	numWrites  atomic.Uint64
	numHits    atomic.Uint64
}

// NewAntispam returns an antispam driver which uses a MySQL table to maintain a mapping of
// previously seen entries and their assigned indices.
//
// Note that the storage for this mapping is entirely separate and unconnected to the storage used for
// maintaining the Merkle tree.
//
// This functionality is experimental!
func NewAntispam(ctx context.Context, dsn string, opts AntispamOpts) (*AntispamStorage, error) {
	if opts.MaxBatchSize == 0 {
		opts.MaxBatchSize = DefaultMaxBatchSize
	}
	if opts.PushbackThreshold == 0 {
		opts.PushbackThreshold = DefaultPushbackThreshold
	}

	dbPool, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL db: %v", err)
	}

	if opts.MaxOpenConns > 0 {
		dbPool.SetMaxOpenConns(opts.MaxOpenConns)
	}
	if opts.MaxIdleConns >= 0 {
		dbPool.SetMaxIdleConns(opts.MaxIdleConns)
	}

	if err := dbPool.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL db: %v", err)
	}

	r := &AntispamStorage{
		opts:   opts,
		dbPool: dbPool,
	}

	if err := r.initDB(ctx); err != nil {
		return nil, fmt.Errorf("failed to initDB: %v", err)
	}
	if err := r.checkDataCompatibility(ctx); err != nil {
		return nil, fmt.Errorf("schema is not compatible with this version of the Tessera library: %v", err)
	}
	return r, nil
}

func (s *AntispamStorage) initDB(ctx context.Context) error {
	if _, err := s.dbPool.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS AntispamMeta (
			id INT UNSIGNED NOT NULL,
			compatibilityVersion BIGINT UNSIGNED NOT NULL,
			PRIMARY KEY (id)
		)`); err != nil {
		return err
	}
	if _, err := s.dbPool.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS AntispamIDSeq (
			h TINYBLOB NOT NULL,
			idx BIGINT UNSIGNED NOT NULL,
			PRIMARY KEY (h(32))
		)`); err != nil {
		return err
	}
	if _, err := s.dbPool.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS AntispamFollowCoord (
			id INT UNSIGNED NOT NULL,
			nextIdx BIGINT UNSIGNED NOT NULL,
			PRIMARY KEY (id)
		)`); err != nil {
		return err
	}
	// Set default values for a newly initialised schema - these rows being present are a precondition for
	// following and population of mapping data to occur.
	// Note that this will only succeed if no row exists, so there's no danger
	// of "resetting" an existing antispam database.
	if _, err := s.dbPool.ExecContext(ctx,
		`INSERT IGNORE INTO AntispamMeta (id, compatibilityVersion) VALUES (0, ?)`, SchemaCompatibilityVersion); err != nil {
		return err
	}
	if _, err := s.dbPool.ExecContext(ctx,
		`INSERT IGNORE INTO AntispamFollowCoord (id, nextIdx) VALUES (0, 0)`); err != nil {
		return err
	}
	return nil
}

// checkDataCompatibility compares the Tessera library SchemaCompatibilityVersion with the one stored in the
// database, and returns an error if they are not identical.
func (s *AntispamStorage) checkDataCompatibility(ctx context.Context) error {
	row := s.dbPool.QueryRowContext(ctx, "SELECT compatibilityVersion FROM AntispamMeta WHERE id = 0")
	var gotVersion uint64
	if err := row.Scan(&gotVersion); err != nil {
		return fmt.Errorf("failed to read schema compatibility version from DB: %v", err)
	}

	if gotVersion != SchemaCompatibilityVersion {
		return fmt.Errorf("schema compatibilityVersion (%d) != library compatibilityVersion (%d)", gotVersion, SchemaCompatibilityVersion)
	}
	return nil
}

// index returns the index (if any) previously associated with the provided hash
func (d *AntispamStorage) index(ctx context.Context, h []byte) (*uint64, error) {
	d.numLookups.Add(1)
	row := d.dbPool.QueryRowContext(ctx, "SELECT idx FROM AntispamIDSeq WHERE h = ?", h)

	var idx uint64
	if err := row.Scan(&idx); err == sql.ErrNoRows {
		return nil, nil
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
				// The follower is too far behind the currently integrated tree, so we're going to push back against
				// the incoming requests.
				// This should have two effects:
				//   1. The tree will cease growing, giving the follower a chance to catch up, and
				//   2. We'll stop doing lookups for each submission, freeing up the DB to catch up.
				//
				// We may decide in the future that serving duplicate reads is more important than catching up as quickly
				// as possible, in which case we'd move this check down below the call to index.
				return func() (tessera.Index, error) { return tessera.Index{}, errPushback }
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
//
// This implements tessera.Antispam.
func (d *AntispamStorage) Follower(b func([]byte) ([][]byte, error)) tessera.Follower {
	return &follower{
		as:           d,
		bundleHasher: b,
	}
}

// follower is a struct which knows how to populate the antispam storage with identity hashes
// for entries in a log.
type follower struct {
	as           *AntispamStorage
	bundleHasher func([]byte) ([][]byte, error)
}

func (f *follower) Name() string {
	return "AWS antispam"
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
		size, err := lr.IntegratedSize(ctx)
		if err != nil {
			klog.Errorf("Populate: IntegratedSize(): %v", err)
			continue
		}

		// Busy loop while there's work to be done
		for streamDone := false; !streamDone; {
			select {
			case <-ctx.Done():
				return
			default:
			}
			err = func() error {
				tx, err := f.as.dbPool.BeginTx(ctx, &sql.TxOptions{
					ReadOnly: false,
				})
				if err != nil {
					return err
				}

				defer func() {
					if tx != nil {
						_ = tx.Rollback()
					}
				}()

				row := tx.QueryRowContext(ctx, "SELECT nextIdx FROM AntispamFollowCoord WHERE id = 0 FOR UPDATE")

				var followFrom uint64
				if err := row.Scan(&followFrom); err != nil {
					return err
				}

				if followFrom >= size {
					// Our view of the log is out of date, exit the busy loop and refresh it.
					streamDone = true
					return nil
				}

				f.as.pushBack.Store(size-followFrom > uint64(f.as.opts.PushbackThreshold))

				// If this is the first time around the loop we need to start the stream of entries now that we know where we want to
				// start reading from:
				if next == nil {
					sizeFn := func(_ context.Context) (uint64, error) {
						return size, nil
					}
					numFetchers := uint(10)
					next, stop = iter.Pull2(client.Entries(client.EntryBundles(ctx, numFetchers, sizeFn, lr.ReadEntryBundle, followFrom, size-followFrom), f.bundleHasher))
				}

				bs := uint64(f.as.opts.MaxBatchSize)
				if r := size - followFrom; r < bs {
					bs = r
				}
				curEntries := make([][]byte, 0, bs)
				for i := range int(bs) {
					e, err, ok := next()
					if !ok {
						// The entry stream has ended so we'll need to start a new stream next time around the loop:
						stop()
						next = nil
						break
					}
					if err != nil {
						return fmt.Errorf("entryReader.next: %v", err)
					}
					if wantIdx := followFrom + uint64(i); e.Index != wantIdx {
						// We're out of sync
						return errOutOfSync
					}
					curEntries = append(curEntries, e.Entry)
				}

				if len(curEntries) == 0 {
					return nil
				}

				klog.V(1).Infof("Inserting %d entries into antispam database (follow from %d of size %d)", len(curEntries), followFrom, size)

				args := make([]string, 0, len(curEntries))
				vals := make([]any, 0, 2*len(curEntries))
				for i, e := range curEntries {
					args = append(args, "(?, ?)")
					vals = append(vals, e, followFrom+uint64(i))
				}
				sqlStr := fmt.Sprintf("INSERT IGNORE INTO AntispamIDSeq (h, idx) VALUES %s", strings.Join(args, ","))

				_, err = tx.ExecContext(ctx, sqlStr, vals...)
				if err != nil {
					return fmt.Errorf("failed to insert into AntispamIDSeq with query %q: %v", sqlStr, err)
				}
				numAdded := uint64(len(curEntries))
				f.as.numWrites.Add(numAdded)
				nextIdx := uint64(followFrom + numAdded)

				// Insertion of dupe entries was successful, so update our follow coordination row:
				_, err = tx.ExecContext(ctx, "UPDATE AntispamFollowCoord SET nextIdx=? WHERE id=0", nextIdx)
				if err != nil {
					return fmt.Errorf("error updating AntispamFollowCoord: %v", err)
				}
				if err := tx.Commit(); err != nil {
					return err
				}
				tx = nil
				return nil
			}()
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
	row := f.as.dbPool.QueryRowContext(ctx, "SELECT nextIdx FROM AntispamFollowCoord WHERE id = 0")

	var idx uint64
	if err := row.Scan(&idx); err != nil {
		return 0, fmt.Errorf("failed to read follow coordination info: %v", err)
	}

	return idx, nil
}
