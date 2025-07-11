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

// Package badger provides a Tessera persistent antispam driver based on
// BadgerDB (https://github.com/hypermodeinc/badger), a high-performance
// pure-go DB with KV support.
package badger

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/internal/otel"
	"k8s.io/klog/v2"
)

const (
	DefaultMaxBatchSize      = 1500
	DefaultPushbackThreshold = 2048
)

var (
	nextKey = []byte("@nextIdx")
)

// AntispamOpts allows configuration of some tunable options.
type AntispamOpts struct {
	// MaxBatchSize is the largest number of mutations permitted in a single BatchWrite operation when
	// updating the antispam index.
	//
	// Larger batches can enable (up to a point) higher throughput, but care should be taken not to
	// overload the Spanner instance.
	//
	// During testing, we've found that 1500 appears to offer maximum throughput when using Spanner instances
	// with 300 or more PU. Smaller deployments (e.g. 100 PU) will likely perform better with smaller batch
	// sizes of around 64.
	MaxBatchSize uint

	// PushbackThreshold allows configuration of when to start responding to Add requests with pushback due to
	// the antispam follower falling too far behind.
	//
	// When the antispam follower is at least this many entries behind the size of the locally integrated tree,
	// the antispam decorator will return tessera.ErrPushback for every Add request.
	PushbackThreshold uint
}

// NewAntispam returns an antispam driver which uses Badger to maintain a mapping between
// previously seen entries and their assigned indices.
//
// Note that the storage for this mapping is entirely separate and unconnected to the storage used for
// maintaining the Merkle tree.
//
// This functionality is experimental!
func NewAntispam(ctx context.Context, badgerPath string, opts AntispamOpts) (*AntispamStorage, error) {
	if opts.MaxBatchSize == 0 {
		opts.MaxBatchSize = DefaultMaxBatchSize
	}
	if opts.PushbackThreshold == 0 {
		opts.PushbackThreshold = DefaultPushbackThreshold
	}

	// Open the Badger database located at badgerPath, it will be created if it doesn't exist.
	db, err := badger.Open(badger.DefaultOptions(badgerPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open badger: %v", err)
	}

	r := &AntispamStorage{
		opts: opts,
		db:   db,
	}

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

		again:
			err := db.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}()

	return r, nil
}

type AntispamStorage struct {
	opts AntispamOpts

	db *badger.DB

	// pushBack is used to prevent the follower from getting too far underwater.
	// Populate dynamically will set this to true/false based on how far behind the follower is from the
	// currently integrated tree size.
	// When pushBack is true, the decorator will start returning ErrPushback to all calls.
	pushBack atomic.Bool

	numLookups atomic.Uint64
	numWrites  atomic.Uint64
	numHits    atomic.Uint64
}

// index returns the index (if any) previously associated with the provided hash
func (d *AntispamStorage) index(ctx context.Context, h []byte) (*uint64, error) {
	_, span := tracer.Start(ctx, "tessera.antispam.badger.index")
	defer span.End()

	d.numLookups.Add(1)
	var idx *uint64
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(h)
		if err == badger.ErrKeyNotFound {
			span.AddEvent("tessera.miss")
			return nil
		}
		span.AddEvent("tessera.hit")
		d.numHits.Add(1)

		return item.Value(func(v []byte) error {
			i := binary.BigEndian.Uint64(v)
			idx = &i
			return nil
		})
	})
	return idx, err
}

// Decorator returns a function which will wrap an underlying Add delegate with
// code to dedup against the stored data.
func (d *AntispamStorage) Decorator() func(f tessera.AddFn) tessera.AddFn {
	return func(delegate tessera.AddFn) tessera.AddFn {
		return func(ctx context.Context, e *tessera.Entry) tessera.IndexFuture {
			ctx, span := tracer.Start(ctx, "tessera.antispam.badger.Add")
			defer span.End()

			if d.pushBack.Load() {
				span.AddEvent("tessera.pushback")
				// The follower is too far behind the currently integrated tree, so we're going to push back against
				// the incoming requests.
				// This should have two effects:
				//   1. The tree will cease growing, giving the follower a chance to catch up, and
				//   2. We'll stop doing lookups for each submission, freeing up Spanner CPU to focus on catching up.
				//
				// We may decide in the future that serving duplicate reads is more important than catching up as quickly
				// as possible, in which case we'd move this check down below the call to index.
				return func() (tessera.Index, error) { return tessera.Index{}, tessera.ErrPushback }
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
	f := &follower{
		as:           d,
		bundleHasher: b,
	}

	return f
}

// follower is a struct which knows how to populate the antispam storage with identity hashes
// for entries in a log.
type follower struct {
	as *AntispamStorage

	bundleHasher func([]byte) ([][]byte, error)
}

func (f *follower) Name() string {
	return "Badger antispam"
}

// Follow uses entry data from the log to populate the antispam storage.
func (f *follower) Follow(ctx context.Context, lr tessera.LogReader) {
	errOutOfSync := errors.New("out-of-sync")

	t := time.NewTicker(time.Second)
	var (
		next func() (client.Entry[[]byte], error, bool)
		stop func()

		curEntries [][]byte
		curIndex   uint64
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		// logSize is the latest known size of the log we're following.
		// This will get initialised below, inside the loop.
		var logSize uint64

		// Busy loop while there's work to be done
		for workDone := true; workDone; {
			err := f.as.db.Update(func(txn *badger.Txn) error {
				ctx, span := tracer.Start(ctx, "tessera.antispam.badger.FollowTxn")
				defer span.End()

				// Figure out the last entry we used to populate our antispam storage.
				var followFrom uint64

				switch row, err := txn.Get(nextKey); {
				case errors.Is(err, badger.ErrKeyNotFound):
					// Ignore this as we're probably just running for the first time on a new DB.
				case err != nil:
					return fmt.Errorf("failed to get nextIdx: %v", err)
				default:
					if err := row.Value(func(val []byte) error {
						followFrom = binary.BigEndian.Uint64(val)
						return nil
					}); err != nil {
						return fmt.Errorf("failed to get nextIdx value: %v", err)
					}
				}

				span.SetAttributes(followFromKey.Int64(otel.Clamp64(followFrom)))

				if followFrom > logSize {
					// Our view of the log is out of date, exit the busy loop and refresh it.
					workDone = false
					return nil
				}
				var err error
				if followFrom >= logSize {
					// Our view of the log is out of date, update it
					logSize, err = lr.IntegratedSize(ctx)
					if err != nil {
						workDone = true
						return fmt.Errorf("populate: IntegratedSize(): %v", err)
					}
					switch {
					case followFrom > logSize:
						workDone = true
						return fmt.Errorf("followFrom %d > size %d", followFrom, logSize)
					case followFrom == logSize:
						// We're caught up, so unblock pushback and go back to sleep
						workDone = true
						f.as.pushBack.Store(false)
						return nil
					default:
						// size > followFrom, so there's more work to be done!
					}
				}

				pushback := logSize-followFrom > uint64(f.as.opts.PushbackThreshold)
				span.SetAttributes(pushbackKey.Bool(pushback))
				f.as.pushBack.Store(pushback)

				// If this is the first time around the loop we need to start the stream of entries now that we know where we want to
				// start reading from:
				if next == nil {
					span.AddEvent("Start streaming entries")
					sizeFn := func(_ context.Context) (uint64, error) {
						return logSize, nil
					}
					numFetchers := uint(10)
					next, stop = iter.Pull2(client.Entries(client.EntryBundles(ctx, numFetchers, sizeFn, lr.ReadEntryBundle, followFrom, logSize-followFrom), f.bundleHasher))
				}

				if curIndex == followFrom && curEntries != nil {
					// Note that it's possible for Spanner to automatically retry transactions in some circumstances, when it does
					// it'll call this function again.
					// If the above condition holds, then we're in a retry situation and we must use the same data again rather
					// than continue reading entries which will take us out of sync.
				} else {
					bs := uint64(f.as.opts.MaxBatchSize)
					if r := logSize - followFrom; r < bs {
						bs = r
					}
					batch := make([][]byte, 0, bs)
					for i := range int(bs) {
						e, err, ok := next()
						if !ok {
							// The entry stream has ended so we'll need to start a new stream next time around the loop:
							next = nil
							break
						}
						if err != nil {
							return fmt.Errorf("entryReader.next: %v", err)
						}
						if wantIdx := followFrom + uint64(i); e.Index != wantIdx {
							klog.Infof("at %d, expected %d - out of sync", e.Index, wantIdx)
							// We're out of sync
							return errOutOfSync
						}
						batch = append(batch, e.Entry)
					}
					curEntries = batch
					curIndex = followFrom
				}

				// Now update the index.
				{
					for i, e := range curEntries {
						if _, err := txn.Get(e); err == badger.ErrKeyNotFound {
							b := make([]byte, 8)
							binary.BigEndian.PutUint64(b, curIndex+uint64(i))
							if err := txn.Set(e, b); err != nil {
								return err
							}
						}
					}
				}

				numAdded := uint64(len(curEntries))
				f.as.numWrites.Add(numAdded)

				// and update the follower state
				b := make([]byte, 8)
				binary.BigEndian.PutUint64(b, curIndex+numAdded)
				if err := txn.Set(nextKey, b); err != nil {
					return fmt.Errorf("failed to update follower state: %v", err)
				}

				return nil
			})
			if err != nil {
				if err != errOutOfSync {
					klog.Errorf("Failed to commit antispam population tx: %v", err)
				}
				stop()
				next = nil
				continue
			}
			curEntries = nil
		}
	}
}

// EntriesProcessed returns the total number of log entries processed.
func (f *follower) EntriesProcessed(ctx context.Context) (uint64, error) {
	var nextIdx uint64
	err := f.as.db.View(func(txn *badger.Txn) error {
		switch item, err := txn.Get(nextKey); {
		case errors.Is(err, badger.ErrKeyNotFound):
			// Ignore this, we've just not done any following yet.
			return nil
		case err != nil:
			return fmt.Errorf("failed to read nextKey: %v", err)
		default:
			return item.Value(func(val []byte) error {
				nextIdx = binary.BigEndian.Uint64(val)
				return nil
			})
		}
	})

	return nextIdx, err
}
