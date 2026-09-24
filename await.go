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

package tessera

import (
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"time"

	"log/slog"

	"github.com/transparency-dev/tessera/internal/otel"
	"github.com/transparency-dev/tessera/internal/parse"
	"go.opentelemetry.io/otel/trace"
)

// NewPublicationAwaiter provides a PublicationAwaiter that can be cancelled
// using the provided context. The PublicationAwaiter will poll every `pollPeriod`
// to fetch checkpoints using the `readCheckpoint` function.
func NewPublicationAwaiter(ctx context.Context, readCheckpoint func(ctx context.Context) ([]byte, error), pollPeriod time.Duration) *PublicationAwaiter {
	a := &PublicationAwaiter{
		waiters: make(map[*waiter]struct{}),
	}
	go a.pollLoop(ctx, readCheckpoint, pollPeriod)
	return a
}

// awaitResult is the outcome delivered to a single blocked Await call.
type awaitResult struct {
	checkpoint []byte
	err        error
}

// waiter represents a single blocked Await call.
type waiter struct {
	index uint64

	// errObserved preserves the "two consecutive errors" tolerance that Await
	// has always had: a waiter is only failed once it has seen two consecutive
	// failed polls. Guarded by PublicationAwaiter.mu.
	errObserved bool

	// res is buffered with capacity 1 and is written to at most once, so the
	// poll loop never blocks while resolving a waiter.
	res chan awaitResult
}

// PublicationAwaiter allows client threads to block until a leaf is published.
// This means it has a sequence number, and been integrated into the tree, and
// a checkpoint has been published for it.
// A single long-lived PublicationAwaiter instance
// should be reused for all requests in the application code as there is some
// overhead to each one; the core of a PublicationAwaiter is a poll loop that
// will fetch checkpoints whenever it has clients waiting.
//
// The expected call pattern is:
//
// i, cp, err := awaiter.Await(ctx, storage.Add(myLeaf))
//
// When used this way, it requires very little code at the point of use to
// block until the new leaf is integrated into the tree.
type PublicationAwaiter struct {
	mu sync.Mutex

	// waiters is the set of currently blocked Await calls. The poll loop
	// resolves and removes only the entries which the latest observation
	// satisfies, so unrelated waiters are left undisturbed.
	waiters map[*waiter]struct{}

	// Only used for testing coordination
	preWaitSignaller chan struct{}

	// size, checkpoint, and err keep track of the latest size and checkpoint
	// (or error) seen by the poller.
	size       uint64
	checkpoint []byte
	err        error

	// closed is set once the poll loop has exited, after which no further
	// observations will ever be published.
	closed bool
}

// publish records the latest observation from the poll loop and resolves every
// waiter which that observation settles, leaving the rest blocked.
//
// closed is only ever set from the poll loop's ctx.Done() path, which always
// records that context's error, so err is non-nil whenever closed is true.
func (a *PublicationAwaiter) publish(cp []byte, size uint64, err error, closed bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.checkpoint, a.size, a.err, a.closed = cp, size, err, closed

	for w := range a.waiters {
		var res awaitResult
		switch {
		case a.size > w.index:
			res = awaitResult{checkpoint: a.checkpoint} // Success
		case a.err != nil && w.errObserved:
			res = awaitResult{checkpoint: a.checkpoint, err: a.err} // Second consecutive error
		case a.closed:
			// The poll loop has gone, so this waiter can never be satisfied.
			res = awaitResult{err: a.err}
		default:
			w.errObserved = a.err != nil
			continue
		}
		w.res <- res
		delete(a.waiters, w)
	}
}

// Await blocks until the IndexFuture is resolved, and this new index has been
// integrated into the log, i.e. the log has made a checkpoint available that
// commits to this new index. When this happens, Await returns the index at
// which the leaf has been added, and a checkpoint that commits to this index.
//
// This operation can be aborted early by cancelling the context. In this event,
// or in the event that there is an error getting a valid checkpoint, an error
// will be returned from this method.
func (a *PublicationAwaiter) Await(ctx context.Context, future IndexFuture) (Index, []byte, error) {
	return otel.Trace2(ctx, "tessera.Await", tracer, func(ctx context.Context, span trace.Span) (Index, []byte, error) {
		i, err := future()
		if err != nil {
			return i, nil, err
		}
		span.AddEvent("Resolved future")
		span.SetAttributes(indexKey.Int64(int64(i.Index)), dupeKey.Bool(i.IsDup))

		a.mu.Lock()
		if a.preWaitSignaller != nil {
			a.preWaitSignaller <- struct{}{}
		}
		// Fast path: the tree has already grown past this index.
		if a.size > i.Index {
			cp := a.checkpoint
			a.mu.Unlock()
			return i, cp, nil // Success
		}
		if a.closed {
			cErr := a.err
			a.mu.Unlock()
			return i, nil, cErr
		}
		// Register interest before releasing the lock, so that an observation
		// published concurrently cannot be missed. errObserved is seeded from
		// the current state to preserve the pre-existing behaviour of only
		// failing after two consecutive errors.
		w := &waiter{
			index:       i.Index,
			errObserved: a.err != nil,
			res:         make(chan awaitResult, 1),
		}
		a.waiters[w] = struct{}{}
		a.mu.Unlock()

		span.AddEvent("Waiting for tree growth")

		// Await the tree growing to include the new leaf, two consecutive
		// errors being reported, the poll loop going away, or our own context
		// being cancelled or expiring.
		select {
		case res := <-w.res:
			return i, res.checkpoint, res.err
		case <-ctx.Done():
			a.mu.Lock()
			delete(a.waiters, w)
			a.mu.Unlock()
			return i, nil, ctx.Err()
		}
	})
}

// pollLoop MUST be called in a goroutine when constructing a PublicationAwaiter
// and will run continually until its context is cancelled. It wakes up every
// `pollPeriod`, requests the latest checkpoint from the log, parses the tree
// size, and releases all clients that were blocked on an index smaller than
// this tree size.
func (a *PublicationAwaiter) pollLoop(ctx context.Context, readCheckpoint func(ctx context.Context) ([]byte, error), pollPeriod time.Duration) {
	var (
		cp     []byte
		cpErr  error
		cpSize uint64
	)
	for done := false; !done; {
		done, _ = otel.Trace(ctx, "tessera.awaiter.pollLoopIteration", tracer, func(ctx context.Context, span trace.Span) (bool, error) {

			ctxDone := false

			select {
			case <-ctx.Done():
				span.AddEvent("context.done")
				slog.DebugContext(ctx, "PublicationAwaiter exiting due to context completion")
				cp, cpSize, cpErr = nil, 0, ctx.Err()
				ctxDone = true
			case <-time.After(pollPeriod):
				span.AddEvent("tessera.wake")
				cp, cpErr = readCheckpoint(ctx)
				switch {
				case errors.Is(cpErr, os.ErrNotExist):
					return false, nil
				case cpErr != nil:
					cpSize = 0
				default:
					_, cpSize, _, cpErr = parse.CheckpointUnsafe(cp)
					if cpSize <= math.MaxInt64 && cpErr == nil {
						span.SetAttributes(checkpointSizeKey.Int64(int64(cpSize)))
					}
				}
			}

			span.AddEvent("Publishing")
			a.publish(cp, cpSize, cpErr, ctxDone)
			span.AddEvent("Published")

			return ctxDone, nil
		}, trace.WithAttributes(otel.PeriodicKey.Bool(true)))
	}
}
