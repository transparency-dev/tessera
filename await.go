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
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/transparency-dev/tessera/internal/otel"
	"github.com/transparency-dev/tessera/internal/parse"
	"go.opentelemetry.io/otel/trace"
)

const (
	// bucketShift groups 16 consecutive log indices onto a single wakeup
	// channel. With 1,024 buckets (8 KB inline, channels lazily allocated on
	// demand), waiters span a 16,384-index window before slots wrap around.
	bucketShift = 4
	numBuckets  = 1024
	bucketMask  = numBuckets - 1
)

// NewPublicationAwaiter provides a PublicationAwaiter that can be cancelled
// using the provided context. The PublicationAwaiter will poll every `pollPeriod`
// to fetch checkpoints using the `readCheckpoint` function.
func NewPublicationAwaiter(ctx context.Context, readCheckpoint func(ctx context.Context) ([]byte, error), pollPeriod time.Duration) *PublicationAwaiter {
	a := &PublicationAwaiter{}
	a.state.Store(&awaitState{})
	go a.pollLoop(ctx, readCheckpoint, pollPeriod)
	return a
}

// awaitState is an immutable snapshot of the latest observation published by
// pollLoop. Await reads it lock-free.
type awaitState struct {
	size       uint64
	checkpoint []byte
	err        error
	errCount   int
	closed     bool
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
	state atomic.Pointer[awaitState]

	// buckets holds lazily-allocated wakeup channels indexed by
	// (index >> bucketShift) & bucketMask. Waiters install a channel with
	// CompareAndSwap(nil, &ch) before loading state; pollLoop stores the new
	// state before swapping affected buckets back to nil and closing them.
	// This avoids per-waiter allocations, mutex contention, and waking
	// unrelated waiters in later buckets.
	buckets [numBuckets]atomic.Pointer[chan struct{}]

	// Only used for testing coordination
	preWaitSignaller chan struct{}
}

// bucketChan returns the active wakeup channel for slot, lazily creating and
// installing one if the slot is empty.
func (a *PublicationAwaiter) bucketChan(slot uint64) chan struct{} {
	for {
		if p := a.buckets[slot].Load(); p != nil {
			return *p
		}
		ch := make(chan struct{})
		if a.buckets[slot].CompareAndSwap(nil, &ch) {
			return ch
		}
	}
}

// closeBucket removes and closes any active wakeup channel in slot.
// Must only be called by pollLoop.
func (a *PublicationAwaiter) closeBucket(slot uint64) {
	if a.buckets[slot].Load() == nil {
		return
	}
	if prev := a.buckets[slot].Swap(nil); prev != nil {
		close(*prev)
	}
}

// closeAllBuckets closes every active wakeup channel across all buckets.
// Must only be called by pollLoop.
func (a *PublicationAwaiter) closeAllBuckets() {
	for i := range uint64(numBuckets) {
		a.closeBucket(i)
	}
}

// closeRange closes every active wakeup channel covering indices in [oldSize, newSize).
// Must only be called by pollLoop.
func (a *PublicationAwaiter) closeRange(oldSize, newSize uint64) {
	if newSize <= oldSize {
		return
	}
	startBucket := oldSize >> bucketShift
	endBucket := (newSize - 1) >> bucketShift
	if endBucket-startBucket+1 >= numBuckets {
		a.closeAllBuckets()
		return
	}
	for b := startBucket; b <= endBucket; b++ {
		a.closeBucket(b & bucketMask)
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

		s := a.state.Load()
		if a.preWaitSignaller != nil {
			a.preWaitSignaller <- struct{}{}
		}

		slot := (i.Index >> bucketShift) & bucketMask
		waited := false
		// Await the tree growing to include the new leaf, two consecutive
		// poll errors, poller shutdown, or our own context being done.
		for ctx.Err() == nil {
			if s.size > i.Index {
				return i, s.checkpoint, nil // Success
			}
			if s.closed {
				return i, nil, s.err
			}
			if waited && s.errCount >= 2 {
				return i, nil, s.err // Second consecutive error
			}

			// Acquire the bucket channel BEFORE reloading state. Combined with
			// pollLoop storing state BEFORE swapping and closing buckets, Go's
			// sequentially consistent atomics guarantee no wakeup can be lost.
			ch := a.bucketChan(slot)
			s = a.state.Load()
			if s.size > i.Index || s.closed || (waited && s.errCount >= 2) {
				continue
			}
			if !waited {
				span.AddEvent("Waiting for tree growth")
			}

			select {
			case <-ch:
				waited = true
				s = a.state.Load()
			case <-ctx.Done():
				return i, nil, ctx.Err()
			}
		}

		// The loop only exits if the context was cancelled or expired.
		return i, nil, ctx.Err()
	})
}

// pollLoop MUST be called in a goroutine when constructing a PublicationAwaiter
// and will run continually until its context is cancelled. It wakes up every
// `pollPeriod`, requests the latest checkpoint from the log, parses the tree
// size, and releases all clients that were blocked on an index smaller than
// this tree size.
func (a *PublicationAwaiter) pollLoop(ctx context.Context, readCheckpoint func(ctx context.Context) ([]byte, error), pollPeriod time.Duration) {
	consecutiveErrs := 0
	for done := false; !done; {
		done, _ = otel.Trace(ctx, "tessera.awaiter.pollLoopIteration", tracer, func(ctx context.Context, span trace.Span) (bool, error) {
			var (
				cp      []byte
				cpErr   error
				cpSize  uint64
				ctxDone bool
			)

			select {
			case <-ctx.Done():
				span.AddEvent("context.done")
				slog.DebugContext(ctx, "PublicationAwaiter exiting due to context completion")
				cpErr = ctx.Err()
				ctxDone = true
			case <-time.After(pollPeriod):
				span.AddEvent("tessera.wake")
				cp, cpErr = readCheckpoint(ctx)
				switch {
				case errors.Is(cpErr, os.ErrNotExist):
					// The log has not published a checkpoint yet.
					cp, cpSize, cpErr = nil, 0, nil
				case cpErr != nil:
					cpSize = 0
				default:
					_, cpSize, _, cpErr = parse.CheckpointUnsafe(cp)
					if cpSize <= math.MaxInt64 && cpErr == nil {
						span.SetAttributes(checkpointSizeKey.Int64(int64(cpSize)))
					}
				}
			}

			prev := a.state.Load()

			// If pollLoop's context was cancelled while readCheckpoint was
			// in-flight, close immediately while preserving any newer valid
			// checkpoint that was just read.
			if !ctxDone && ctx.Err() != nil {
				span.AddEvent("context.done")
				slog.DebugContext(ctx, "PublicationAwaiter exiting due to context completion")
				if cpErr != nil {
					cpSize = 0
				}
				cpErr = ctx.Err()
				ctxDone = true
			}

			switch {
			case ctxDone:
				size, bestCP := prev.size, prev.checkpoint
				if cpSize > size {
					size, bestCP = cpSize, cp
				}
				span.AddEvent("Publishing")
				a.state.Store(&awaitState{
					size:       size,
					checkpoint: bestCP,
					err:        cpErr,
					closed:     true,
				})
				a.closeAllBuckets()
				span.AddEvent("Published")
			case cpErr != nil:
				consecutiveErrs++
				if consecutiveErrs >= 2 {
					span.AddEvent("Publishing")
					a.state.Store(&awaitState{
						size:       prev.size,
						checkpoint: prev.checkpoint,
						err:        cpErr,
						errCount:   consecutiveErrs,
					})
					a.closeAllBuckets()
					span.AddEvent("Published")
				}
			default:
				consecutiveErrs = 0
				if cpSize > prev.size {
					span.AddEvent("Publishing")
					a.state.Store(&awaitState{
						size:       cpSize,
						checkpoint: cp,
					})
					a.closeRange(prev.size, cpSize)
					span.AddEvent("Published")
				}
			}

			return ctxDone, nil
		}, trace.WithAttributes(otel.PeriodicKey.Bool(true)))
	}
}
