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

// NewPublicationAwaiter provides an PublicationAwaiter that can be cancelled
// using the provided context. The PublicationAwaiter will poll every `pollPeriod`
// to fetch checkpoints using the `readCheckpoint` function.
func NewPublicationAwaiter(ctx context.Context, readCheckpoint func(ctx context.Context) ([]byte, error), pollPeriod time.Duration) *PublicationAwaiter {
	a := &PublicationAwaiter{
		c: sync.NewCond(&sync.Mutex{}),
	}
	go a.pollLoop(ctx, readCheckpoint, pollPeriod)
	return a
}

// PublicationAwaiter allows client threads to block until a leaf is published.
// This means it has a sequence number, and been integrated into the tree, and
// a checkpoint has been published for it.
// A single long-lived PublicationAwaiter instance
// should be reused for all requests in the application code as there is some
// overhead to each one; the core of an PublicationAwaiter is a poll loop that
// will fetch checkpoints whenever it has clients waiting.
//
// The expected call pattern is:
//
// i, cp, err := awaiter.Await(ctx, storage.Add(myLeaf))
//
// When used this way, it requires very little code at the point of use to
// block until the new leaf is integrated into the tree.
type PublicationAwaiter struct {
	c *sync.Cond

	// Only used for testing coordination
	preWaitSignaller chan struct{}

	// size, checkpoint, and err keep track of the latest size and checkpoint
	// (or error) seen by the poller.
	size       uint64
	checkpoint []byte
	err        error
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

		span.AddEvent("Waiting for tree growth")
		a.c.L.Lock()
		defer a.c.L.Unlock()
		if a.preWaitSignaller != nil {
			a.preWaitSignaller <- struct{}{}
		}

		// Await the tree growing to include the new leaf, or for two consecutive errors to be reported.
		errorObserved := false
		for ctx.Err() == nil {
			if a.size > i.Index {
				return i, a.checkpoint, nil // Success
			}
			if a.err != nil {
				if errorObserved {
					return i, a.checkpoint, a.err // Second consecutive error
				}
				errorObserved = true
			} else {
				errorObserved = false
			}
			a.c.Wait()
		}

		// The loop only exits if the context was cancelled or expired.
		return i, nil, ctx.Err()
	})
}

// pollLoop MUST be called in a goroutine when constructing an PublicationAwaiter
// and will run continually until its context is cancelled. It wakes up every
// `pollPeriod` to check if there are clients blocking. If there are, it requests
// the latest checkpoint from the log, parses the tree size, and releases all clients
// that were blocked on an index smaller than this tree size.
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
					if cpSize <= math.MaxInt64 && cpErr != nil {
						span.SetAttributes(checkpointSizeKey.Int64(int64(cpSize)))
					}
				}
			}

			span.AddEvent("Taking lock")
			a.c.L.Lock()
			span.AddEvent("Locked")
			a.checkpoint = cp
			a.size = cpSize
			a.err = cpErr
			// Note that this releases all clients in the event of any failure.
			// However individual clients (via Await) can decide whether to ignore or fail.
			a.c.Broadcast()
			span.AddEvent("Broadcast Sent")
			a.c.L.Unlock()
			span.AddEvent("Unlocked")
			return ctxDone, nil
		}, trace.WithAttributes(otel.PeriodicKey.Bool(true)))
	}
}
