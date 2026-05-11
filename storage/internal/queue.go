// Copyright 2024 Google LLC. All Rights Reserved.
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

// Package storage provides implementations and shared components for tessera storage backends.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/internal/future"
	"github.com/transparency-dev/tessera/internal/otel"
	"go.opentelemetry.io/otel/trace"
)

// Queue knows how to queue up a number of entries in order.
//
// When the buffered queue grows past a defined size, or the age of the oldest entry in the
// queue reaches a defined threshold, the queue will call a provided FlushFunc with
// a slice containing all queued entries in the same order as they were added.
type Queue struct {
	inputs chan queueItem
}

// FlushFunc is the signature of a function which will receive the slice of queued entries.
// Normally, this function would be provided by storage implementations. It's important to note
// that the implementation MUST call each entry's MarshalBundleData function before attempting
// to integrate it into the tree.
// See the comment on Entry.MarshalBundleData for further info.
type FlushFunc func(ctx context.Context, entries []*tessera.Entry) error

// NewQueue creates a new queue with the specified maximum age and size.
//
// The provided FlushFunc will be called with a slice containing the contents of the queue, in
// the same order as they were added, when either the oldest entry in the queue has been there
// for maxAge, or the size of the queue reaches maxSize.
func NewQueue(ctx context.Context, maxAge time.Duration, maxSize uint, f FlushFunc) *Queue {
	q := &Queue{
		inputs: make(chan queueItem, maxSize),
	}
	batches := make(chan []queueItem, 1)

	// Spin off a goroutine which accumulates added items into batches and sends them to the flush worker
	// via the batches channel.
	go func() {
		defer close(batches)

		var items []queueItem

		// Initialise a timer with an arbitrarily large value, and immediately
		// call Stop() to avoid a spurious trigger on the first iteration.
		timer := time.NewTimer(time.Hour)
		timer.Stop()

		flush := func() {
			if len(items) == 0 {
				return
			}
			if !timer.Stop() {
				// If the timer has already fired, drain it to avoid a stutter.
				select {
				case <-timer.C:
				default:
				}
			}
			// Send to batches channel. This might block if the worker is busy,
			// but that's fine as it applies backpressure to the inputs channel.
			select {
			case batches <- items:
			case <-ctx.Done():
				return
			}
			items = nil
		}

		// Process the incoming items into batches, flushing the batch when either
		// the batch is full, or the max age is reached.
		for {
			select {
			case <-ctx.Done():
				return
			case item := <-q.inputs:
				items = append(items, item)
				if len(items) == 1 {
					timer.Reset(maxAge)
				}
				if len(items) >= int(maxSize) {
					flush()
				}
			case <-timer.C:
				flush()
			}
		}
	}()

	// Spin off a worker thread to process the flushed batches.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case b, ok := <-batches:
				if !ok {
					return
				}
				q.doFlush(ctx, f, b)
			}
		}
	}()

	return q
}

// Add places e into the queue, and returns a func which should be called to retrieve the assigned index.
func (q *Queue) Add(ctx context.Context, e *tessera.Entry) tessera.IndexFuture {
	qi := newEntry(e)
	select {
	case q.inputs <- qi:
	case <-ctx.Done():
		return func() (tessera.Index, error) {
			return tessera.Index{}, ctx.Err()
		}
	}
	return qi.f
}

// doFlush handles the queue flush, and sending notifications of assigned log indices.
func (q *Queue) doFlush(ctx context.Context, f FlushFunc, entries []queueItem) {
	err := otel.TraceErr(ctx, "tessera.storage.queue.doFlush", tracer, func(ctx context.Context, span trace.Span) error {
		entriesData := make([]*tessera.Entry, 0, len(entries))
		for _, e := range entries {
			entriesData = append(entriesData, e.entry)
		}

		return f(ctx, entriesData)
	})

	// Send assigned indices to all the waiting Add() requests
	for _, e := range entries {
		e.notify(err)
	}
}

// queueItem represents an in-flight queueItem in the queue.
//
// The f field acts as a future for the queueItem's assigned index/error, and will
// hang until assign is called.
type queueItem struct {
	entry *tessera.Entry
	f     tessera.IndexFuture
	set   func(tessera.Index, error)
}

// newEntry creates a new entry for the provided data.
func newEntry(data *tessera.Entry) queueItem {
	f, set := future.NewFutureErr[tessera.Index]()
	e := queueItem{
		entry: data,
		f:     f.Get,
		set:   set,
	}
	return e
}

// notify sets the assigned log index (or an error) to the entry.
//
// This func must only be called once, and will cause any current or future callers of index()
// to be given the values provided here.
func (e *queueItem) notify(err error) {
	if e.entry.Index() == nil && err == nil {
		panic(errors.New("logic error: flush complete without error, but entry was not assigned an index - did storage fail to call entry.MarshalBundleData?"))
	}
	var idx uint64
	if e.entry.Index() != nil {
		idx = *e.entry.Index()
	}
	e.set(tessera.Index{Index: idx}, err)
}
