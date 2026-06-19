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

package loadtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/transparency-dev/tessera/client"

	"log/slog"
)

type HammerOpts struct {
	MaxReadOpsPerSecond  int
	MaxWriteOpsPerSecond int

	NumReadersRandom int
	NumReadersFull   int
	NumWriters       int
}

func NewHammer(tracker *client.LogStateTracker, f client.EntryBundleFetcherFunc, w LeafWriter, gen func() []byte, seqLeafChan chan<- LeafTime, errChan chan<- error, opts HammerOpts) *Hammer {
	readThrottle := NewThrottle(opts.MaxReadOpsPerSecond)
	writeThrottle := NewThrottle(opts.MaxWriteOpsPerSecond)

	randomReaders := NewWorkerPool(func() Worker {
		return NewLeafReader(tracker, f, RandomNextLeaf(), readThrottle.TokenChan, errChan)
	})
	fullReaders := NewWorkerPool(func() Worker {
		return NewLeafReader(tracker, f, MonotonicallyIncreasingNextLeaf(), readThrottle.TokenChan, errChan)
	})
	writers := NewWorkerPool(func() Worker {
		return NewLogWriter(w, gen, writeThrottle.TokenChan, errChan, seqLeafChan)
	})

	return &Hammer{
		opts:          opts,
		randomReaders: randomReaders,
		fullReaders:   fullReaders,
		writers:       writers,
		readThrottle:  readThrottle,
		writeThrottle: writeThrottle,
		tracker:       tracker,
	}
}

// Hammer is responsible for coordinating the operations against the log in the form
// of write and read operations. The work of analysing the results of hammering should
// live outside of this class.
type Hammer struct {
	opts          HammerOpts
	randomReaders WorkerPool
	fullReaders   WorkerPool
	writers       WorkerPool
	readThrottle  *Throttle
	writeThrottle *Throttle
	tracker       *client.LogStateTracker
}

func (h *Hammer) Run(ctx context.Context) {
	// Kick off readers & writers
	for range h.opts.NumReadersRandom {
		h.randomReaders.Grow(ctx)
	}
	for range h.opts.NumReadersFull {
		h.fullReaders.Grow(ctx)
	}
	for range h.opts.NumWriters {
		h.writers.Grow(ctx)
	}

	go h.readThrottle.Run(ctx)
	go h.writeThrottle.Run(ctx)

	go h.updateCheckpointLoop(ctx)
}

func (h *Hammer) updateCheckpointLoop(ctx context.Context) {
	tick := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			size := h.tracker.Latest().Size
			_, _, _, err := h.tracker.Update(ctx)
			if err != nil {
				slog.WarnContext(ctx, "Failed to update LogStateTracker", slog.Any("error", err))
				inconsistentErr := client.ErrInconsistency{}
				if errors.As(err, &inconsistentErr) {
					slog.ErrorContext(ctx, fmt.Sprintf("Last Good Checkpoint:\n%s\n\nFirst Bad Checkpoint:\n%s\n\n%v", string(inconsistentErr.SmallerRaw), string(inconsistentErr.LargerRaw), inconsistentErr), slog.Any("error", inconsistentErr))
					os.Exit(1)
				}
			}
			newSize := h.tracker.Latest().Size
			if newSize > size {
				slog.DebugContext(ctx, "Updated checkpoint", slog.Uint64("from", size), slog.Uint64("to", newSize))
			} else {
				slog.DebugContext(ctx, "Checkpoint size unchanged", slog.Uint64("size", newSize))
			}
		}
	}
}
