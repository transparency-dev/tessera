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
	"time"

	"log/slog"

	movingaverage "github.com/RobinUS2/golang-moving-average"
)

func NewHammerAnalyser(treeSizeFn func() uint64) *HammerAnalyser {
	leafSampleChan := make(chan LeafTime, 100)
	errChan := make(chan error, 20)
	return &HammerAnalyser{
		treeSizeFn:      treeSizeFn,
		SeqLeafChan:     leafSampleChan,
		ErrChan:         errChan,
		IntegrationTime: movingaverage.Concurrent(movingaverage.New(30)),
		QueueTime:       movingaverage.Concurrent(movingaverage.New(30)),
	}
}

// HammerAnalyser is responsible for measuring and interpreting the result of hammering.
type HammerAnalyser struct {
	treeSizeFn  func() uint64
	SeqLeafChan chan LeafTime
	ErrChan     chan error

	QueueTime       *movingaverage.ConcurrentMovingAverage
	IntegrationTime *movingaverage.ConcurrentMovingAverage
}

func (a *HammerAnalyser) Run(ctx context.Context) {
	go a.updateStatsLoop(ctx)
	go a.errorLoop(ctx)
}

func (a *HammerAnalyser) updateStatsLoop(ctx context.Context) {
	tick := time.NewTicker(100 * time.Millisecond)
	size := a.treeSizeFn()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		newSize := a.treeSizeFn()
		if newSize <= size {
			continue
		}
		now := time.Now()
		totalLatency := time.Duration(0)
		queueLatency := time.Duration(0)
		numLeaves := 0
		var sample *LeafTime
	ReadLoop:
		for {
			if sample == nil {
				select {
				case l, ok := <-a.SeqLeafChan:
					if !ok {
						break ReadLoop
					}
					sample = &l
				default:
					break ReadLoop
				}
			}
			// Stop considering leaf times once we've caught up with that cross
			// either the current checkpoint or "now":
			// - leaves with indices beyond the tree size we're considering are not integrated yet, so we can't calculate their TTI
			// - leaves which were queued before "now", but not assigned by "now" should also be ignored as they don't fall into this epoch (and would contribute a -ve latency if they were included).
			if sample.Index >= newSize || sample.AssignedAt.After(now) {
				break
			}
			queueLatency += sample.AssignedAt.Sub(sample.QueuedAt)
			// totalLatency is skewed towards being higher than perhaps it may technically be by:
			// - the tick interval of this goroutine,
			// - the tick interval of the goroutine which updates the LogStateTracker,
			// - any latency in writes to the log becoming visible for reads.
			// But it's probably good enough for now.
			totalLatency += now.Sub(sample.QueuedAt)

			numLeaves++
			sample = nil
		}
		if numLeaves > 0 {
			a.IntegrationTime.Add(float64(totalLatency/time.Millisecond) / float64(numLeaves))
			a.QueueTime.Add(float64(queueLatency/time.Millisecond) / float64(numLeaves))
		}
	}
}

func (a *HammerAnalyser) errorLoop(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	pbCount := 0
	lastErr := ""
	lastErrCount := 0
	for {
		select {
		case <-ctx.Done(): //context cancelled
			return
		case <-tick.C:
			if pbCount > 0 {
				slog.Warn("received pushback from log", slog.Int("requests", pbCount))
				pbCount = 0
			}
			if lastErrCount > 0 {
				slog.Warn("errors", slog.Int("count", lastErrCount), slog.String("lasterror", lastErr))
				lastErrCount = 0
			}
		case err := <-a.ErrChan:
			if errors.Is(err, ErrRetry) {
				pbCount++
				continue
			}
			es := err.Error()
			if es != lastErr && lastErrCount > 0 {
				slog.Warn("errors", slog.Int("count", lastErrCount), slog.String("lasterror", lastErr))
				lastErr = es
				lastErrCount = 0
				continue
			}
			lastErr = es
			lastErrCount++
		}
	}
}
