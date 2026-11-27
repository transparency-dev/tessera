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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHammerAnalyser_Stats(t *testing.T) {
	ctx := t.Context()

	var treeSize treeSizeState
	ha := NewHammerAnalyser(treeSize.getSize)

	go ha.updateStatsLoop(ctx)

	time.Sleep(100 * time.Millisecond)

	baseTime := time.Now().Add(-1 * time.Minute)
	for i := range 10 {
		ha.SeqLeafChan <- LeafTime{
			Index:      uint64(i),
			QueuedAt:   baseTime,
			AssignedAt: baseTime.Add(time.Duration(i) * time.Second),
		}
	}
	treeSize.setSize(10)
	time.Sleep(500 * time.Millisecond)

	avg := ha.QueueTime.Avg()
	if want := float64(4500); avg != want {
		t.Errorf("integration time avg: got != want (%f != %f)", avg, want)
	}
}

type treeSizeState struct {
	size uint64
	mux  sync.RWMutex
}

func (s *treeSizeState) getSize() uint64 {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return s.size
}

func (s *treeSizeState) setSize(size uint64) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.size = size
}

// TestHammerAnalyser_ReportsEveryError checks that errorLoop logs each error
// it receives, including one whose message differs from the error before it.
func TestHammerAnalyser_ReportsEveryError(t *testing.T) {
	var out lockedBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ha := NewHammerAnalyser(func() uint64 { return 0 })
	go ha.errorLoop(ctx)
	ha.ErrChan <- errors.New("first error")
	ha.ErrChan <- errors.New("second error")

	// errorLoop reports what it has gathered once a second.
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if s := out.String(); strings.Contains(s, "first error") && strings.Contains(s, "second error") {
			return
		}
	}
	t.Fatalf("want both errors reported, got:\n%s", out.String())
}

// lockedBuffer is a bytes.Buffer which is safe for concurrent use.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
