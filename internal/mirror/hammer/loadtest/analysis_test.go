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
