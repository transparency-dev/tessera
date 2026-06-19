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
	"fmt"
	"sync"
	"time"
)

func NewThrottle(opsPerSecond int) *Throttle {
	return &Throttle{
		opsPerSecond: opsPerSecond,
		TokenChan:    make(chan bool, opsPerSecond),
	}
}

type Throttle struct {
	TokenChan    chan bool
	mu           sync.Mutex
	opsPerSecond int

	oversupply int
}

func (t *Throttle) Increase() {
	t.mu.Lock()
	defer t.mu.Unlock()
	tokenCount := t.opsPerSecond
	delta := max(float64(tokenCount)*0.1, 1)
	t.opsPerSecond = tokenCount + int(delta)
}

func (t *Throttle) Decrease() {
	t.mu.Lock()
	defer t.mu.Unlock()
	tokenCount := t.opsPerSecond
	if tokenCount <= 1 {
		return
	}
	delta := max(float64(tokenCount)*0.1, 1)
	t.opsPerSecond = tokenCount - int(delta)
}

func (t *Throttle) Run(ctx context.Context) {
	interval := time.Second
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done(): //context cancelled
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(ctx, interval)
			t.supplyTokens(ctx)
			cancel()
		}
	}
}

func (t *Throttle) supplyTokens(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tokenCount := t.opsPerSecond
	for range t.opsPerSecond {
		select {
		case t.TokenChan <- true:
			tokenCount--
		case <-ctx.Done():
			t.oversupply = tokenCount
			return
		}
	}
	t.oversupply = 0
}

func (t *Throttle) String() string {
	return fmt.Sprintf("Current max: %d/s. Oversupply in last second: %d", t.opsPerSecond, t.oversupply)
}
