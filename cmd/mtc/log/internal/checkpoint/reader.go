// Copyright 2026 The Tessera authors. All Rights Reserved.
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

// Package checkpoint provides helpers for reading checkpoint and caching sizes.
package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"

	flog "github.com/transparency-dev/formats/log"
)

// Reader wraps a ReadCheckpoint function to cache the latest checkpoint size observed from storage.
type Reader struct {
	readCheckpoint func(context.Context) ([]byte, error)

	mu         sync.RWMutex
	latestSize uint64
}

// NewReader creates a new Reader instance wrapping readCheckpoint and reads the initial checkpoint from storage.
func NewReader(ctx context.Context, readCheckpoint func(context.Context) ([]byte, error)) (*Reader, error) {
	if readCheckpoint == nil {
		return nil, errors.New("readCheckpoint must not be nil")
	}
	r := &Reader{
		readCheckpoint: readCheckpoint,
	}
	if _, err := r.Checkpoint(ctx); err != nil {
		return nil, fmt.Errorf("initial checkpoint read: %v", err)
	}
	return r, nil
}

// Checkpoint reads the latest checkpoint from underlying storage and updates the latest observed size.
func (r *Reader) Checkpoint(ctx context.Context) ([]byte, error) {
	rawCp, err := r.readCheckpoint(ctx)
	if err != nil {
		return nil, err
	}

	var cp flog.Checkpoint
	if _, err := cp.Unmarshal(rawCp); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %v", err)
	}

	r.mu.Lock()
	if cp.Size > r.latestSize {
		r.latestSize = cp.Size
	}
	r.mu.Unlock()

	return rawCp, nil
}

// LatestSize returns the most recently observed checkpoint size.
func (r *Reader) LatestSize(_ context.Context) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.latestSize, nil
}
