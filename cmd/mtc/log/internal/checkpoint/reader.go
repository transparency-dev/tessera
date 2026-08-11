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

	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/tessera/internal/parse"
)

// maxSubtrees is the maximum number of recent subtrees kept in memory.
// Retaining 16 subtrees covers 8+ checkpoint publication cycles, which is
// enough to cover ongoing AddTBS requests.
const maxSubtrees = 16

// subtree represents a contiguous range of log entries [start, end).
type subtree struct {
	start uint64
	end   uint64
}

// Reader wraps a ReadCheckpoint function to maintain an in-memory sliding window
// of the most recently published subtrees.
type Reader struct {
	readCheckpoint func(context.Context) ([]byte, error)

	mu sync.RWMutex
	// subtrees stores the decomposition of successive [last_ckpt_size, new_ckpt_size)
	// ranges into subtrees as per draft-ietf-plants-merkle-tree-certs Section 4.1.
	// The `end` field of the last element is the latest tree size.
	subtrees []subtree
}

// NewReader creates a new Reader instance wrapping readCheckpoint and reads the initial checkpoint from storage.
func NewReader(ctx context.Context, readCheckpoint func(context.Context) ([]byte, error)) (*Reader, error) {
	if readCheckpoint == nil {
		return nil, errors.New("readCheckpoint must not be nil")
	}
	r := &Reader{
		readCheckpoint: readCheckpoint,
	}
	// Populate subtrees with the two subtrees covering [0, checkpoint_size).
	if _, err := r.Checkpoint(ctx); err != nil {
		return nil, fmt.Errorf("initial checkpoint read: %v", err)
	}
	return r, nil
}

// Checkpoint reads the latest checkpoint from storage and store corresponding subtrees.
func (r *Reader) Checkpoint(ctx context.Context) ([]byte, error) {
	rawCp, err := r.readCheckpoint(ctx)
	if err != nil {
		return nil, err
	}

	_, size, _, err := parse.CheckpointUnsafe(rawCp)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint: %v", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	lastSize := uint64(0)
	if len(r.subtrees) > 0 {
		lastSize = r.subtrees[len(r.subtrees)-1].end
	}

	if size > lastSize {
		s, mid, e, err := proof.FindSubtrees(lastSize, size)
		if err != nil {
			return nil, fmt.Errorf("find subtrees for [%d, %d): %w", lastSize, size, err)
		}
		if s < mid {
			r.pushSubtreeLocked(subtree{start: s, end: mid})
		}
		if mid < e {
			r.pushSubtreeLocked(subtree{start: mid, end: e})
		}
	}

	return rawCp, nil
}

// pushSubtreeLocked adds a subtree and potentially removes old ones, capping the number of elements.
// r.mu MUST be locked when calling this method.
func (r *Reader) pushSubtreeLocked(st subtree) {
	if len(r.subtrees) >= maxSubtrees {
		copy(r.subtrees, r.subtrees[1:])
		r.subtrees[len(r.subtrees)-1] = st
	} else {
		r.subtrees = append(r.subtrees, st)
	}
}

// LatestSize returns the most recently observed checkpoint size.
func (r *Reader) LatestSize() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.subtrees) == 0 {
		return 0
	}
	return r.subtrees[len(r.subtrees)-1].end
}

// SubtreeForIndex returns the exact [start, end) subtree covering index (start <= index < end).
// If index precedes the earliest retained subtree, it returns [0, subtrees[0].start).
func (r *Reader) SubtreeForIndex(index uint64) (start, end uint64, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.subtrees) == 0 || index >= r.subtrees[len(r.subtrees)-1].end {
		return 0, 0, false
	}

	// Search backwards from the end because index is almost always in the most
	// recently added subtree (e.g. during AddTBS).
	for i := len(r.subtrees) - 1; i >= 0; i-- {
		st := r.subtrees[i]
		if index >= st.start && index < st.end {
			return st.start, st.end, true
		}
	}

	if index < r.subtrees[0].start {
		return 0, r.subtrees[0].start, true
	}

	return 0, 0, false
}
