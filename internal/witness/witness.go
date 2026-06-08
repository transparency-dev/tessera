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

package witness

import (
	"context"
	"errors"
)

// Persistence is the contract for a storage implementation used by a witness.
type Persistence interface {
	// UpdateCheckpoint atomically updates the latest checkpoint, given the original.
	// The provided function should be called by the storage with the current latest checkpoint, and will return
	// an updated checkpoint to be stored in its place.
	// Implementations MUST ensure this operation is atomic and thread-safe.
	UpdateCheckpoint(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error
}

// Witness is an implementation of the core logic of a tlog-witness.
type Witness struct {
	storage Persistence
}

func New(s Persistence) *Witness {
	return &Witness{storage: s}
}

func (w *Witness) Update(ctx context.Context, oldSize uint64, nextRaw []byte, cProof [][]byte) ([]byte, uint64, error) {
	return nil, 0, errors.New("unimplemented")
}
