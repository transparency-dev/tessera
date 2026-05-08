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

package mirror

import (
	"context"
	"io"
	"log/slog"
)

// Mirror is a placeholder. TBD where this actually lives, and how much is inside a storage lifecycle.
type Mirror struct {
}

// Package represents a single package of entries and its subtree consistency proof.
type Package struct {
	Entries [][]byte
	Proof   [][]byte
}

func (m *Mirror) AddCheckpoint(ctx context.Context, oldSize uint64, proof [][]byte, cpRaw []byte) error {
	slog.InfoContext(ctx, "AddCheckpoint", slog.Uint64("old_size", oldSize), slog.Int("proof_len", len(proof)), slog.String("cp", string(cpRaw)))
	return nil
}

func (m *Mirror) AddEntries(ctx context.Context, logOrigin string, uploadStart, uploadEnd uint64, ticket []byte, next func() (*Package, error)) ([]byte, error) {
	slog.InfoContext(ctx, "AddEntries", slog.String("origin", logOrigin), slog.Uint64("start", uploadStart), slog.Uint64("end", uploadEnd))

	// Drain the packages
	for {
		pkg, err := next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		slog.InfoContext(ctx, "AddEntries received package", slog.Int("num_entries", len(pkg.Entries)), slog.Int("num_proof_hashes", len(pkg.Proof)))
	}

	// SPEC: "Success (200 OK): A sequence of one or more cosignature lines"
	// Return a dummy cosignature for now.
	return []byte("— mirror-dummy-cosignature\n"), nil
}
