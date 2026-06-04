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
	"errors"
	"io"
	"log/slog"
	"sync"
)

var (
	// ErrUnknownLog is returned when a requested log is unknown to the mirror.
	ErrUnknownLog = errors.New("unknown log origin")
)

// Package represents a single package of entries and its subtree consistency proof.
type Package struct {
	Entries [][]byte
	Proof   [][]byte
}

// New creates a new Mirror.
//
// TODO(al): Add some way to configure and create targets.
func New() *Mirror {
	return &Mirror{
		targets: make(map[string]*target),
	}
}

// Mirror is the backend for the tlog-mirror HTTP service.
//
// Mirror is mostly a multiplexer over the various log targets. It knows about
// the configured logs and routes requests to the appropriate target.
type Mirror struct {
	lock    sync.RWMutex
	targets map[string]*target
}

func (m *Mirror) AddCheckpoint(ctx context.Context, origin string, oldSize uint64, proof [][]byte, cpRaw []byte) error {
	t, err := m.target(origin)
	if err != nil {
		return err
	}
	return t.AddCheckpoint(ctx, oldSize, proof, cpRaw)
}

func (m *Mirror) AddEntries(ctx context.Context, origin string, uploadStart, uploadEnd uint64, ticket []byte, next func() (*Package, error)) ([]byte, error) {
	t, err := m.target(origin)
	if err != nil {
		return nil, err
	}
	return t.AddEntries(ctx, uploadStart, uploadEnd, ticket, next)
}

// target returns the target for the given origin, or ErrUnknownLog if it doesn't exist.
func (m *Mirror) target(origin string) (*target, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	r, ok := m.targets[origin]
	if !ok {
		return nil, ErrUnknownLog
	}
	return r, nil
}

// target represents a single log that the mirror is configured to maintain.
type target struct {
	origin string
	// TODO: MirrorLifecycle
}


func (t *target) AddCheckpoint(ctx context.Context, oldSize uint64, proof [][]byte, cpRaw []byte) error {
	slog.InfoContext(ctx, "AddCheckpoint", slog.String("origin", t.origin), slog.Uint64("old_size", oldSize), slog.Int("proof_len", len(proof)), slog.String("cp", string(cpRaw)))
	return nil
}

func (t *target) AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*Package, error)) ([]byte, error) {
	slog.InfoContext(ctx, "AddEntries", slog.String("origin", t.origin), slog.Uint64("start", uploadStart), slog.Uint64("end", uploadEnd))

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
