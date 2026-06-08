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
	"log/slog"
	"maps"
	"sync"

	"github.com/transparency-dev/tessera"
)

var (
	// ErrUnknownLog is returned when a requested log is unknown to the mirror.
	ErrUnknownLog = errors.New("unknown log origin")
)

// New creates a new Mirror from the provided map of origins to mirror target.
func New(targets map[string]Target) *Mirror {
	m := &Mirror{
		targets: maps.Clone(targets),
	}
	return m
}

// Mirror is the backend for the tlog-mirror HTTP service.
//
// Mirror is mostly a multiplexer over the various log targets. It knows about
// the configured logs and routes requests to the appropriate target.
type Mirror struct {
	lock    sync.RWMutex
	targets map[string]Target
}

func (m *Mirror) AddCheckpoint(ctx context.Context, origin string, oldSize uint64, proof [][]byte, cpRaw []byte) ([]byte, uint64, error) {
	t, err := m.target(origin)
	if err != nil {
		return nil, 0, err
	}
	slog.InfoContext(ctx, "AddCheckpoint", slog.String("origin", origin), slog.Uint64("old_size", oldSize), slog.Int("proof_len", len(proof)), slog.String("cp", string(cpRaw)))
	return t.AddCheckpoint(ctx, oldSize, proof, cpRaw)
}

func (m *Mirror) AddEntries(ctx context.Context, origin string, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) ([]byte, error) {
	t, err := m.target(origin)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "AddEntries", slog.String("origin", origin), slog.Uint64("start", uploadStart), slog.Uint64("end", uploadEnd))
	return t.AddEntries(ctx, uploadStart, uploadEnd, ticket, next)
}

// target returns the target for the given origin, or ErrUnknownLog if it doesn't exist.
func (m *Mirror) target(origin string) (Target, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	r, ok := m.targets[origin]
	if !ok {
		return nil, ErrUnknownLog
	}
	return r, nil
}

// Target describes the contract that a mirror target must satisfy.
type Target interface {
	// AddCheckpoint is a tlog-witness.
	AddCheckpoint(ctx context.Context, oldSize uint64, proof [][]byte, cpRaw []byte) ([]byte, uint64, error)
	// AddEntries adds verified consistent entries to the mirror.
	AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) ([]byte, error)
}