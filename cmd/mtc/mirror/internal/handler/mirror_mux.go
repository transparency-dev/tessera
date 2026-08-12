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

package handler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/internal/parse"
	"github.com/transparency-dev/witness/witness"
)

var (
	// ErrUnknownLog is returned when a requested log is unknown to the mirror
	ErrUnknownLog = witness.ErrUnknownLog
)

// NewMirrorMux creates a new MirrorMux from the provided map of origins to mirror targets.
func NewMirrorMux() *MirrorMux {
	return &MirrorMux{
		targets: make(map[string]MirrorTarget),
	}
}

// AddTarget adds a new mirror target for the given origin.
// It is an error to add a target for an origin that already has been added.
func (m *MirrorMux) AddTarget(origin string, t MirrorTarget) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.targets[origin]; ok {
		return fmt.Errorf("origin %q already added", origin)
	}
	m.targets[origin] = t
	return nil
}

// MirrorMux is a backend for the tlog-mirror HTTP service that multiplexes incoming requests
// over a set of target mirrors based on the log origin.
type MirrorMux struct {
	mu      sync.RWMutex
	targets map[string]MirrorTarget // keyed by log origin.
}

func (m *MirrorMux) AddEntries(ctx context.Context, origin string, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (nextIdx uint64, curSize uint64, newTicket []byte, cosigs []byte, err error) {
	t, err := m.target(origin)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	slog.InfoContext(ctx, "AddEntries", slog.String("origin", origin), slog.Uint64("start", uploadStart), slog.Uint64("end", uploadEnd))

	return t.AddEntries(ctx, uploadStart, uploadEnd, ticket, next)
}

func (m *MirrorMux) SignSubtree(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
	// Need to crack open the checkpoint to figure out the origin so we know which mux target to send it to.
	// This is safe, though, because the target's SignSubtree will open the checkpoint properly, verifying its
	// signature and asserting the origin is correct.
	cpOrigin, _, _, err := parse.CheckpointUnsafe(cp)
	if err != nil {
		return nil, err
	}

	t, err := m.target(cpOrigin)
	if err != nil {
		return nil, ErrUnknownLog
	}

	slog.InfoContext(ctx, "SignSubtree", slog.String("origin", cpOrigin))
	return t.SignSubtree(ctx, start, end, subRoot, proof, cp)
}

// target returns the target for the given origin, or ErrUnknownLog if it doesn't exist.
func (m *MirrorMux) target(origin string) (MirrorTarget, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.targets[origin]
	if !ok {
		return nil, ErrUnknownLog
	}
	return r, nil
}

// MirrorTarget describes the contract that a mirror target must satisfy.
type MirrorTarget interface {
	// AddEntries adds verified consistent entries to the mirror.
	AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (nextIdx uint64, curSize uint64, newTicket []byte, cosigs []byte, err error)
	// SignSubtree should verify that:
	// - The provided checkpoint originates from a known log, is valid, and is counter-signed by the same key which will cosign the subtree.
	// - The provided subtree and proof are valid for the provided checkpoint.
	// If all checks pass, it should return a cosignature for the subtree, or an error.
	SignSubtree(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error)
}
