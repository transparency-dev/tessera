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
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/transparency-dev/tessera"
)

var (
	// ErrUnknownLog is returned when a request is made for an unknown log origin.
	ErrUnknownLog = errors.New("unknown log origin")
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
	// AddEntries adds provided entries into its tree after verifying subtree consistency proofs.
	AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (nextIdx uint64, curSize uint64, newTicket []byte, cosigs []byte, err error)
}
