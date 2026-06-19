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

package tessera

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"golang.org/x/mod/sumdb/note"
)

// MirrorOptions holds mirror lifecycle settings for all storage implementations.
type MirrorOptions struct {
	signer   note.Signer
	cpSource func(context.Context) ([]byte, error)
}

// NewMirrorOptions creates a new options struct with defaults.
func NewMirrorOptions() *MirrorOptions {
	return &MirrorOptions{}
}

// WithSigner configures the note.Signer to use when cosigning checkpoints.
func (o *MirrorOptions) WithSigner(s note.Signer) *MirrorOptions {
	o.signer = s
	return o
}

func (o *MirrorOptions) WithCheckpointSource(f func(context.Context) ([]byte, error)) *MirrorOptions {
	o.cpSource = f
	return o
}

// Signer returns the configured note.Signer.
func (o *MirrorOptions) Signer() note.Signer {
	return o.signer
}

func (o *MirrorOptions) EntriesPath() func(index uint64, partial uint8) string {
	return layout.EntriesPath
}

func (o *MirrorOptions) LeafHasher() func(bundle []byte) (leafHashes [][]byte, err error) {
	return defaultMerkleLeafHasher
}

func (o *MirrorOptions) valid() error {
	if o.signer == nil {
		return errors.New("invalid MirrorOptions: WithSigner must be set")
	}
	if o.cpSource == nil {
		return errors.New("invalid MirrorOptions: WithCheckpointSource must be set")
	}
	return nil
}

// mirrorWriter describes the contract for storage implementation required to support the mirroring lifecycle.
type MirrorWriter interface {
	// IntegrateBundles integrates bundles of log entries, starting at the given index, into the local tree.
	// Returns the size of the tree and its new root hash if successful.
	IntegrateBundles(ctx context.Context, from uint64, bundles iter.Seq[api.EntryBundle]) (uint64, []byte, error)
	// IntegratedSize returns the size of the local integrated tree.
	IntegratedSize(ctx context.Context) (uint64, error)
}

// MirrorTarget is a high-level wrapper that manages the process of mirroring
// a source log into a Tessera instance.
type MirrorTarget struct {
	writer   MirrorWriter
	reader   LogReader
	cpSource func(context.Context) ([]byte, error)
	signer   note.Signer
}

// NewMirrorTarget instantiates a new MirrorTarget for the given driver and options.
func NewMirrorTarget(ctx context.Context, d Driver, opts *MirrorOptions) (*MirrorTarget, error) {
	type mirrorLifecycle interface {
		MirrorWriter(context.Context, *MirrorOptions) (MirrorWriter, LogReader, error)
	}
	lc, ok := d.(mirrorLifecycle)
	if !ok {
		return nil, fmt.Errorf("driver %T does not implement MirrorTarget lifecycle", d)
	}
	if opts == nil {
		return nil, errors.New("opts cannot be nil")
	}
	if err := opts.valid(); err != nil {
		return nil, err
	}
	mw, r, err := lc.MirrorWriter(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to init MirrorTarget lifecycle: %v", err)
	}
	return &MirrorTarget{
		writer:   mw,
		reader:   r,
		cpSource: opts.cpSource,
		signer:   opts.signer,
	}, nil
}

// Package represents a single package of entries and its subtree consistency proof.
type MirrorPackage struct {
	Entries [][]byte
	Proof   [][]byte
}

// AddEntries processes a stream of entry packages, verifies subtree consistency proofs,
// and durably commits entries to the log.
//
// Returns the next required entry index, a recent pending checkpoint size, an opaque ticket for future invocations, and, optionally, a cosignature over a pending checkpoint whose size matches uploadEnd if one exists.
func (mt *MirrorTarget) AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*MirrorPackage, error)) (uint64, uint64, []byte, []byte, error) {
	return 0, 0, nil, nil, errors.New("unimplemented")
}

// IntegratedSize returns the size of the current integrated log.
func (mt *MirrorTarget) IntegratedSize(ctx context.Context) (uint64, error) {
	return mt.reader.IntegratedSize(ctx)
}
