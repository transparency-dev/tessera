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
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"

	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/parse"
	"golang.org/x/mod/sumdb/note"
)

var (
	// ErrConflict is returned when the requested upload range conflicts with the
	// current state of the log.
	ErrConflict = errors.New("tree size conflict")
	// ErrNoPendingCheckpoint is returned when a pending checkpoint cannot be
	// determined.
	ErrNoPendingCheckpoint = errors.New("no pending checkpoint")
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
// Returns the next required entry index, a recent pending checkpoint size, an opaque
// ticket for future invocations, and, optionally, a cosignature over a pending checkpoint
// whose size matches uploadEnd if one exists.
func (mt *MirrorTarget) AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticketBytes []byte, next func() (*MirrorPackage, error)) (nextEntry uint64, pendingSize uint64, newTicket []byte, cosigs []byte, err error) {
	curIntegratedSize, err := mt.reader.IntegratedSize(ctx)
	if err != nil {
		return 0, 0, nil, nil, fmt.Errorf("failed to read integrated size: %w", err)
	}
	var t *ticket
	if t, err = mt.openTicket(ctx, ticketBytes); err != nil {
		// Invalid or empty ticket, return a new one.
		pendingCP, err := mt.cpSource(ctx)
		if err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to get pending checkpoint: %v", err)
		}
		if len(pendingCP) == 0 {
			return 0, 0, nil, nil, ErrNoPendingCheckpoint
		}
		t = &ticket{
			PendingCP: pendingCP,
		}
		ticketBytes, err = mt.sealTicket(ctx, t)
		if err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to create ticket: %v", err)
		}

		// If the client didn't provide a [valid] ticket, then we don't have a pending
		// checkpoint to validate against, so we return a new ticket with the
		// current checkpoint.
		_, pendingSize, _, err := parse.CheckpointUnsafe(t.PendingCP)
		if err != nil {
			slog.ErrorContext(ctx, "Invalid pending checkpoint from source", slog.String("pending_checkpoint", string(t.PendingCP)), slog.String("error", err.Error()))
			return 0, 0, nil, nil, fmt.Errorf("failed to parse pending checkpoint while creating ticket: %v", err)
		}
		return curIntegratedSize, pendingSize, ticketBytes, nil, ErrConflict
	}

	var pendingRoot []byte
	_, pendingSize, pendingRoot, err = parse.CheckpointUnsafe(t.PendingCP)
	if err != nil {
		slog.ErrorContext(ctx, "Invalid pending checkpoint in ticket", slog.String("pending_checkpoint", string(t.PendingCP)), slog.String("error", err.Error()))
		return 0, 0, nil, nil, fmt.Errorf("failed to parse pending checkpoint from ticket: %v", err)
	}

	// Handle 409 Conflicts:
	//    - Zero-request check: If upload_start == 0 and upload_end == 0, the client is
	//      requesting initial mirror information.
	//    - upload_end:
	//      * MUST be equal to the tree size of a known pending checkpoint value.
	//      * MUST NOT be less than the mirror checkpoint's tree size.
	//    - upload_start:
	//      * MUST NOT be greater than the mirror's next expected entry index.
	//      * MUST NOT be too far below the mirror's next entry index.
	if (uploadStart == 0 && uploadEnd == 0) ||
		(uploadEnd != pendingSize || uploadEnd < curIntegratedSize) ||
		(uploadStart > curIntegratedSize) {
		// TODO(al): add flexibility about re-writing some entries
		return curIntegratedSize, pendingSize, ticketBytes, nil, ErrConflict
	}

	bi := func(yield func(api.EntryBundle) bool) {
		for {
			pkg, err := next()
			if err != nil {
				if err == io.EOF {
					return
				}
				// TODO(al): handle this
				slog.WarnContext(ctx, "NextPackage returned an error", slog.String("error", err.Error()))
				return
			}

			// TODO(al): verify entries+proof under checkpoint (Failure -> 422 Unprocessable Entity).

			if !yield(api.EntryBundle{Entries: pkg.Entries}) {
				return
			}
		}
	}

	// TODO(al): Check uploadStart is aligned to EntryBundleWidth.
	bundleIdx := uploadStart / layout.EntryBundleWidth

	nextEntry, newRoot, err := mt.writer.IntegrateBundles(ctx, bundleIdx, bi)
	switch {
	case err != nil:
		return 0, 0, nil, nil, err
	case nextEntry == pendingSize:
		if !bytes.Equal(pendingRoot, newRoot) {
			slog.ErrorContext(ctx, "CORRUPTION DETECTED - pending root != calculated root", slog.String("calculated_root", hex.EncodeToString(newRoot)), slog.String("pending_checkpoint", string(t.PendingCP)))
			return 0, 0, nil, nil, errors.New("internal error")
		}
		// This is a complete upload.
		// TODO(al):
		// 		- cosign the pending checkpoint,
		// 		- publish it IFF we not overwriting a larger checkpoint
		// 		- If published, then return the cosig(s) to the caller.
		return nextEntry, pendingSize, nil, []byte("— test cosig\n"), nil
	case nextEntry > pendingSize:
		// TODO(al): ticket is stale, probably need to update the ticket?
		slog.WarnContext(ctx, "nextEntry > pendingSize", slog.Uint64("nextEntry", nextEntry), slog.Uint64("pendingSize", pendingSize))
		return nextEntry, pendingSize, ticketBytes, nil, nil
	default:
		// Incomplete upload, return an updated ticket with the current checkpoint.
		return nextEntry, pendingSize, ticketBytes, nil, nil
	}
}

// IntegratedSize returns the size of the current integrated log.
func (mt *MirrorTarget) IntegratedSize(ctx context.Context) (uint64, error) {
	return mt.reader.IntegratedSize(ctx)
}

// ticket is the underlying structure of an add-entries ticket.
type ticket struct {
	// PendingCP holds the raw pending checkpoint bytes.
	PendingCP []byte
}

func (mt *MirrorTarget) sealTicket(ctx context.Context, t *ticket) ([]byte, error) {
	out := bytes.Buffer{}
	if err := gob.NewEncoder(&out).Encode(t); err != nil {
		return nil, fmt.Errorf("ticket encoding failed: %v", err)
	}
	// TODO(al): harden ticket & bind to this particular log mirror.
	return out.Bytes(), nil
}

func (mt *MirrorTarget) openTicket(ctx context.Context, ticketBytes []byte) (*ticket, error) {
	if len(ticketBytes) == 0 {
		return nil, errors.New("empty ticket")
	}
	// TODO(al): harden ticket & verify it's for this particular log mirror.
	var t ticket
	if err := gob.NewDecoder(bytes.NewReader(ticketBytes)).Decode(&t); err != nil {
		return nil, fmt.Errorf("ticket decoding failed: %v", err)
	}
	return &t, nil
}
