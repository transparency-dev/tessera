// Copyright 2021 Google LLC. All Rights Reserved.
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
//

// This file contains an implementation of the tlog-witness server logic.
// Large parts of this come from github.com/transparency-dev/witness.

package witness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/mod/sumdb/note"

	iotel "github.com/transparency-dev/tessera/internal/otel"
)

var (
	// ErrNoValidSignature is returned by calls to Update if the provided checkpoint has no valid signature by the expected key.
	ErrNoValidSignature = errors.New("no valid signatures")
	// ErrUnknownLog is returned by calls to Update if the provided checkpoint carries an Origin which is unknown to the
	// witness.
	ErrUnknownLog = errors.New("unknown log")
	// ErrOldSizeInvalid is returned by calls to Update if the provided oldSize parameter is larger than the size of the
	// submitted checkpoint.
	ErrOldSizeInvalid = errors.New("old size > current")
	// ErrCheckpointStale is returned by calls to Update if the oldSize parameter does not match the size of the currently
	// stored checkpoint for the same log.
	ErrCheckpointStale = errors.New("old size != current")
	// ErrInvalidProof is returned by calls to Update if the provided consistency proof is invalid.
	ErrInvalidProof = errors.New("consistency proof invalid")
	// ErrRootMismatch is returned by calls to Update if the provided checkpoint is for the same size tree as the currently
	// stored one, but their root hashes differ.
	ErrRootMismatch = errors.New("roots do not match")
)

// Persistence is the storage contract for a witness's checkpoints.
type Persistence interface {
	// UpdateCheckpoint must atomically call the provided update func with the witness' current latest checkpoint for the log, and update
	// that view with the checkpoint the function returns.
	UpdateCheckpoint(ctx context.Context, update func(oldCP []byte) (newCP []byte, err error)) error
}

// Opts is the options passed to a witness.
type Opts struct {
	Persistence Persistence
	Signers     []note.Signer
	LogVerifier note.Verifier
}

// Witness is a witness for a single log.
type Witness struct {
	p         	Persistence
	signers     []note.Signer
	logVerifier note.Verifier
}

// New creates a new witness for the configured log.
func New(ctx context.Context, wo Opts) (*Witness, error) {
	return &Witness{
		p:         wo.Persistence,
		signers:     wo.Signers,
		logVerifier: wo.LogVerifier,
	}, nil
}

// verifyCheckpoint verifies the checkpoint under the configured key & origin and returns
// the parsed checkpoint and the note itself.
func (w *Witness) verifyCheckpoint(ctx context.Context, chkptRaw []byte) (*log.Checkpoint, *note.Note, error) {
	cp, _, n, err := log.ParseCheckpoint(chkptRaw, w.logVerifier.Name(), w.logVerifier)
	return cp, n, err
}

// Update updates the latest checkpoint if nextRaw is consistent with the current
// latest one for this log.
//
// The values returned depend on whether or not the new checkpoint is accepted, and
// if not, the reason it was rejected. This can be determined through the error:
//
// - no error: The checkpoint was accepted, and a serialised note-signature is returned.
// - ErrCheckpointStale or ErrOldSizeInvalid: the presented checkpoint is out of date, the size of the current checkpoint is returned.
// - Any other error, no supporting values are returned.
func (w *Witness) Update(ctx context.Context, oldSize uint64, nextRaw []byte, cProof [][]byte) ([]byte, uint64, error) {
	return iotel.Trace2(ctx, "tessera.witness.server.update", tracer, func(ctx context.Context, span trace.Span) ([]byte, uint64, error) {
		// Check the signatures on the raw checkpoint and parse it
		// into the log.Checkpoint format.
		//
		// SPEC: The witness MUST verify the checkpoint signature against the public key(s) it trusts for the
		//       checkpoint origin, and it MUST ignore signatures from unknown keys.
		next, nextNote, err := w.verifyCheckpoint(ctx, nextRaw)
		if err != nil {
			return nil, 0, err
		}

		var retSigs []byte
		var retSize uint64

		// Attempt to atomically update the latest known checkpoint for the given origin.
		err = w.p.UpdateCheckpoint(ctx, func(prevRaw []byte) ([]byte, error) {
			// If there was nothing stored already then treat this new
			// checkpoint as trust-on-first-use (TOFU).
			if prevRaw == nil {
				// Store a witness cosigned version of the checkpoint.
				signed, sigs, err := w.signChkpt(nextNote)
				if err != nil {
					return nil, fmt.Errorf("couldn't sign input checkpoint: %v", err)
				}
				retSigs = sigs
				return signed, nil
			}

			prev, _, err := w.verifyCheckpoint(ctx, prevRaw)
			if err != nil {
				retSize, retSigs = 0, nil
				return nil, fmt.Errorf("couldn't parse stored checkpoint: %v", err)
			}

			switch {
			case oldSize > next.Size:
				// SPEC: The old size MUST be equal to or lower than the (submitted) checkpoint size.
				retSize, retSigs = prev.Size, nil
				return nil, ErrOldSizeInvalid

			case oldSize != prev.Size:
				// SPEC: The witness MUST check that the old size matches the size of the latest checkpoint it cosigned
				//       for the checkpoint's origin (or zero if it never cosigned a checkpoint for that origin)
				retSize, retSigs = prev.Size, nil
				return nil, fmt.Errorf("%w (%d != %d)", ErrCheckpointStale, oldSize, prev.Size)

			case next.Size == prev.Size:
				// SPEC:  If the old size matches the checkpoint size, the witness MUST check that the root hashes are
				//        also identical.
				if !bytes.Equal(next.Hash, prev.Hash) {
					slog.ErrorContext(ctx, "INCONSISTENT CHECKPOINTS", slog.String("origin", w.logVerifier.Name()), slog.Any("prev", prev), slog.Any("next", next))
					retSize, retSigs = 0, nil
					return nil, ErrRootMismatch
				}
				// We'll continue on to signing the checkpoints below, so the log gets a fresh cosignature with updated timestamp.

			case prev.Size == 0:
				// SPEC:  The proof MUST be empty if the old size is zero.
				//
				// Checkpoints of size 0 are really placeholders and consistency proofs can't be performed.
				// If we initialized on a tree size of 0, then we simply ratchet forward and effectively TOFU the new checkpoint.
				if len(cProof) > 0 {
					retSize, retSigs = 0, nil
					return nil, ErrInvalidProof
				}

			case next.Size > prev.Size:
				// The only remaining option is next.Size > prev.Size. This might be valid so we verify the consistency proof.
				if err := proof.VerifyConsistency(rfc6962.DefaultHasher, prev.Size, next.Size, cProof, prev.Hash, next.Hash); err != nil {
					// Complain if the checkpoints aren't consistent.
					return nil, ErrInvalidProof
				}

			default:
				// This should never occur, but if it does, fail safe.
				slog.ErrorContext(ctx, "unexpected state in Update", slog.String("origin", w.logVerifier.Name()), slog.Any("prev", prev), slog.Any("next", next))
				return nil, fmt.Errorf("unexpected state: prev.Size=%d, next.Size=%d", prev.Size, next.Size)
			}

			// All checks complete, we're satisfied with the new checkpoint and will sign it.
			signed, sigs, err := w.signChkpt(nextNote)
			if err != nil {
				retSize, retSigs = 0, nil
				return nil, fmt.Errorf("couldn't sign input checkpoint: %v", err)
			}

			retSize, retSigs = next.Size, sigs
			return signed, nil
		})
		return retSigs, retSize, err
	})
}

// signChkpt adds the witness' signature to a checkpoint.
//
// Returns:
// - A serialised signed note including new witness signatures.
// - A serialised representation of just the witness signature line(s).
func (w *Witness) signChkpt(n *note.Note) ([]byte, []byte, error) {
	// Code below is a lightly tweaked snippet from sumdb/note/note.go
	// https://cs.opensource.google/go/x/mod/+/refs/tags/v0.24.0:sumdb/note/note.go;l=625-649

	// Prepare signatures.
	//
	// We need to return both a full serialised signed note, as well as the just the
	// signature lines we're adding - this is because we want to _store_ the full note, but
	// the tlog-witness API requires that we only return the signature lines.
	//
	// Rather than using note.Sign, then running note.Open in order to get access to our
	// signatures, we'll instead use our note.Signer(s) directly to sign the note message
	// and then use the returned signature bytes to create both the serialised signed note
	// as well as the serialised signature lines.

	var sigs = bytes.Buffer{}
	for _, s := range w.signers {
		name := s.Name()
		hash := s.KeyHash()
		if !isValidSignerName(name) {
			return nil, nil, errors.New("invalid signer")
		}

		sig, err := s.Sign([]byte(n.Text))
		if err != nil {
			return nil, nil, err
		}

		// Create serialised signature line and append it to our sigs buffer:
		var hbuf [4]byte
		binary.BigEndian.PutUint32(hbuf[:], hash)
		b64 := base64.StdEncoding.EncodeToString(append(hbuf[:], sig...))
		sigs.WriteString("— ")
		sigs.WriteString(name)
		sigs.WriteString(" ")
		sigs.WriteString(b64)
		sigs.WriteString("\n")

		// Also create a new note.Signature and pop it into the note's Sigs list (this will cause
		// the signature to be present in the output when we call note.Sign below.
		n.Sigs = append(n.Sigs, note.Signature{Name: name, Hash: hash, Base64: b64})
	}
	// Serialise the full signed note by calling Sign.
	// Note that we're not passing any signers here because we've already added signatures in the loop above, so
	// this call becomes just a serialisation function.
	signed, err := note.Sign(n)
	if err != nil {
		return nil, nil, err
	}

	return signed, sigs.Bytes(), nil
}

// isValidSignerName reports whether name is valid.
// It must be non-empty and not have any Unicode spaces or pluses.
func isValidSignerName(name string) bool {
	return name != "" && utf8.ValidString(name) && strings.IndexFunc(name, unicode.IsSpace) < 0 && !strings.Contains(name, "+")
}
