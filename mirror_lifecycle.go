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
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
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
	signer      note.Signer
	cpSource    func(context.Context) ([]byte, error)
	logVerifier note.Verifier
}

// NewMirrorOptions creates a new options struct with defaults.
func NewMirrorOptions() *MirrorOptions {
	return &MirrorOptions{}
}

// WithLogVerifier sets the note.Verifier used to verify log checkpoint signatures.
func (o *MirrorOptions) WithLogVerifier(v note.Verifier) *MirrorOptions {
	o.logVerifier = v
	return o
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
	if o.logVerifier == nil {
		return errors.New("invalid MirrorOptions: WithLogVerifier must be set")
	}
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

	// UpdateCheckpoint MUST atomically update the local published checkpoint for the log mirror.
	// The provided function f should be called with the contents of the currently published checkpoint,
	// this will be of zero length if there is no currently published checkpoint, and should
	// return the new serialised checkpoint or an error. If the function returns an error, the currently
	// published checkpoint MUST NOT be altered.
	UpdateCheckpoint(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error
}

// MirrorTarget manages the process of mirroring a source log into a Tessera instance.
type MirrorTarget struct {
	writer      MirrorWriter
	reader      LogReader
	cpSource    func(context.Context) ([]byte, error)
	signer      note.Signer
	logVerifier note.Verifier
	ticketKey   []byte
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
	tK, err := ticketKey(opts.signer.Name(), opts.logVerifier.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to derive ticket key: %v", err)
	}
	return &MirrorTarget{
		writer:      mw,
		reader:      r,
		cpSource:    opts.cpSource,
		signer:      opts.signer,
		logVerifier: opts.logVerifier,
		ticketKey:   tK,
	}, nil
}

// ticketKey derives a unique HMAC key for sealing tickets based on:
//   - An ephemeral seed,
//   - Identity (origin) of the mirror cosigner,
//   - Identity (origin) of the log being mirrored.
//
// It should be called, once, at startup to set the ticket MAC key for the mirror.
//
// TODO(al): We should allow the operator to pass in the seed, so that tickets
// will work across multiple mirror instances and/or restarts.
func ticketKey(mirrorOrigin, logOrigin string) ([]byte, error) {
	seed := make([]byte, sha256.Size)
	if _, err := rand.Read(seed); err != nil {
		return nil, fmt.Errorf("failed to generate ephemeral seed: %v", err)
	}
	// This salt will keep the key unique per mirror, even if the random seed generation above
	// were changed to be a "fixed" value provided by the operator.
	salt := sha256.Sum256(fmt.Appendf(nil, "mirror:\n%s\n", mirrorOrigin))
	// Bind this key to its usage for MACing tickets for the given log.
	info := fmt.Sprintf("ticket-hmac\nlog:\n%s\n", logOrigin)
	return hkdf.Key(sha256.New, seed, salt[:], info, sha256.Size)
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
	// TODO(al) handle non-aligned uploadStart.

	curIntegratedSize, err := mt.reader.IntegratedSize(ctx)
	if err != nil {
		return 0, 0, nil, nil, fmt.Errorf("failed to read integrated size: %w", err)
	}
	ticketCP, err := mt.openTicket(ticketBytes)
	if err != nil {
		slog.DebugContext(ctx, "Invalid Ticket received, returning new one", slog.Any("error", err), slog.Uint64("uploadStart", uploadStart), slog.Uint64("uploadEnd", uploadEnd))

		// If the client didn't provide a [valid] ticket, then we don't have a pending
		// checkpoint to validate against, so we return a new ticket with the
		// current checkpoint.
		pendingCPRaw, err := mt.cpSource(ctx)
		if err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to get pending checkpoint: %v", err)
		}
		if len(pendingCPRaw) == 0 {
			return 0, 0, nil, nil, ErrNoPendingCheckpoint
		}
		ticketBytes, err = mt.sealTicket(pendingCPRaw)
		if err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to create ticket: %v", err)
		}

		pendingCP, _, _, err := log.ParseCheckpoint(pendingCPRaw, mt.logVerifier.Name(), mt.logVerifier)
		if err != nil {
			slog.ErrorContext(ctx, "Invalid pending checkpoint from source", slog.String("pending_checkpoint", string(pendingCPRaw)), slog.String("error", err.Error()))
			return 0, 0, nil, nil, fmt.Errorf("failed to parse pending checkpoint while creating ticket: %v", err)
		}

		slog.DebugContext(ctx, "Returning new ticket", slog.Uint64("curIntegratedSize", curIntegratedSize), slog.Uint64("pendingSize", pendingCP.Size))

		return curIntegratedSize, pendingCP.Size, ticketBytes, nil, ErrConflict
	}

	pendingCP, _, pendingNote, err := log.ParseCheckpoint(ticketCP, mt.logVerifier.Name(), mt.logVerifier)
	if err != nil {
		slog.ErrorContext(ctx, "Invalid pending checkpoint in ticket", slog.String("pending_checkpoint", string(ticketCP)), slog.String("error", err.Error()))
		return 0, 0, nil, nil, fmt.Errorf("failed to parse pending checkpoint from ticket: %v", err)
	}

	slog.DebugContext(ctx, "Valid ticket, proceeding", slog.Uint64("curIntegratedSize", curIntegratedSize), slog.Uint64("pendingSize", pendingCP.Size))
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
		(uploadEnd != pendingCP.Size || uploadEnd < curIntegratedSize) ||
		(uploadStart > curIntegratedSize) {
		// TODO(al): add flexibility about re-writing some entries
		slog.ErrorContext(ctx, "Returning conflict", slog.Uint64("curIntegratedSize", curIntegratedSize), slog.Uint64("pendingSize", pendingSize), slog.Uint64("uploadStart", uploadStart), slog.Uint64("uploadEnd", uploadEnd))
		return curIntegratedSize, pendingCP.Size, ticketBytes, nil, ErrConflict
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

	bundleIdx := uploadStart / layout.EntryBundleWidth

	nextEntry, newRoot, err := mt.writer.IntegrateBundles(ctx, bundleIdx, bi)
	switch {
	case err != nil:
		return 0, 0, nil, nil, err
	case nextEntry == pendingCP.Size:
		if !bytes.Equal(pendingCP.Hash, newRoot) {
			slog.ErrorContext(ctx, "CORRUPTION DETECTED - pending root != calculated root", slog.String("calculated_root", hex.EncodeToString(newRoot)), slog.String("pending_checkpoint", string(ticketCP)))
			return 0, 0, nil, nil, errors.New("internal error")
		}

		// This is a complete upload.
		sigs, pubSize, err := mt.publishCheckpoint(ctx, pendingCP, pendingNote)
		if err != nil {
			return nextEntry, pubSize, nil, nil, fmt.Errorf("publishCheckpoint %w", err) // %w as we may need to signal ErrConflict.
		}

		slog.WarnContext(ctx, "Completed upload", slog.Uint64("nextEntry", nextEntry), slog.Uint64("pendingSize", pendingCP.Size), slog.String("sigs", string(sigs)))
		return nextEntry, pendingCP.Size, ticketBytes, sigs, nil
	case nextEntry > pendingCP.Size:
		// TODO(al): ticket is stale, probably need to update the ticket?
		slog.WarnContext(ctx, "nextEntry > pendingSize", slog.Uint64("nextEntry", nextEntry), slog.Uint64("pendingSize", pendingCP.Size))
		return nextEntry, pendingCP.Size, ticketBytes, nil, nil
	default:
		slog.WarnContext(ctx, "Incomplete upload", slog.Uint64("nextEntry", nextEntry), slog.Uint64("pendingSize", pendingCP.Size))
		// Incomplete upload, return an updated ticket with the current checkpoint.
		return nextEntry, pendingCP.Size, ticketBytes, nil, nil
	}
}

// publishCheckpoint attempts to sign and atomically publish the provided checkpoint.
func (mt *MirrorTarget) publishCheckpoint(ctx context.Context, newCP *log.Checkpoint, cpNote *note.Note) ([]byte, uint64, error) {
	var retSigs []byte
	retSize := newCP.Size

	if err := mt.writer.UpdateCheckpoint(ctx, func(oldCP []byte) ([]byte, error) {
		// SPEC: Finally, the mirror performs the following steps atomically. Note the mirror
		// 			checkpoint may have changed since the start of this process.
		//  			- Check if upload_end is still greater than or equal to the mirror checkpoint's tree size.
		// 				- If so, update the mirror checkpoint to the pending checkpoint of size upload_end.
		// 			If upload_end was too small, the mirror MUST respond with a "409 Conflict" HTTP status
		//    	code, [with approriate response body].
		// 			Otherwise, if the mirror checkpoint was updated, the mirror MUST respond with a "200 Success"
		// 			HTTP status code. The response body MUST be formatted as in a witness's successful add-checkpoint
		// 			response: a sequence of one or more note signature lines.
		if len(oldCP) > 0 {
			publishedCP, _, _, err := log.ParseCheckpoint(oldCP, mt.logVerifier.Name(), mt.logVerifier)
			if err != nil {
				return nil, fmt.Errorf("failed to parse published checkpoint: %v", err)
			}
			if publishedCP.Origin != newCP.Origin {
				return nil, fmt.Errorf("published CP origin %q != new CP origin %q", publishedCP.Origin, newCP.Origin)
			}
			if publishedCP.Size > newCP.Size {
				// Return the larger size so that the caller re-syncs.
				retSize = publishedCP.Size
				return nil, ErrConflict
			}
		}
		signedNewCPRaw, sigs, err := signNote(cpNote, mt.signer)
		if err != nil {
			return nil, fmt.Errorf("failed to cosign pending checkpoint: %v", err)
		}

		retSigs = sigs

		return signedNewCPRaw, nil
	}); err != nil {
		return nil, retSize, fmt.Errorf("failed to update checkpoint: %v", err)
	}

	return retSigs, retSize, nil
}

// IntegratedSize returns the size of the current integrated log.
func (mt *MirrorTarget) IntegratedSize(ctx context.Context) (uint64, error) {
	return mt.reader.IntegratedSize(ctx)
}

func (mt *MirrorTarget) sealTicket(pendingCP []byte) ([]byte, error) {
	h := hmac.New(sha256.New, mt.ticketKey)
	h.Write(pendingCP)
	mac := h.Sum(nil)
	return append(mac, pendingCP...), nil
}

func (mt *MirrorTarget) openTicket(ticketBytes []byte) ([]byte, error) {
	if len(ticketBytes) < sha256.Size {
		return nil, errors.New("invalid ticket")
	}

	mac, pendingCP := ticketBytes[:sha256.Size], ticketBytes[sha256.Size:]

	h := hmac.New(sha256.New, mt.ticketKey)
	h.Write(pendingCP)
	if !hmac.Equal(mac, h.Sum(nil)) {
		return nil, errors.New("invalid ticketMAC")
	}
	return pendingCP, nil
}

func signNote(n *note.Note, signers ...note.Signer) ([]byte, []byte, error) {
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
	for _, s := range signers {
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

// isValiSignerdName reports whether name is valid.
// It must be non-empty and not have any Unicode spaces or pluses.
func isValidSignerName(name string) bool {
	return name != "" && utf8.ValidString(name) && strings.IndexFunc(name, unicode.IsSpace) < 0 && !strings.Contains(name, "+")
}
