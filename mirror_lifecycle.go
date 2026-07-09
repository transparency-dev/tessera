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
	"github.com/transparency-dev/merkle"
	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
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
	// ErrInvalidProof is returned when a proof fails to verify.
	ErrInvalidProof = errors.New("invalid proof")
)

// maxExcessEntries is the maximum number of "excess" entries that can be
// re-uploaded. This is intended to prevent mirror re-uploads from going too far
// back in time.
const maxExcessEntries = 2048

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
	// IntegrateBundles integrates bundles of log entries, starting at the given bundle index, into the local tree.
	// Bundles are _always_ aligned on bundle boundaries.
	// Implementations MUST NOT overwrite entries that are already integrated into the tree.
	//
	// Returns the size of the tree and its new root hash if successful.
	// If the provided iterator yields an error, the MirrorWriter MUST return it either directly, or wrapped so the caller can identify it.
	IntegrateBundles(ctx context.Context, fromBundleIdx uint64, bundles iter.Seq2[*api.EntryBundle, error]) (uint64, []byte, error)
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
	writer             MirrorWriter
	reader             LogReader
	cpSource           func(context.Context) ([]byte, error)
	signer             note.Signer
	logVerifier        note.Verifier
	verifySubtreeProof func(hasher merkle.LogHasher, start, end, size uint64, proof [][]byte, subRoot []byte, root []byte) error
	ticketKey          []byte
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
		writer:             mw,
		reader:             r,
		cpSource:           opts.cpSource,
		signer:             opts.signer,
		logVerifier:        opts.logVerifier,
		verifySubtreeProof: proof.VerifySubtreeConsistency,
		ticketKey:          tK,
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
// Returns:
// - the next required entry index,
// - a recent pending checkpoint size,
// - an opaque ticket for future invocation,
// - optionally, a cosignature over a pending checkpoint whose size matches uploadEnd if one exists.
func (mt *MirrorTarget) AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticketBytes []byte, next func() (*MirrorPackage, error)) (uint64, uint64, []byte, []byte, error) {
	nextEntry, pendingSize, userTicketValid, ticketBytes, pendingCP, pendingNote, err := mt.openOrCreateTicket(ctx, ticketBytes, uploadEnd)
	if err != nil {
		return nextEntry, pendingSize, ticketBytes, nil, err
	}

	// Handle 409 Conflicts:
	//    - Zero-request check: If upload_start == 0 and upload_end == 0 and provided no valid ticket, the client is
	//      requesting initial mirror information.
	//    - upload_end:
	//      * MUST be equal to the tree size of a known pending checkpoint.
	//      * MUST NOT be less than the mirror's current checkpoint tree size.
	//    - upload_start:
	//      * MUST NOT be greater than the mirror's next expected entry index.
	//      * MUST NOT be too far below the mirror's next entry index.
	if excessEntries := min(uploadEnd, nextEntry) - uploadStart; (uploadStart == 0 && uploadEnd == 0 && !userTicketValid) ||
		(uploadEnd != pendingSize || uploadEnd < nextEntry) ||
		(uploadStart > nextEntry || excessEntries > maxExcessEntries) {
		slog.ErrorContext(ctx, "Returning conflict", slog.Bool("ticket_valid", userTicketValid), slog.Uint64("next_entry", nextEntry), slog.Uint64("pending_size", pendingSize), slog.Uint64("upload_start", uploadStart), slog.Uint64("upload_end", uploadEnd), slog.Uint64("excess_entries", excessEntries))
		return nextEntry, pendingSize, ticketBytes, nil, ErrConflict
	}

	bundleIdx := uploadStart / layout.EntryBundleWidth
	nextEntry, newRoot, err := mt.writer.IntegrateBundles(ctx, bundleIdx, mt.bundleIterator(ctx, next, uploadStart, pendingCP))
	switch {
	case err != nil:
		return 0, 0, nil, nil, err
	case nextEntry == pendingSize:
		if !bytes.Equal(pendingCP.Hash, newRoot) {
			slog.ErrorContext(ctx, "CORRUPTION DETECTED - pending root != calculated root", slog.String("calculated_root", hex.EncodeToString(newRoot)), slog.String("pending_checkpoint", string(pendingNote.Text)))
			return 0, 0, nil, nil, errors.New("internal error")
		}

		// This is a complete upload.
		sigs, pubSize, err := mt.publishCheckpoint(ctx, pendingCP, pendingNote)
		if err != nil {
			return nextEntry, pubSize, nil, nil, fmt.Errorf("publishCheckpoint %w", err) // %w as we may need to signal ErrConflict.
		}

		slog.WarnContext(ctx, "Completed upload", slog.Uint64("nextEntry", nextEntry), slog.Uint64("pendingSize", pendingSize), slog.String("sigs", string(sigs)))
		return nextEntry, pendingSize, nil, sigs, nil
	case nextEntry > pendingSize:
		slog.WarnContext(ctx, "nextEntry > pendingSize", slog.Uint64("nextEntry", nextEntry), slog.Uint64("pendingSize", pendingSize))
		return nextEntry, pendingSize, nil, nil, nil
	default:
		slog.WarnContext(ctx, "Incomplete upload", slog.Uint64("nextEntry", nextEntry), slog.Uint64("pendingSize", pendingSize))
		// Incomplete upload, return an updated ticket with the current checkpoint.
		return nextEntry, pendingSize, ticketBytes, nil, nil
	}
}

// bundleIterator returns an iterator which yields entry bundles after verifying their subtree consistency with the provided pending checkpoint.
//
// Yielded entry bundles are always aligned to bundle boundaries. Specifically, this means that if the provided start is _not_ bundle aligned, then we will
// fetch entries from the bundle at start/256 and use those entries to left-pad the first yielded bundle.
func (mt *MirrorTarget) bundleIterator(ctx context.Context, next func() (*MirrorPackage, error), start uint64, pendingCP *log.Checkpoint) func(func(*api.EntryBundle, error) bool) {
	crf := compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}
	return func(yield func(*api.EntryBundle, error) bool) {
		// Check for unaligned upload start, and fetch entries from the start of the bundle to use to pad.
		// This is necessary for the subtree proof for such an unaligned first bundle to validate.
		var padEntries [][]byte
		if p := start % layout.EntryBundleWidth; p != 0 {
			// non-aligned starting bundle
			br, err := mt.reader.ReadEntryBundle(ctx, start/layout.EntryBundleWidth, uint8(p))
			if err != nil {
				yield(nil, fmt.Errorf("failed to read bundle containing uploadStart (%d): %v", start, err))
				return
			}
			// Parse and clip, just in case we were returned data from a full tile.
			b := &api.EntryBundle{}
			if err := b.UnmarshalText(br); err != nil {
				yield(nil, fmt.Errorf("failed to unmarshal bundle containing uploadStart (%d): %v", start, err))
				return
			}
			if l := len(b.Entries); l < int(p) {
				yield(nil, fmt.Errorf("POTENTIAL CORRUPTION: partial bundle at index %d.%d has only %d entries", start/layout.EntryBundleWidth, p, l))
				return
			}
			padEntries = b.Entries[:p]
			// SPEC: The subtree consistency proof is computed from the subtree defined by [rounded_start + i * 256, end), and the log
			//       checkpoint with tree size upload_end
			start &= ^uint64(0xff) // floor to bundle boundary
		}

		for {
			pkg, err := next()
			if err != nil {
				if err == io.EOF {
					return
				}
				slog.WarnContext(ctx, "NextPackage returned an error", slog.String("error", err.Error()))
				yield(nil, fmt.Errorf("failed to get next package: %w", err)) // Wrap to preserve err from next().
				return
			}

			// Handle the case where the first mirror package is not bundle-aligned.
			if len(padEntries) > 0 {
				pkg.Entries = append(padEntries, pkg.Entries...)
				padEntries = nil
			}

			// Build the subtree root so that we can verify the package proof.
			cr := crf.NewEmptyRange(0)
			for _, e := range pkg.Entries {
				if err := cr.Append(rfc6962.DefaultHasher.HashLeaf(e), nil); err != nil {
					yield(nil, fmt.Errorf("failed to append hash to compact range: %v", err))
					return
				}
			}
			subRoot, err := cr.GetRootHash(nil)
			if err != nil {
				yield(nil, fmt.Errorf("failed to get root of compact range: %v", err))
				return
			}

			// SPEC: For each entry package, it MUST authenticate the entries by verifying the subtree consistency proof:
			// 				- First, it reconstructs the subtree hash based on the received entries and entries already in the log.
			// 				- It then verifies the subtree consistency proof using this hash and the checkpoint at upload_end.
			//			If this verification process fails, it MUST respond with a "422 Unprocessable Entity" HTTP status code and end processing.
			if err := mt.verifySubtreeProof(rfc6962.DefaultHasher, start, start+uint64(len(pkg.Entries)), pendingCP.Size, pkg.Proof, subRoot, pendingCP.Hash); err != nil {
				// Return ErrInvalidProof which the handler can turn into a 422 status.
				yield(nil, fmt.Errorf("failed to verify subtree consistency: %w", ErrInvalidProof))
				return
			}
			start += uint64(len(pkg.Entries))

			if !yield(&api.EntryBundle{Entries: pkg.Entries}, nil) {
				return
			}
		}
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
		return nil, retSize, fmt.Errorf("failed to update checkpoint: %w", err)
	}

	return retSigs, retSize, nil
}

// IntegratedSize returns the size of the current integrated log.
func (mt *MirrorTarget) IntegratedSize(ctx context.Context) (uint64, error) {
	return mt.reader.IntegratedSize(ctx)
}

// openOrCreateTicket handles ticket logic, returning a new ticket if the provided one is invalid/missing.
//
// Returns next entry index to upload, size of the pending checkpoint, a bool indicating whether the provided ticket was valid, the ticket to return to the caller (may be the same as the provided one), pending checkpoint structure, pending note, and error.
func (mt *MirrorTarget) openOrCreateTicket(ctx context.Context, ticketBytes []byte, expectedSize uint64) (uint64, uint64, bool, []byte, *log.Checkpoint, *note.Note, error) {
	nextEntry, err := mt.reader.IntegratedSize(ctx)
	if err != nil {
		return 0, 0, false, nil, nil, nil, fmt.Errorf("failed to read integrated size: %v", err)
	}

	var pendingCP *log.Checkpoint
	var pendingNote *note.Note
	userTicketValid := false

	ticketCP, err := mt.open(ticketBytes)
	if err != nil {
		slog.DebugContext(ctx, "Failed to open ticket", slog.Any("error", err))
	} else {
		pendingCP, _, pendingNote, err = log.ParseCheckpoint(ticketCP, mt.logVerifier.Name(), mt.logVerifier)
		if err != nil {
			slog.DebugContext(ctx, "Failed to parse ticket", slog.Any("error", err))
		} else {
			slog.DebugContext(ctx, "Valid ticket", slog.Uint64("nextEntry", nextEntry), slog.Uint64("pendingSize", pendingCP.Size))
			userTicketValid = true
		}
	}

	if pendingCP == nil || pendingCP.Size != expectedSize {
		slog.DebugContext(ctx, "Invalid or incorrect ticket, returning new ticket", slog.Uint64("next_entry", nextEntry), slog.String("ticketCP", string(ticketCP)), slog.Uint64("expectedSize", expectedSize))
		ticketBytes, pendingCP, pendingNote, err = mt.createNewTicket(ctx)
		if err != nil {
			return 0, 0, userTicketValid, nil, nil, nil, fmt.Errorf("failed to create new ticket: %w", err)
		}
		// If the new pending checkpoint still doesn't match expectedSize, return 409 Conflict with the fresh ticket.
		if pendingCP.Size != expectedSize {
			return nextEntry, pendingCP.Size, userTicketValid, ticketBytes, pendingCP, pendingNote, ErrConflict
		}
		// Otherwise, allow the request to continue (because their uploadEnd == pendingCP.Size).
	}

	return nextEntry, pendingCP.Size, userTicketValid, ticketBytes, pendingCP, pendingNote, nil
}

func (mt *MirrorTarget) createNewTicket(ctx context.Context) (ticket []byte, pendingCP *log.Checkpoint, pendingNote *note.Note, err error) {
	pendingCPRaw, err := mt.cpSource(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get pending checkpoint: %v", err)
	}
	if len(pendingCPRaw) == 0 {
		return nil, nil, nil, ErrNoPendingCheckpoint
	}
	pendingCP, _, pendingNote, err = log.ParseCheckpoint(pendingCPRaw, mt.logVerifier.Name(), mt.logVerifier)
	if err != nil {
		slog.ErrorContext(ctx, "Invalid pending checkpoint from source", slog.String("pending_checkpoint", string(pendingCPRaw)), slog.String("error", err.Error()))
		return nil, nil, nil, fmt.Errorf("failed to parse pending checkpoint while creating ticket: %v", err)
	}
	ticket, err = mt.seal(pendingCPRaw)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create ticket: %v", err)
	}
	return ticket, pendingCP, pendingNote, nil
}

func (mt *MirrorTarget) seal(b []byte) ([]byte, error) {
	h := hmac.New(sha256.New, mt.ticketKey)
	h.Write(b)
	mac := h.Sum(nil)
	return append(mac, b...), nil
}

func (mt *MirrorTarget) open(sealed []byte) ([]byte, error) {
	if len(sealed) < sha256.Size {
		return nil, errors.New("invalid sealed value")
	}

	mac, b := sealed[:sha256.Size], sealed[sha256.Size:]

	h := hmac.New(sha256.New, mt.ticketKey)
	h.Write(b)
	if !hmac.Equal(mac, h.Sum(nil)) {
		return nil, errors.New("invalid sealed value MAC")
	}
	return b, nil
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
