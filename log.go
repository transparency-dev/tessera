// Copyright 2024 The Tessera authors. All Rights Reserved.
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
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	f_log "github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/trillian-tessera/internal/options"
	"golang.org/x/mod/sumdb/note"
	"k8s.io/klog/v2"
)

// ErrPushback is returned by underlying storage implementations when there are too many
// entries with indices assigned but which have not yet been integrated into the tree.
//
// Personalities encountering this error should apply back-pressure to the source of new entries
// in an appropriate manner (e.g. for HTTP services, return a 503 with a Retry-After header).
var ErrPushback = errors.New("too many unintegrated entries")

// IndexFuture is the signature of a function which can return an assigned index or error.
//
// Implementations of this func are likely to be "futures", or a promise to return this data at
// some point in the future, and as such will block when called if the data isn't yet available.
type IndexFuture func() (uint64, error)

// WithCheckpointSigner is an option for setting the note signer and verifier to use when creating and parsing checkpoints.
//
// A primary signer must be provided:
// - the primary signer is the "canonical" signing identity which should be used when creating new checkpoints.
//
// Zero or more dditional signers may also be provided.
// This enables cases like:
//   - a rolling key rotation, where checkpoints are signed by both the old and new keys for some period of time,
//   - using different signature schemes for different audiences, etc.
//
// When providing additional signers, their names MUST be identical to the primary signer name, and this name will be used
// as the checkpoint Origin line.
//
// Checkpoints signed by these signer(s) will be standard checkpoints as defined by https://c2sp.org/tlog-checkpoint.
func WithCheckpointSigner(s note.Signer, additionalSigners ...note.Signer) func(*options.StorageOptions) {
	origin := s.Name()
	for _, signer := range additionalSigners {
		if origin != signer.Name() {
			klog.Exitf("WithCheckpointSigner: additional signer name (%q) does not match primary signer name (%q)", signer.Name(), origin)
		}

	}
	return func(o *options.StorageOptions) {
		o.NewCP = func(size uint64, hash []byte) ([]byte, error) {
			// If we're signing a zero-sized tree, the tlog-checkpoint spec says (via RFC6962) that
			// the root must be SHA256 of the empty string, so we'll enforce that here:
			if size == 0 {
				emptyRoot := sha256.Sum256([]byte{})
				hash = emptyRoot[:]
			}
			cpRaw := f_log.Checkpoint{
				Origin: origin,
				Size:   size,
				Hash:   hash,
			}.Marshal()

			n, err := note.Sign(&note.Note{Text: string(cpRaw)}, append([]note.Signer{s}, additionalSigners...)...)
			if err != nil {
				return nil, fmt.Errorf("note.Sign: %w", err)
			}
			return n, nil
		}
	}
}

// WithBatching enables batching of write requests.
func WithBatching(maxSize uint, maxAge time.Duration) func(*options.StorageOptions) {
	return func(o *options.StorageOptions) {
		o.BatchMaxAge = maxAge
		o.BatchMaxSize = maxSize
	}
}

// WithPushback allows configuration of when the storage should start pushing back on add requests.
//
// maxOutstanding is the number of "in-flight" add requests - i.e. the number of entries with sequence numbers
// assigned, but which are not yet integrated into the log.
func WithPushback(maxOutstanding uint) func(*options.StorageOptions) {
	return func(o *options.StorageOptions) {
		o.PushbackMaxOutstanding = maxOutstanding
	}
}
