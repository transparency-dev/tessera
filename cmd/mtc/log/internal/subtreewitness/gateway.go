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

// Package subtreewitness provides a gateway for subtree cosigning with witnesses.
package subtreewitness

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sync"

	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/mtcproof"
	wc "github.com/transparency-dev/witness/client/http"
	"golang.org/x/mod/sumdb/note"
)

// ErrPolicyNotSatisfied is returned when witness responses do not satisfy the required policy.
var ErrPolicyNotSatisfied = errors.New("witness policy was not satisfied")

type witnessKey struct {
	name    string
	keyHash uint32
}

type witness struct {
	client     SubtreeWitnessClient
	verifier   f_note.SubtreeVerifier
	cosignerID []byte
}

// SubtreeWitnessClient defines the interface for calling a witness's sign-subtree endpoint.
type SubtreeWitnessClient interface {
	SignSubtree(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error)
}

// Gateway manages concurrent requests to subtree witnesses and evaluates policy satisfaction.
type Gateway struct {
	witnesses map[witnessKey]witness
	policy    tessera.WitnessGroup
}

// New creates a new subtree witness Gateway.
// It only creates witness endpoints which implement f_note.SubtreeVerifier, i.e. which
// use ML-DSA signatures.
// SPEC: [DRAFT] Chrome Quantum-resistant Root Program Policy, Version 0.3.0, Section 3.1.
// "Mirroring Cosigner Keys MUST be ML-DSA-44"
func New(httpClient *http.Client, policy tessera.WitnessGroup) (*Gateway, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	witnesses := make(map[witnessKey]witness)
	for uStr, vs := range policy.WitnessEndpoints() {
		u, err := url.Parse(uStr)
		if err != nil {
			return nil, fmt.Errorf("invalid witness URL %q: %w", uStr, err)
		}
		if len(vs) == 0 {
			return nil, fmt.Errorf("no verifiers for witness %s", uStr)
		}
		client := wc.NewWitness(u, httpClient)
		for _, v := range vs {
			if sv, ok := v.(f_note.SubtreeVerifier); ok {
				cosignerID, err := mtcproof.ParseCosignerID(sv.Name())
				if err != nil {
					return nil, fmt.Errorf("invalid cosigner ID for witness %s: %w", sv.Name(), err)
				}
				witnesses[witnessKey{name: sv.Name(), keyHash: sv.KeyHash()}] = witness{
					client:     client,
					verifier:   sv,
					cosignerID: cosignerID,
				}
			}
		}
	}

	return &Gateway{
		witnesses: witnesses,
		policy:    policy,
	}, nil
}

// CosignSubtree sends concurrent subtree cosigning requests to all witnesses and returns gathered
// SubtreeSignatures as soon as the policy the Gateway was constructed with is satisfied.
//
// CosignSubtree checks for policy satisfaction on a reconstructed checkpoint, containing the
// checkpoint signatures corresponding to the collected subtree cosignatures. This means that policy
// will be met once sufficient cosignatures from cosigners who have signed a corresponding
// checkpoint have been collected.
// TODO: implement subtree cosignature policy matching directly.
func (gw *Gateway) CosignSubtree(ctx context.Context, origin string, start, end uint64, subRoot []byte, consProof [][]byte, rawCp []byte) ([]mtcproof.SubtreeSignature, error) {
	if len(gw.witnesses) == 0 {
		if gw.policy.Satisfied(rawCp) {
			return nil, nil
		}
		return nil, ErrPolicyNotSatisfied
	}

	// Open the checkpoint without verifying it to extract all signatures.
	_, err := note.Open(rawCp, note.VerifierList())
	var unverified *note.UnverifiedNoteError
	if !errors.As(err, &unverified) {
		return nil, fmt.Errorf("failed to parse checkpoint note: %v", err)
	}
	n := unverified.Note

	// reconstructCp is used for policy checking.
	reconstructedCp := fmt.Appendf(nil, "%s\n", n.Text)

	cpSigs := make(map[witnessKey]string)
	for _, s := range n.UnverifiedSigs {
		cpSigs[witnessKey{name: s.Name, keyHash: s.Hash}] = s.Base64
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var waitGroup sync.WaitGroup
	type sigOrErr struct {
		sig []byte
		err error
	}
	results := make(chan sigOrErr, len(gw.witnesses))

	// Kick off a goroutine for each witness and send result to results chan
	for _, w := range gw.witnesses {
		waitGroup.Add(1)
		go func(w witness) {
			defer waitGroup.Done()
			sig, err := w.client.SignSubtree(ctx, start, end, subRoot, consProof, rawCp)
			results <- sigOrErr{
				sig: sig,
				err: err,
			}
		}(w)
	}

	go func() {
		waitGroup.Wait()
		close(results)
	}()

	verifiedSubtreeSigs := make(map[witnessKey]mtcproof.SubtreeSignature)
	err = ErrPolicyNotSatisfied

	// Consume the results coming back from each witness
	for r := range results {
		if r.err != nil {
			err = errors.Join(err, r.err)
			continue
		}

		var sigNote *note.UnverifiedNoteError
		// Use note.Open on a synthetic note to enforce strict signature formatting.
		_, sigErr := note.Open(append([]byte("text\n\n"), r.sig...), note.VerifierList())
		if !errors.As(sigErr, &sigNote) {
			slog.WarnContext(ctx, "Failed to parse witness subtree signature response", slog.Any("error", sigErr))
			continue
		}

		for _, s := range sigNote.Note.UnverifiedSigs {
			raw, bErr := base64.StdEncoding.DecodeString(s.Base64)
			if bErr != nil || len(raw) < 4 {
				slog.WarnContext(ctx, "Failed to decode witness subtree signature base64", slog.String("witness", s.Name), slog.Any("error", bErr))
				continue
			}
			keyHash := s.Hash
			sigBytes := raw[4:]

			k := witnessKey{name: s.Name, keyHash: keyHash}
			// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
			// "An MTCProof parser MUST reject the input if there are duplicate cosigner_id values"
			if _, ok := verifiedSubtreeSigs[k]; ok {
				continue
			}

			b64, ok := cpSigs[k]
			if !ok {
				slog.WarnContext(ctx, "Received subtree signature from witness not present on checkpoint",
					slog.String("witness", s.Name),
					slog.String("key_hash", fmt.Sprintf("%08x", keyHash)),
				)
				continue
			}

			w, ok := gw.witnesses[k]
			if !ok {
				slog.ErrorContext(ctx, "Received subtree signature from unknown witness key",
					slog.String("witness", s.Name),
					slog.String("key_hash", fmt.Sprintf("%08x", keyHash)),
				)
				continue
			}

			if !w.verifier.VerifySubtree(0, origin, start, end, subRoot, sigBytes) {
				slog.ErrorContext(ctx, "Subtree signature verification failed",
					slog.String("witness", s.Name),
					slog.Uint64("start", start),
					slog.Uint64("end", end),
				)
				continue
			}

			verifiedSubtreeSigs[k] = mtcproof.SubtreeSignature{
				CosignerID: w.cosignerID,
				Signature:  sigBytes,
			}
			reconstructedCp = fmt.Appendf(reconstructedCp, "— %s %s\n", s.Name, b64)
		}

		if gw.policy.Satisfied(reconstructedCp) {
			return slices.Collect(maps.Values(verifiedSubtreeSigs)), nil
		}
	}

	return nil, err
}
