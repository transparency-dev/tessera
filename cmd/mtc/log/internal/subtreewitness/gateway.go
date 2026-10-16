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
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"

	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/formats/policy"
	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/mtcproof"
	wc "github.com/transparency-dev/witness/client/http"
	"golang.org/x/mod/sumdb/note"
)

// ErrPolicyNotSatisfied is returned when witness responses do not satisfy the required policy.
var ErrPolicyNotSatisfied = errors.New("witness policy was not satisfied")

type witnessKey struct {
	// string representation of the cosignerID
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
	policy    policy.TLogPolicy
}

// subtreeVerifier returns a SubtreeVerifier for a given witness.
// If w.Verifier already implements f_note.SubtreeVerifier, it is returned
// directly. Otherwise, it attempts to construct one from w.VKey.
func subtreeVerifier(w policy.Witness) (f_note.SubtreeVerifier, error) {
	if sv, ok := w.Verifier.(f_note.SubtreeVerifier); ok {
		return sv, nil
	}
	return f_note.NewMLDSAVerifier(w.VKey)
}

// New creates a new subtree witness Gateway.
// It only creates witness endpoints which implement f_note.SubtreeVerifier, i.e. which
// use ML-DSA signatures.
// SPEC: [DRAFT] Chrome Quantum-resistant Root Program Policy, Version 0.3.0, Section 3.1.
// "Mirroring Cosigner Keys MUST be ML-DSA-44"
func New(httpClient *http.Client, pol policy.TLogPolicy) (*Gateway, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	witnesses := make(map[witnessKey]witness)
	for _, w := range pol.Witnesses {
		if w.URL == nil {
			return nil, fmt.Errorf("missing URL for subtree witness %q", w.Name)
		}
		if w.Verifier == nil {
			return nil, fmt.Errorf("missing Verifier for witness %q", w.Name)
		}
		sv, err := subtreeVerifier(w)
		if err != nil {
			// Ignore witnesses that do not implement SubtreeVerifier.
			slog.WarnContext(context.Background(), "witness verifier does not implement SubtreeVerifier",
				slog.String("name", w.Name),
				slog.Any("error", err),
			)
			continue
		}
		cosignerID, err := mtcproof.ParseCosignerID(sv.Name())
		if err != nil {
			return nil, fmt.Errorf("invalid cosigner name in subtree verifier %q: %v", sv.Name(), err)
		}
		k := witnessKey{name: sv.Name(), keyHash: sv.KeyHash()}
		if _, exists := witnesses[k]; exists {
			return nil, fmt.Errorf("duplicate witness %q with key hash %x", k.name, k.keyHash)
		}
		client := wc.NewWitness(w.URL, httpClient)
		witnesses[k] = witness{
			client:     client,
			verifier:   sv,
			cosignerID: cosignerID,
		}
	}

	return &Gateway{
		witnesses: witnesses,
		policy:    pol,
	}, nil
}

// CosignSubtree sends concurrent subtree cosigning requests to all witnesses which cosigned the checkpoint,
// and returns gathered SubtreeSignatures as soon as the policy the Gateway was constructed with is satisfied.
//
// CosignSubtree checks for policy satisfaction on a reconstructed checkpoint, containing the
// checkpoint signatures corresponding to the collected subtree cosignatures. This means that policy
// will be met once sufficient cosignatures from cosigners who have signed a corresponding
// checkpoint have been collected.
// TODO: implement subtree cosignature policy matching directly.
func (gw *Gateway) CosignSubtree(ctx context.Context, origin string, start, end uint64, subRoot []byte, consProof [][]byte, rawCp []byte) ([]mtcproof.SubtreeSignature, error) {
	// TODO: consider disallowing this if empty policies are not allowed.
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

	// reconstructedCp is used for policy checking.
	reconstructedCp := fmt.Appendf(nil, "%s\n", n.Text)

	// Figure out which witnesses signed the checkpoint so we know who the candidates for getting
	// subtree cosignatures from are, and store their note-formatted checkpoint cosignatures.
	cpSigs := make(map[witnessKey][]byte)
	for _, s := range n.UnverifiedSigs {
		cpSigs[witnessKey{name: s.Name, keyHash: s.Hash}] = fmt.Appendf(nil, "— %s %s\n", s.Name, s.Base64)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type sigOrErr struct {
		k   witnessKey
		sig []byte
		err error
	}
	results := make(chan sigOrErr, len(gw.witnesses))

	// Kick off a goroutine for each witness that signed the checkpoint and send result to results chan
	var waitGroup sync.WaitGroup
	for k, w := range gw.witnesses {
		cpSig, ok := cpSigs[k]
		if !ok {
			slog.DebugContext(ctx, "Skipping witness not present on checkpoint",
				slog.String("witness", k.name),
				slog.String("key_hash", fmt.Sprintf("%08x", k.keyHash)),
			)
			continue
		}
		// The checkpoint we send must carry exactly one signature, and it MUST be the
		// checkpoint cosignature from the witness we're sending to.
		witCp := fmt.Appendf(nil, "%s\n%s", n.Text, cpSig)

		waitGroup.Go(func() {
			sig, err := w.client.SignSubtree(ctx, start, end, subRoot, consProof, witCp)
			results <- sigOrErr{
				k:   k,
				sig: sig,
				err: err,
			}
		})
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

		w := gw.witnesses[r.k]
		if !w.verifier.VerifySubtree(origin, start, end, subRoot, r.sig) {
			slog.ErrorContext(ctx, "Subtree signature verification failed",
				slog.String("witness", r.k.name),
				slog.Uint64("start", start),
				slog.Uint64("end", end),
			)
			continue
		}

		verifiedSubtreeSigs[r.k] = mtcproof.SubtreeSignature{
			CosignerID: w.cosignerID,
			Signature:  r.sig,
		}
		reconstructedCp = append(reconstructedCp, cpSigs[r.k]...)

		if gw.policy.Satisfied(reconstructedCp) {
			return slices.Collect(maps.Values(verifiedSubtreeSigs)), nil
		}
	}

	return nil, err
}
