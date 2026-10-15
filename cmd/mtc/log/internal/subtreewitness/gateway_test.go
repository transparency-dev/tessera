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

package subtreewitness

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"testing"

	f_log "github.com/transparency-dev/formats/log"
	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/formats/policy"
	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/mtcproof"
	"golang.org/x/mod/sumdb/note"
)

type mockSubtreeClient struct {
	signFunc func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error)
}

func (m *mockSubtreeClient) SignSubtree(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
	return m.signFunc(ctx, start, end, subRoot, proof, rawCp)
}

// mustSignSubtree signs a subtree and returns the raw signature bytes.
func mustSignSubtree(t *testing.T, s f_note.SubtreeSigner, origin string, start, end uint64, root []byte) []byte {
	t.Helper()
	rawSig, err := s.SignSubtree(origin, start, end, root)
	if err != nil {
		t.Fatalf("SignSubtree: %v", err)
	}
	return rawSig
}

func TestNew(t *testing.T) {
	_, vkeyValid, err := f_note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.106")
	if err != nil {
		t.Fatalf("GenerateMLDSAKey: %v", err)
	}
	verValid, err := f_note.NewMLDSAVerifier(vkeyValid)
	if err != nil {
		t.Fatalf("NewMLDSAVerifier: %v", err)
	}
	u1, _ := url.Parse("https://wit1.example.com")
	witValid := policy.Witness{
		Name:     verValid.Name(),
		URL:      u1,
		VKey:     vkeyValid,
		Verifier: verValid,
	}

	_, vkeyNonSubtree, err := note.GenerateKey(nil, "non-subtree-witness")
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	u2, _ := url.Parse("https://wit2.example.com")
	vNonSubtree, err := f_note.NewVerifierForCosignatureV1(vkeyNonSubtree)
	if err != nil {
		t.Fatalf("NewVerifierForCosignatureV1: %v", err)
	}
	witNonSubtree := policy.Witness{
		Name:     vNonSubtree.Name(),
		URL:      u2,
		VKey:     vkeyNonSubtree,
		Verifier: vNonSubtree,
	}

	_, vkeyInvalidName, err := f_note.GenerateMLDSAKey("invalid-name-not-oid")
	if err != nil {
		t.Fatalf("GenerateMLDSAKey: %v", err)
	}
	verInvalid, err := f_note.NewMLDSAVerifier(vkeyInvalidName)
	if err != nil {
		t.Fatalf("NewMLDSAVerifier: %v", err)
	}
	u3, _ := url.Parse("https://wit3.example.com")
	witInvalidName := policy.Witness{
		Name:     verInvalid.Name(),
		URL:      u3,
		VKey:     vkeyInvalidName,
		Verifier: verInvalid,
	}

	verFromPolicy, err := f_note.NewVerifier(vkeyValid)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	witFromPolicy := policy.Witness{
		Name:     verFromPolicy.Name(),
		URL:      u1,
		VKey:     vkeyValid,
		Verifier: verFromPolicy,
	}

	for _, test := range []struct {
		name          string
		policy        policy.TLogPolicy
		wantWitnesses int
		wantErr       bool
	}{
		{
			name: "valid single subtree witness",
			policy: policy.TLogPolicy{
				Witnesses: []policy.Witness{witValid},
				Quorum:    witValid.Name,
			},
			wantWitnesses: 1,
			wantErr:       false,
		},
		{
			name: "fallback to VKey for policy witness",
			policy: policy.TLogPolicy{
				Witnesses: []policy.Witness{witFromPolicy},
				Quorum:    witFromPolicy.Name,
			},
			wantWitnesses: 1,
			wantErr:       false,
		},
		{
			name: "non-subtree verifier is skipped",
			policy: policy.TLogPolicy{
				Witnesses: []policy.Witness{witNonSubtree},
				Quorum:    witNonSubtree.Name,
			},
			wantWitnesses: 0,
			wantErr:       false,
		},
		{
			name: "mixed subtree and non-subtree witnesses",
			policy: policy.TLogPolicy{
				Witnesses: []policy.Witness{witValid, witNonSubtree},
				Groups: []policy.Group{
					{
						Name:      "group",
						Threshold: 1,
						Members:   []string{witValid.Name, witNonSubtree.Name},
					},
				},
				Quorum: "group",
			},
			wantWitnesses: 1,
			wantErr:       false,
		},
		{
			name: "invalid cosigner name in subtree verifier",
			policy: policy.TLogPolicy{
				Witnesses: []policy.Witness{witInvalidName},
				Quorum:    witInvalidName.Name,
			},
			wantWitnesses: 0,
			wantErr:       true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			gw, err := New(nil, test.policy)
			if (err != nil) != test.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, test.wantErr)
			}
			if !test.wantErr && len(gw.witnesses) != test.wantWitnesses {
				t.Errorf("got %d witnesses in gateway, want %d", len(gw.witnesses), test.wantWitnesses)
			}
		})
	}
}

func TestGateway_CosignSubtree(t *testing.T) {
	origin := "example.com/log"
	start := uint64(0)
	end := uint64(1024)
	root := bytes.Repeat([]byte{0xcc}, 32)
	cp := f_log.Checkpoint{
		Origin: origin,
		Size:   end,
		Hash:   root,
	}
	cpText := string(cp.Marshal())

	logSKey, _, _ := f_note.GenerateMLDSAKey(origin)
	logSigner, _ := f_note.NewMLDSASigner(logSKey)

	skey1, vkey1, _ := f_note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.106")
	signer1, _ := f_note.NewMLDSASigner(skey1)
	ver1, err := f_note.NewMLDSAVerifier(vkey1)
	if err != nil {
		t.Fatalf("NewMLDSAVerifier: %v", err)
	}
	cosignerID1, err := mtcproof.ParseCosignerID(ver1.Name())
	if err != nil {
		t.Fatalf("ParseCosignerID: %v", err)
	}

	skey2, vkey2, _ := f_note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.107")
	signer2, _ := f_note.NewMLDSASigner(skey2)
	ver2, err := f_note.NewMLDSAVerifier(vkey2)
	if err != nil {
		t.Fatalf("NewMLDSAVerifier: %v", err)
	}
	cosignerID2, err := mtcproof.ParseCosignerID(ver2.Name())
	if err != nil {
		t.Fatalf("ParseCosignerID: %v", err)
	}

	rawCpWithWit, err := note.Sign(&note.Note{Text: cpText}, logSigner, signer1)
	if err != nil {
		t.Fatalf("note.Sign: %v", err)
	}
	rawCpNoWit, err := note.Sign(&note.Note{Text: cpText}, logSigner)
	if err != nil {
		t.Fatalf("note.Sign: %v", err)
	}

	// Construct a checkpoint where signer1's signature is valid, but signer2's signature
	// was computed over different checkpoint text and is therefore invalid for cpText.
	otherCp := f_log.Checkpoint{
		Origin: origin,
		Size:   end + 1,
		Hash:   root,
	}
	otherSignedBy2, err := note.Sign(&note.Note{Text: string(otherCp.Marshal())}, signer2)
	if err != nil {
		t.Fatalf("note.Sign: %v", err)
	}
	_, sig2Line, ok := bytes.Cut(otherSignedBy2, []byte("\n\n"))
	if !ok {
		t.Fatalf("expected signature separator in signed note")
	}
	rawCpWithCorruptWit2 := append(bytes.Clone(rawCpWithWit), sig2Line...)

	rawSubSig := mustSignSubtree(t, signer1, origin, start, end, root)
	corruptSubSig := mustSignSubtree(t, signer1, origin, start, end, bytes.Repeat([]byte{0xee}, 32))

	u1, _ := url.Parse("https://wit1.example.com")
	u2, _ := url.Parse("https://wit2.example.com")
	policy1 := policy.TLogPolicy{
		Witnesses: []policy.Witness{
			{
				Name:     ver1.Name(),
				URL:      u1,
				VKey:     vkey1,
				Verifier: ver1,
			},
		},
		Quorum: ver1.Name(),
	}
	policyAnyOf2 := policy.TLogPolicy{
		Witnesses: []policy.Witness{
			{
				Name:     ver1.Name(),
				URL:      u1,
				VKey:     vkey1,
				Verifier: ver1,
			},
			{
				Name:     ver2.Name(),
				URL:      u2,
				VKey:     vkey2,
				Verifier: ver2,
			},
		},
		Groups: []policy.Group{
			{
				Name:      "any2",
				Threshold: 1,
				Members:   []string{ver1.Name(), ver2.Name()},
			},
		},
		Quorum: "any2",
	}

	for _, test := range []struct {
		name       string
		witnesses  map[witnessKey]witness
		policy     policy.TLogPolicy
		rawCp      []byte
		wantSigs   int
		wantSubSig []byte
		wantErr    error
	}{
		{
			name: "policy satisfied with valid witness signature",
			witnesses: map[witnessKey]witness{
				{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
					client: &mockSubtreeClient{
						signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
							n, err := note.Open(rawCp, note.VerifierList(ver1))
							if err != nil {
								t.Errorf("expected checkpoint signed by witness: %v", err)
							} else if len(n.Sigs) != 1 || len(n.UnverifiedSigs) != 0 {
								t.Errorf("expected checkpoint with exactly 1 signature, got %d verified and %d unverified", len(n.Sigs), len(n.UnverifiedSigs))
							}
							return rawSubSig, nil
						},
					},
					verifier:   ver1,
					cosignerID: cosignerID1,
				},
			},
			policy:     policy1,
			rawCp:      rawCpWithWit,
			wantSigs:   1,
			wantSubSig: rawSubSig,
		},
		{
			name: "policy satisfied when another witness has a corrupted checkpoint signature",
			witnesses: map[witnessKey]witness{
				{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
					client: &mockSubtreeClient{
						signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
							return rawSubSig, nil
						},
					},
					verifier:   ver1,
					cosignerID: cosignerID1,
				},
				{name: ver2.Name(), keyHash: ver2.KeyHash()}: {
					client: &mockSubtreeClient{
						signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
							t.Errorf("SignSubtree should not be called for witness with invalid checkpoint signature")
							return nil, errors.New("unexpected call")
						},
					},
					verifier:   ver2,
					cosignerID: cosignerID2,
				},
			},
			policy:     policyAnyOf2,
			rawCp:      rawCpWithCorruptWit2,
			wantSigs:   1,
			wantSubSig: rawSubSig,
		},
		{
			name: "policy not satisfied when witness key not on checkpoint",
			witnesses: map[witnessKey]witness{
				{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
					client: &mockSubtreeClient{
						signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
							return rawSubSig, nil
						},
					},
					verifier:   ver1,
					cosignerID: cosignerID1,
				},
			},
			policy:   policy1,
			rawCp:    rawCpNoWit,
			wantSigs: 0,
			wantErr:  ErrPolicyNotSatisfied,
		},
		{
			name: "policy not satisfied when subtree signature verification fails",
			witnesses: map[witnessKey]witness{
				{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
					client: &mockSubtreeClient{
						signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
							return corruptSubSig, nil
						},
					},
					verifier:   ver1,
					cosignerID: cosignerID1,
				},
			},
			policy:   policy1,
			rawCp:    rawCpWithWit,
			wantSigs: 0,
			wantErr:  ErrPolicyNotSatisfied,
		},
		{
			name: "policy not satisfied when witness fails",
			witnesses: map[witnessKey]witness{
				{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
					client: &mockSubtreeClient{
						signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
							return nil, errors.New("witness down")
						},
					},
					verifier:   ver1,
					cosignerID: cosignerID1,
				},
			},
			policy:   policy1,
			rawCp:    rawCpWithWit,
			wantSigs: 0,
			wantErr:  ErrPolicyNotSatisfied,
		},
		{
			name:      "policy not satisfied when gateway has no witnesses",
			witnesses: map[witnessKey]witness{},
			policy:    policy1,
			rawCp:     rawCpNoWit,
			wantSigs:  0,
			wantErr:   ErrPolicyNotSatisfied,
		},
		{
			name:      "policy not satisfied when gateway has no witnesses even if checkpoint satisfies policy",
			witnesses: map[witnessKey]witness{},
			policy:    policy1,
			rawCp:     rawCpWithWit,
			wantSigs:  0,
			wantErr:   ErrPolicyNotSatisfied,
		},
		{
			name:      "empty policy satisfied with empty gateway",
			witnesses: map[witnessKey]witness{},
			policy:    policy.TLogPolicy{Quorum: "none"},
			rawCp:     rawCpNoWit,
			wantSigs:  0,
			wantErr:   nil,
		},
		{
			name: "empty policy satisfied with non-empty gateway and no witness on checkpoint",
			witnesses: map[witnessKey]witness{
				{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
					client: &mockSubtreeClient{
						signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
							t.Errorf("SignSubtree should not be called when policy is already satisfied")
							return nil, errors.New("unexpected call")
						},
					},
					verifier:   ver1,
					cosignerID: cosignerID1,
				},
			},
			policy:   policy.TLogPolicy{Quorum: "none"},
			rawCp:    rawCpNoWit,
			wantSigs: 0,
			wantErr:  nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			gw := &Gateway{
				witnesses: test.witnesses,
				policy:    test.policy,
			}
			verified, err := gw.CosignSubtree(context.Background(), origin, start, end, root, nil, test.rawCp)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("got error %v, want %v", err, test.wantErr)
			}
			if len(verified) != test.wantSigs {
				t.Fatalf("got %d verified sigs, want %d", len(verified), test.wantSigs)
			}
			if test.wantSigs > 0 {
				if !bytes.Equal(verified[0].CosignerID, cosignerID1) {
					t.Errorf("got cosigner ID %x, want %x", verified[0].CosignerID, cosignerID1)
				}
				if !bytes.Equal(verified[0].Signature, test.wantSubSig) {
					t.Errorf("got signature %x, want %x", verified[0].Signature, test.wantSubSig)
				}
			}
		})
	}
}
