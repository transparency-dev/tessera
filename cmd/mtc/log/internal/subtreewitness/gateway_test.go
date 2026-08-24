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
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"testing"

	f_log "github.com/transparency-dev/formats/log"
	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"golang.org/x/mod/sumdb/note"
)

type mockSubtreeClient struct {
	signFunc func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error)
}

func (m *mockSubtreeClient) SignSubtree(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
	return m.signFunc(ctx, start, end, subRoot, proof, rawCp)
}

// mustSignSubtree signs a subtree and formats the signature as a note-style signature line.
func mustSignSubtree(t *testing.T, s f_note.SubtreeSigner, origin string, start, end uint64, root []byte) (rawSig []byte, sigLine []byte) {
	t.Helper()
	rawSig, err := s.SignSubtree(0, origin, start, end, root)
	if err != nil {
		t.Fatalf("SignSubtree: %v", err)
	}
	buf := binary.BigEndian.AppendUint32(nil, s.KeyHash())
	buf = append(buf, rawSig...)
	sigLine = []byte(fmt.Sprintf("— %s %s\n", s.Name(), base64.StdEncoding.EncodeToString(buf)))
	return rawSig, sigLine
}

func TestNew(t *testing.T) {
	_, vkeyValid, err := f_note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.106")
	if err != nil {
		t.Fatalf("GenerateMLDSAKey: %v", err)
	}
	u1, _ := url.Parse("https://wit1.example.com")
	wValid, err := tessera.NewWitness(vkeyValid, u1)
	if err != nil {
		t.Fatalf("NewWitness: %v", err)
	}

	_, vkeyNonSubtree, err := note.GenerateKey(nil, "non-subtree-witness")
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	u2, _ := url.Parse("https://wit2.example.com")
	wNonSubtree, err := tessera.NewWitness(vkeyNonSubtree, u2)
	if err != nil {
		t.Fatalf("NewWitness: %v", err)
	}

	_, vkeyInvalidName, err := f_note.GenerateMLDSAKey("invalid-name-not-oid")
	if err != nil {
		t.Fatalf("GenerateMLDSAKey: %v", err)
	}
	u3, _ := url.Parse("https://wit3.example.com")
	wInvalidName, err := tessera.NewWitness(vkeyInvalidName, u3)
	if err != nil {
		t.Fatalf("NewWitness: %v", err)
	}

	tests := []struct {
		name          string
		policy        tessera.WitnessGroup
		wantWitnesses int
		wantErr       bool
	}{
		{
			name:          "valid single subtree witness",
			policy:        tessera.NewWitnessGroup(1, wValid),
			wantWitnesses: 1,
			wantErr:       false,
		},
		{
			name:          "non-subtree verifier is skipped",
			policy:        tessera.NewWitnessGroup(1, wNonSubtree),
			wantWitnesses: 0,
			wantErr:       false,
		},
		{
			name:          "mixed subtree and non-subtree witnesses",
			policy:        tessera.NewWitnessGroup(1, wValid, wNonSubtree),
			wantWitnesses: 1,
			wantErr:       false,
		},
		{
			name:          "invalid cosigner name in subtree verifier",
			policy:        tessera.NewWitnessGroup(1, wInvalidName),
			wantWitnesses: 0,
			wantErr:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gw, err := New(nil, tc.policy)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewGateway() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && len(gw.witnesses) != tc.wantWitnesses {
				t.Errorf("got %d witnesses in gateway, want %d", len(gw.witnesses), tc.wantWitnesses)
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

	rawCpWithWit, err := note.Sign(&note.Note{Text: cpText}, logSigner, signer1)
	if err != nil {
		t.Fatalf("note.Sign: %v", err)
	}
	rawCpNoWit, err := note.Sign(&note.Note{Text: cpText}, logSigner)
	if err != nil {
		t.Fatalf("note.Sign: %v", err)
	}

	rawSubSig, subSigLine := mustSignSubtree(t, signer1, origin, start, end, root)

	corruptSubSig := bytes.Clone(rawSubSig)
	corruptSubSig[0] ^= 0xff
	corruptBuf := binary.BigEndian.AppendUint32(nil, signer1.KeyHash())
	corruptBuf = append(corruptBuf, corruptSubSig...)
	corruptSubSigLine := []byte(fmt.Sprintf("— %s %s\n", signer1.Name(), base64.StdEncoding.EncodeToString(corruptBuf)))

	u1, _ := url.Parse("https://wit1.example.com")
	w1, err := tessera.NewWitness(vkey1, u1)
	if err != nil {
		t.Fatalf("NewWitness: %v", err)
	}
	policy1 := tessera.NewWitnessGroup(1, w1)

	tests := []struct {
		name       string
		witnesses  map[witnessKey]witness
		policy     tessera.WitnessGroup
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
							return subSigLine, nil
						},
					},
					verifier:   ver1,
					cosignerID: []byte{0x01},
				},
			},
			policy:     policy1,
			rawCp:      rawCpWithWit,
			wantSigs:   1,
			wantSubSig: rawSubSig,
		},
		{
			name: "duplicate witness signature response is deduplicated",
			witnesses: map[witnessKey]witness{
				{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
					client: &mockSubtreeClient{
						signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
							return append(bytes.Clone(subSigLine), subSigLine...), nil
						},
					},
					verifier:   ver1,
					cosignerID: []byte{0x01},
				},
			},
			policy:     policy1,
			rawCp:      rawCpWithWit,
			wantSigs:   1,
			wantSubSig: rawSubSig,
		},
		{
			name: "policy not satisfied when witness key not on checkpoint",
			witnesses: map[witnessKey]witness{
				{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
					client: &mockSubtreeClient{
						signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, rawCp []byte) ([]byte, error) {
							return subSigLine, nil
						},
					},
					verifier:   ver1,
					cosignerID: []byte{0x01},
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
							return corruptSubSigLine, nil
						},
					},
					verifier:   ver1,
					cosignerID: []byte{0x01},
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
					cosignerID: []byte{0x01},
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
			name:      "empty policy satisfied with empty gateway",
			witnesses: map[witnessKey]witness{},
			policy:    tessera.WitnessGroup{},
			rawCp:     rawCpNoWit,
			wantSigs:  0,
			wantErr:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gw := &Gateway{
				witnesses: tc.witnesses,
				policy:    tc.policy,
			}
			verified, err := gw.CosignSubtree(context.Background(), origin, start, end, root, nil, tc.rawCp)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got error %v, want %v", err, tc.wantErr)
			}
			if len(verified) != tc.wantSigs {
				t.Fatalf("got %d verified sigs, want %d", len(verified), tc.wantSigs)
			}
			if tc.wantSigs > 0 && !bytes.Equal(verified[0].Signature, tc.wantSubSig) {
				t.Errorf("got signature %x, want %x", verified[0].Signature, tc.wantSubSig)
			}
		})
	}
}
