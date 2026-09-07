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

// mustSignSubtree signs a subtree and formats the signature as a note-style signature line.
func mustSignSubtree(t *testing.T, s f_note.SubtreeSigner, origin string, start, end uint64, root []byte) (rawSig []byte, sigLine []byte) {
	t.Helper()
	noteSig, err := s.SignSubtree(0, origin, start, end, root)
	if err != nil {
		t.Fatalf("SignSubtree: %v", err)
	}
	buf := binary.BigEndian.AppendUint32(nil, s.KeyHash())
	buf = append(buf, noteSig...)
	sigLine = fmt.Appendf(nil, "— %s %s\n", s.Name(), base64.StdEncoding.EncodeToString(buf))
	sigObj, err := mtcproof.NewSubtreeSignatureFromCosig(nil, noteSig)
	if err != nil {
		t.Fatalf("NewSubtreeSignatureFromCosig: %v", err)
	}
	return sigObj.Signature, sigLine
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

	tests := []struct {
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
	}

	for _, test := range tests {
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

	rawCpWithWit, err := note.Sign(&note.Note{Text: cpText}, logSigner, signer1)
	if err != nil {
		t.Fatalf("note.Sign: %v", err)
	}
	rawCpNoWit, err := note.Sign(&note.Note{Text: cpText}, logSigner)
	if err != nil {
		t.Fatalf("note.Sign: %v", err)
	}

	rawSubSig, subSigLine := mustSignSubtree(t, signer1, origin, start, end, root)

	corruptNoteSig, _ := signer1.SignSubtree(0, origin, start, end, root)
	corruptNoteSig[len(corruptNoteSig)-1] ^= 0xff
	corruptBuf := binary.BigEndian.AppendUint32(nil, signer1.KeyHash())
	corruptBuf = append(corruptBuf, corruptNoteSig...)
	corruptSubSigLine := fmt.Appendf(nil, "— %s %s\n", signer1.Name(), base64.StdEncoding.EncodeToString(corruptBuf))

	u1, _ := url.Parse("https://wit1.example.com")
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

	tests := []struct {
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
			policy:    policy.TLogPolicy{Quorum: "none"},
			rawCp:     rawCpNoWit,
			wantSigs:  0,
			wantErr:   nil,
		},
	}

	for _, test := range tests {
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
			if test.wantSigs > 0 && !bytes.Equal(verified[0].Signature, test.wantSubSig) {
				t.Errorf("got signature %x, want %x", verified[0].Signature, test.wantSubSig)
			}
		})
	}
}
