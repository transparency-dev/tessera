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

func TestGateway_CosignSubtree(t *testing.T) {
	origin := "example.com/log"
	start := uint64(0)
	end := uint64(1024)
	root := bytes.Repeat([]byte{0xcc}, 32)
	cpBody := fmt.Sprintf("%s\n%d\n%s\n", origin, end, base64.StdEncoding.EncodeToString(root))

	logSKey, logVKey, _ := f_note.GenerateMLDSAKey(origin)
	logSigner, _ := f_note.NewMLDSASigner(logSKey)
	logVer, _ := f_note.NewMLDSAVerifier(logVKey)

	logCpSig, err := logSigner.Sign([]byte(cpBody))
	if err != nil {
		t.Fatalf("Sign checkpoint with log key: %v", err)
	}
	logCpSigRaw := binary.BigEndian.AppendUint32(nil, logSigner.KeyHash())
	logCpSigRaw = append(logCpSigRaw, logCpSig...)
	logCpSigLine := fmt.Sprintf("— %s %s\n", logSigner.Name(), base64.StdEncoding.EncodeToString(logCpSigRaw))

	skey1, vkey1, _ := f_note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.101")
	signer1, _ := f_note.NewMLDSASigner(skey1)
	ver1, err := f_note.NewMLDSAVerifier(vkey1)
	if err != nil {
		t.Fatalf("NewMLDSAVerifier: %v", err)
	}

	// Checkpoint signature on the checkpoint body for witness 1
	witCpSig, err := signer1.Sign([]byte(cpBody))
	if err != nil {
		t.Fatalf("Sign checkpoint with witness key: %v", err)
	}
	witCpSigRaw := binary.BigEndian.AppendUint32(nil, signer1.KeyHash())
	witCpSigRaw = append(witCpSigRaw, witCpSig...)
	witCpSigLine := fmt.Sprintf("— %s %s\n", signer1.Name(), base64.StdEncoding.EncodeToString(witCpSigRaw))

	rawCp := append(append([]byte(cpBody), '\n'), []byte(logCpSigLine+witCpSigLine)...)

	// Subtree signature for the subtree
	rawSubSig, err := signer1.SignSubtree(0, origin, start, end, root)
	if err != nil {
		t.Fatalf("SignSubtree: %v", err)
	}
	subSigRaw := binary.BigEndian.AppendUint32(nil, signer1.KeyHash())
	subSigRaw = append(subSigRaw, rawSubSig...)
	subSigLine := []byte(fmt.Sprintf("— %s %s\n", signer1.Name(), base64.StdEncoding.EncodeToString(subSigRaw)))

	u1, _ := url.Parse("https://wit1.example.com")
	w1, err := tessera.NewWitness(vkey1, u1)
	if err != nil {
		t.Fatalf("NewWitness: %v", err)
	}
	policy := tessera.NewWitnessGroup(1, w1)

	gw := &Gateway{
		witnesses: map[witnessKey]witness{
			{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
				client: &mockSubtreeClient{
					signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
						return subSigLine, nil
					},
				},
				verifier:   ver1,
				cosignerID: []byte{0x01},
			},
		},
		policy: policy,
	}

	verified, err := gw.CosignSubtree(context.Background(), origin, logVer, start, end, root, nil, rawCp)
	if err != nil {
		t.Fatalf("CosignSubtree: %v", err)
	}

	if len(verified) != 1 {
		t.Fatalf("got %d verified sigs, want 1", len(verified))
	}
	if !bytes.Equal(verified[0].Signature, rawSubSig) {
		t.Errorf("got signature %x, want %x", verified[0].Signature, rawSubSig)
	}
}

func TestGateway_CosignSubtree_PolicyNotSatisfied(t *testing.T) {
	origin := "example.com/log"
	start := uint64(0)
	end := uint64(1024)
	root := bytes.Repeat([]byte{0xcc}, 32)

	logSKey, logVKey, _ := f_note.GenerateMLDSAKey(origin)
	logSigner, _ := f_note.NewMLDSASigner(logSKey)
	logVer, _ := f_note.NewMLDSAVerifier(logVKey)

	cpBody := fmt.Sprintf("%s\n%d\n%s\n", origin, end, base64.StdEncoding.EncodeToString(root))
	logCpSig, _ := logSigner.Sign([]byte(cpBody))
	logCpSigRaw := binary.BigEndian.AppendUint32(nil, logSigner.KeyHash())
	logCpSigRaw = append(logCpSigRaw, logCpSig...)
	logCpSigLine := fmt.Sprintf("— %s %s\n", logSigner.Name(), base64.StdEncoding.EncodeToString(logCpSigRaw))

	skey1, vkey1, _ := f_note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.101")
	signer1, _ := f_note.NewMLDSASigner(skey1)
	ver1, _ := f_note.NewMLDSAVerifier(vkey1)

	witCpSig, _ := signer1.Sign([]byte(cpBody))
	witCpSigRaw := binary.BigEndian.AppendUint32(nil, signer1.KeyHash())
	witCpSigRaw = append(witCpSigRaw, witCpSig...)
	witCpSigLine := fmt.Sprintf("— %s %s\n", signer1.Name(), base64.StdEncoding.EncodeToString(witCpSigRaw))

	rawCp := append(append([]byte(cpBody), '\n'), []byte(logCpSigLine+witCpSigLine)...)

	u1, _ := url.Parse("https://wit1.example.com")
	w1, _ := tessera.NewWitness(vkey1, u1)
	policy := tessera.NewWitnessGroup(1, w1)

	gw := &Gateway{
		witnesses: map[witnessKey]witness{
			{name: ver1.Name(), keyHash: ver1.KeyHash()}: {
				client: &mockSubtreeClient{
					signFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
						return nil, errors.New("witness down")
					},
				},
				verifier:   ver1,
				cosignerID: []byte{0x01},
			},
		},
		policy: policy,
	}

	_, err := gw.CosignSubtree(context.Background(), origin, logVer, start, end, root, nil, rawCp)
	if !errors.Is(err, ErrPolicyNotSatisfied) {
		t.Fatalf("got error %v, want ErrPolicyNotSatisfied", err)
	}
}

func TestNewGateway_NonSubtreeVerifierSkipped(t *testing.T) {
	skey, vkey, err := note.GenerateKey(nil, "non-subtree-witness")
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	_ = skey
	u, _ := url.Parse("https://wit.example.com")
	w, err := tessera.NewWitness(vkey, u)
	if err != nil {
		t.Fatalf("NewWitness: %v", err)
	}
	policy := tessera.NewWitnessGroup(1, w)

	gw, err := NewGateway(nil, policy)
	if err != nil {
		t.Fatalf("NewGateway returned unexpected error: %v", err)
	}
	if len(gw.witnesses) != 0 {
		t.Errorf("got %d witnesses in gateway, want 0", len(gw.witnesses))
	}
}

func TestGateway_CosignSubtree_EmptyGateway(t *testing.T) {
	origin := "example.com/log"
	start := uint64(0)
	end := uint64(1024)
	root := bytes.Repeat([]byte{0xcc}, 32)
	cpBody := fmt.Sprintf("%s\n%d\n%s\n", origin, end, base64.StdEncoding.EncodeToString(root))

	logSKey, logVKey, _ := f_note.GenerateMLDSAKey(origin)
	logSigner, _ := f_note.NewMLDSASigner(logSKey)
	logVer, _ := f_note.NewMLDSAVerifier(logVKey)

	logCpSig, _ := logSigner.Sign([]byte(cpBody))
	logCpSigRaw := binary.BigEndian.AppendUint32(nil, logSigner.KeyHash())
	logCpSigRaw = append(logCpSigRaw, logCpSig...)
	logCpSigLine := fmt.Sprintf("— %s %s\n", logSigner.Name(), base64.StdEncoding.EncodeToString(logCpSigRaw))

	rawCp := append(append([]byte(cpBody), '\n'), []byte(logCpSigLine)...)

	_, vkey1, _ := f_note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.101")
	u1, _ := url.Parse("https://wit1.example.com")
	w1, _ := tessera.NewWitness(vkey1, u1)

	// Policy requires 1 witness, but gateway has 0 witnesses
	gw := &Gateway{
		witnesses: map[witnessKey]witness{},
		policy:    tessera.NewWitnessGroup(1, w1),
	}

	_, err := gw.CosignSubtree(context.Background(), origin, logVer, start, end, root, nil, rawCp)
	if !errors.Is(err, ErrPolicyNotSatisfied) {
		t.Fatalf("got error %v, want ErrPolicyNotSatisfied", err)
	}

	// Policy requires 0 witnesses (empty group)
	gwEmptyPolicy := &Gateway{
		witnesses: map[witnessKey]witness{},
		policy:    tessera.WitnessGroup{},
	}
	sigs, err := gwEmptyPolicy.CosignSubtree(context.Background(), origin, logVer, start, end, root, nil, rawCp)
	if err != nil {
		t.Fatalf("CosignSubtree with empty policy: %v", err)
	}
	if len(sigs) != 0 {
		t.Errorf("got %d sigs, want 0", len(sigs))
	}
}
