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

package witness

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/transparency-dev/formats/log"
	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle/rfc6962"
	"golang.org/x/mod/sumdb/note"
)

var (
	// https://go.dev/play/p/FVJgyhl7URt to regenerate any messages if needed.
	mPK = "monkeys+db4d9f7e+AULaJMvTtDLHPUcUrjdDad9vDlh/PTfC2VV60JUtCfWT"
	mSK = "PRIVATE+KEY+monkeys+db4d9f7e+ATWIAF3yVBG+Hv1rZFQoNt/BaURkLPtOFMAM2HrEeIr6"
	// wPK       = "witness+f13a86db+AdYV1Ztajd9BvyjP2HgpwrqYL6TjOwIjGMOq8Bu42xbN"
	wSK       = "PRIVATE+KEY+witness+f13a86db+AaLa/dfyBhyo/m0Z7WCi98ENVZWtrP8pxgRNrx7tIWiA"
	mInit     = []byte("monkeys\n5\n41smjBUiAU70EtKlT6lIOIYtRTYxYXsDB+XHfcvu/BE=\n\n— monkeys 202fftzGl3LVoqjXfwCFZZXs8I+5G22+Ek2K0AOyBuSJ/8/CZawNF+6fNlTKOCd622pbzJNkkJFWuw9DbicZCkEx9AY=\n")
	mNext     = []byte("monkeys\n8\nV8K9aklZ4EPB+RMOk1/8VsJUdFZR77GDtZUQq84vSbo=\n\n— monkeys 202ffoUEboiQYpHzICeaFmoy3RNviHTpAxYrq/eO4QQVQMvu9UebKBMX2MJC76NLthZaKsnKbCA8GxrjePZhvDCH7Ag=\n")
	consProof = [][]byte{
		dh("b9e1d62618f7fee8034e4c5010f727ab24d8e4705cb296c374bf2025a87a10d2", 32),
		dh("aac66cd7a79ce4012d80762fe8eec3a77f22d1ca4145c3f4cee022e7efcd599d", 32),
		dh("89d0f753f66a290c483b39cd5e9eafb12021293395fad3d4a2ad053cfbcfdc9e", 32),
		dh("29e40bb79c966f4c6fe96aff6f30acfce5f3e8d84c02215175d6e018a5dee833", 32),
	}
)

type logOpts struct {
	origin string
	PK     string
}

func newWitness(t *testing.T, log logOpts, p *testPersistence) *Witness {
	// Set up Opts for the witness.
	ns, err := f_note.NewSignerForCosignatureV1(wSK)
	if err != nil {
		t.Fatalf("couldn't create a witness signer: %v", err)
	}
	logV, err := note.NewVerifier(log.PK)
	if err != nil {
		t.Fatalf("couldn't create a log verifier: %v", err)
	}
	opts := Opts{
		Persistence: p,
		Signers:     []note.Signer{ns},
		LogVerifier: logV,
	}
	// Create the witness
	w, err := New(t.Context(), opts)
	if err != nil {
		t.Fatalf("couldn't create witness: %v", err)
	}
	return w
}

// dh is taken from https://github.com/google/trillian/blob/master/merkle/logverifier/log_verifier_test.go.
func dh(h string, expLen int) []byte {
	r, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	if got := len(r); got != expLen {
		panic(fmt.Sprintf("decode %q: len=%d, want %d", h, got, expLen))
	}
	return r
}

func mustCreateCheckpoint(t *testing.T, sk string, origin string, size uint64, rootHash []byte) []byte {
	t.Helper()
	cp := log.Checkpoint{
		Origin: origin,
		Size:   size,
		Hash:   rootHash,
	}
	signer, err := note.NewSigner(sk)
	if err != nil {
		t.Fatal(err)
	}

	msg, err := note.Sign(&note.Note{Text: string(cp.Marshal())}, signer)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func TestUpdate(t *testing.T) {
	for _, test := range []struct {
		desc       string
		origin     string
		initC      []byte
		oldSize    uint64
		newC       []byte
		pf         [][]byte
		wantUpdate bool
		wantError  error
	}{
		{
			desc:       "vanilla consistency happy path",
			origin:     "monkeys",
			initC:      mustCreateCheckpoint(t, mSK, "monkeys", 5, dh("e35b268c1522014ef412d2a54fa94838862d453631617b0307e5c77dcbeefc11", 32)),
			oldSize:    5,
			newC:       mNext,
			pf:         consProof,
			wantUpdate: true,
		}, {
			desc:      "oldSize doesn't match current state",
			origin:    "monkeys",
			initC:     mustCreateCheckpoint(t, mSK, "monkeys", 5, dh("e35b268c1522014ef412d2a54fa94838862d453631617b0307e5c77dcbeefc11", 32)),
			oldSize:   2,
			newC:      mNext,
			wantError: ErrCheckpointStale,
		}, {
			desc:    "vanilla consistency starting from tree size 0 with proof",
			origin:  "monkeys",
			initC:   mustCreateCheckpoint(t, mSK, "monkeys", 0, rfc6962.DefaultHasher.EmptyRoot()),
			oldSize: 0,
			newC:    mustCreateCheckpoint(t, mSK, "monkeys", 5, dh("e35b268c1522014ef412d2a54fa94838862d453631617b0307e5c77dcbeefc11", 32)),
			pf:      consProof,
			// Proof should be empty for oldSize=zero.
			wantError: ErrInvalidProof,
		}, {
			desc:       "vanilla resubmit known CP",
			origin:     "monkeys",
			initC:      mustCreateCheckpoint(t, mSK, "monkeys", 5, dh("e35b268c1522014ef412d2a54fa94838862d453631617b0307e5c77dcbeefc11", 32)),
			oldSize:    5,
			newC:       mustCreateCheckpoint(t, mSK, "monkeys", 5, dh("e35b268c1522014ef412d2a54fa94838862d453631617b0307e5c77dcbeefc11", 32)),
			wantUpdate: true,
		}, {
			desc:      "resubmit known CP with changed root",
			origin:    "monkeys",
			initC:     mustCreateCheckpoint(t, mSK, "monkeys", 5, dh("e35b268c1522014ef412d2a54fa94838862d453631617b0307e5c77dcbeefc11", 32)),
			oldSize:   5,
			newC:      mustCreateCheckpoint(t, mSK, "monkeys", 5, dh("fffffffffffffffef412d2a54fa94838862d453631617b0307e5c77dcbeefc11", 32)),
			wantError: ErrRootMismatch,
		}, {
			desc:      "missing proof",
			origin:    "monkeys",
			initC:     mustCreateCheckpoint(t, mSK, "monkeys", 4, dh("e35b268c1522014ef412d2a54fa94838862d453631617b0307e5c77dcbeefc11", 32)),
			oldSize:   4,
			newC:      mustCreateCheckpoint(t, mSK, "monkeys", 5, dh("e35b268c1522014ef412d2a54fa94838862d453631617b0307e5c77dcbeefc11", 32)),
			pf:        [][]byte{},
			wantError: ErrInvalidProof,
		}, {
			desc:      "submit smaller checkpoint",
			initC:     mNext,
			oldSize:   8,
			newC:      mInit,
			pf:        consProof,
			wantError: ErrOldSizeInvalid,
		}, {
			desc:    "vanilla consistency garbage proof",
			initC:   mInit,
			oldSize: 5,
			newC:    mNext,
			pf: [][]byte{
				dh("aaaa", 2),
				dh("bbbb", 2),
				dh("cccc", 2),
				dh("dddd", 2),
			},
			wantError: ErrInvalidProof,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			ctx := context.Background()
			p := newPersistence()
			// Set up witness.
			w := newWitness(t, logOpts{
				origin: "monkeys",
				PK:     mPK,
			}, p)
			// Set an initial checkpoint for the log.
			if _, _, err := w.Update(ctx, 0, test.initC, nil); err != nil {
				t.Errorf("failed to set checkpoint: %v", err)
			}
			// Now update from this checkpoint to a newer one.
			_, _, err := w.Update(ctx, test.oldSize, test.newC, test.pf)
			if err != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("Got error %v, want %v", err, test.wantError)
				}
				return
			}
			if test.wantUpdate {
				curCP, err := p.latest(ctx)
				if err != nil {
					t.Fatalf("failed to get latest checkpoint: %v", err)
				}
				// curCP should be a prefix of the new CP as the witness will have added a signature.
				if !bytes.HasPrefix(curCP, test.newC) {
					t.Fatalf("updated checkpoint != test.newC:\nGot:\n%s\nWant:\n%s\n", curCP, test.newC)
				}
			}
		})
	}
}

func newPersistence() *testPersistence {
	return &testPersistence{}
}

type testPersistence struct {
	mu         sync.RWMutex
	checkpoint []byte
}

func (p *testPersistence) latest(_ context.Context) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.checkpoint, nil
}

func (p *testPersistence) UpdateCheckpoint(_ context.Context, f func([]byte) ([]byte, error)) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	u, err := f(p.checkpoint)
	if err != nil {
		return err
	}

	bits := bytes.Split(u, []byte{'\n'})
	if len(bits) == 0 {
		return errors.New("invalid checkpoint")
	}

	p.checkpoint = u
	return nil
}
