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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	"testing"

	fnote "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"golang.org/x/mod/sumdb/note"
)

type fakeMirrorWriter struct {
	integrateFunc        func(ctx context.Context, from uint64, bundles iter.Seq2[*api.EntryBundle, error]) (uint64, []byte, error)
	sizeFunc             func(ctx context.Context) (uint64, error)
	updateCheckpointFunc func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error
}

func (f *fakeMirrorWriter) IntegrateBundles(ctx context.Context, from uint64, bundles iter.Seq2[*api.EntryBundle, error]) (uint64, []byte, error) {
	if f.integrateFunc != nil {
		return f.integrateFunc(ctx, from, bundles)
	}
	return from, nil, nil
}

func (f *fakeMirrorWriter) IntegratedSize(ctx context.Context) (uint64, error) {
	if f.sizeFunc != nil {
		return f.sizeFunc(ctx)
	}
	return 0, nil
}

func (f *fakeMirrorWriter) UpdateCheckpoint(ctx context.Context, g func(oldCP []byte) (newCP []byte, err error)) error {
	if f.updateCheckpointFunc != nil {
		return f.updateCheckpointFunc(ctx, g)
	}
	return nil
}

type fakeLogReader struct {
	sizeFunc            func(ctx context.Context) (uint64, error)
	readEntryBundleFunc func(ctx context.Context, index uint64, p uint8) ([]byte, error)
}

func (f *fakeLogReader) IntegratedSize(ctx context.Context) (uint64, error) {
	if f.sizeFunc != nil {
		return f.sizeFunc(ctx)
	}
	return 0, nil
}
func (f *fakeLogReader) ReadCheckpoint(ctx context.Context) ([]byte, error) { return nil, nil }
func (f *fakeLogReader) ReadTile(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
	return nil, nil
}
func (f *fakeLogReader) ReadEntryBundle(ctx context.Context, index uint64, p uint8) ([]byte, error) {
	if f.readEntryBundleFunc != nil {
		return f.readEntryBundleFunc(ctx, index, p)
	}
	return nil, nil
}
func (f *fakeLogReader) NextIndex(ctx context.Context) (uint64, error) { return 0, nil }

const (
	testPendingCPOrigin = "test-origin"
	testPendingCPSize   = uint64(512)
	testPendingCPRoot   = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="

	testMirrorOrigin = "test-mirror-origin"
)

var (
	testLogSigner, testLogVerifier = mustGenerateKey(testPendingCPOrigin)
	testMirrorSigner, _            = mustGenerateKey(testMirrorOrigin)
	testPendingCP                  = mustSignCP(testPendingCPOrigin, testPendingCPSize, testPendingCPRoot, testLogSigner)
)

func TestTicketRoundTrip(t *testing.T) {
	mt := &MirrorTarget{
		logVerifier: testLogVerifier,
		cpSource: func(ctx context.Context) ([]byte, error) {
			return testPendingCP, nil
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testPendingCPSize, nil },
		},
	}

	ticket, _, _, err := mt.createNewTicket(t.Context())
	if err != nil {
		t.Fatalf("createNewTicket failed: %v", err)
	}

	mt.cpSource = func(ctx context.Context) ([]byte, error) {
		t.Fatalf("cpSource called, but ticket should not be stale")
		return nil, nil // Unreachable.
	}

	_, _, oldTicketValid, newTicket, _, _, err := mt.openOrCreateTicket(t.Context(), ticket, testPendingCPSize)
	if err != nil {
		t.Fatalf("openOrCreateTicket: %v", err)
	}

	if !oldTicketValid {
		t.Fatalf("openOrCreateTicket: oldTicketValid = %v, want true", oldTicketValid)
	}

	if !bytes.Equal(ticket, newTicket) {
		t.Fatalf("ticket should not have been updated")
	}
}

func TestCreateNewTicket(t *testing.T) {
	testPendingCPLessOne := mustSignCP(testPendingCPOrigin, testPendingCPSize-1, testPendingCPRoot, testLogSigner)
	mt := &MirrorTarget{
		logVerifier: testLogVerifier,
		cpSource: func(ctx context.Context) ([]byte, error) {
			return testPendingCPLessOne, nil
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testPendingCPSize, nil },
		},
	}

	// Create a ticket for size CP-1
	ticket, _, _, err := mt.createNewTicket(t.Context())
	if err != nil {
		t.Fatalf("createNewTicket failed: %v", err)
	}

	// Then move the pending checkpoint to the next size along.
	// This will allow us to determine when we get a fresh ticket back.
	mt.cpSource = func(ctx context.Context) ([]byte, error) {
		return testPendingCP, nil
	}

	for _, test := range []struct {
		name            string
		ticket          []byte
		expectedSize    uint64
		wantTicketValid bool
		wantNewTicket   bool
		wantConflict    bool
	}{
		{
			name:            "ticket valid",
			ticket:          ticket,
			expectedSize:    testPendingCPSize - 1, // Expect old size
			wantTicketValid: true,
		}, {
			name:            "ticket stale",
			ticket:          ticket,
			expectedSize:    testPendingCPSize, // Expect new size because we want a new ticket.
			wantTicketValid: true,
			wantNewTicket:   true,
			wantConflict:    true,
		}, {
			name:          "ticket corrupt",
			ticket:        append(append([]byte{}, ticket[:len(ticket)-1]...), ticket[len(ticket)-1]^0xff),
			expectedSize:  testPendingCPSize, // Expect new size because we want a new ticket.
			wantNewTicket: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, oldTicketValid, newTicket, _, _, err := mt.openOrCreateTicket(t.Context(), test.ticket, test.expectedSize)
			if err != nil {
				gotConflict := errors.Is(err, ErrConflict)
				if gotConflict != test.wantConflict {
					t.Fatalf("openOrCreateTicket: %v want err %v", err, test.wantConflict)
				}
				if !gotConflict {
					t.Fatalf("openOrCreateTicket: %v", err)
				}
			}

			if test.wantTicketValid != oldTicketValid {
				t.Fatalf("openOrCreateTicket: newTicket Valid = %v, want %v", oldTicketValid, test.wantTicketValid)
			}
			if test.wantNewTicket {
				if bytes.Equal(test.ticket, newTicket) {
					t.Fatalf("ticket should have been updated")
				}
			} else {
				if !bytes.Equal(test.ticket, newTicket) {
					t.Fatalf("ticket should not have been updated")
				}
			}
		})
	}

}

func TestMirrorTarget_SealAndOpen(t *testing.T) {
	mt := &MirrorTarget{}

	ticket, err := mt.seal([]byte(testPendingCP))
	if err != nil {
		t.Fatalf("seal failed: %v", err)
	}

	if newTicket, err := mt.open(ticket); err != nil {
		t.Errorf("Failed to open untampered: %v", err)
	} else if !bytes.Equal(newTicket, testPendingCP) {
		t.Errorf("open: eturn unexpected bytes")
	}

	for i := 0; i < len(ticket); i++ {
		b := ticket[i]
		ticket[i] = b ^ 0xff
		if _, err := mt.open(ticket); err == nil {
			t.Errorf("open: tampering did not fail")
		}
		ticket[i] = b
	}
}

func TestMirrorTarget_AddEntries_NoTicket(t *testing.T) {
	const (
		testIntegratedSize = uint64(100)
	)
	ctx := context.Background()
	mt := &MirrorTarget{
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		writer: &fakeMirrorWriter{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
			updateCheckpointFunc: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
				return nil
			},
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
		cpSource: func(ctx context.Context) ([]byte, error) {
			return []byte(testPendingCP), nil
		},
	}

	nextEntry, pendingSize, newTicket, _, err := mt.AddEntries(ctx, 0, 0, nil, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("got %v, want ErrConflict", err)
	}
	if got, want := nextEntry, testIntegratedSize; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	if got, want := pendingSize, testPendingCPSize; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	if len(newTicket) == 0 {
		t.Errorf("got empty ticket, want non-empty")
	}
}

func TestMirrorTarget_AddEntries_CompleteUpload(t *testing.T) {
	const (
		testIntegratedSize = uint64(256)
		testUploadStart    = uint64(256)
		testUploadEnd      = testPendingCPSize
	)

	ctx := context.Background()
	var gotUpdatedCP []byte
	mt := &MirrorTarget{
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		writer: &fakeMirrorWriter{
			integrateFunc: func(ctx context.Context, fromBundleIdx uint64, bundles iter.Seq2[*api.EntryBundle, error]) (uint64, []byte, error) {
				if fromBundleIdx != testUploadStart/layout.EntryBundleWidth {
					return 0, nil, fmt.Errorf("got from %d want %d", fromBundleIdx, testUploadStart/layout.EntryBundleWidth)
				}
				pendingCPRoot, err := base64.StdEncoding.DecodeString(testPendingCPRoot)
				return testUploadEnd, pendingCPRoot, err
			},
			updateCheckpointFunc: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
				cp, err := f(nil)
				gotUpdatedCP = cp
				return err
			},
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
		cpSource: func(ctx context.Context) ([]byte, error) { return []byte(testPendingCP), nil },
	}

	validTicket, err := mt.seal([]byte(testPendingCP))
	if err != nil {
		t.Fatalf("seal failed: %v", err)
	}
	nextEntry, pendingSize, _, cosigs, err := mt.AddEntries(ctx, testUploadStart, testUploadEnd, validTicket, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := nextEntry, testUploadEnd; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	if got, want := pendingSize, testUploadEnd; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	if len(cosigs) == 0 {
		t.Errorf("got empty cosigs, want non-empty")
	}
	if wantCP := append([]byte(testPendingCP), cosigs...); !bytes.Equal(gotUpdatedCP, wantCP) {
		t.Errorf("got updated CP %x, want %x", gotUpdatedCP, wantCP)
	}
}

func TestMirrorTarget_AddEntries_ZeroCheckpoint(t *testing.T) {
	testPendingCPZero := mustSignCP(testPendingCPOrigin, 0, testPendingCPRoot, testLogSigner)
	ctx := context.Background()
	var gotUpdatedCP []byte
	mt := &MirrorTarget{
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		writer: &fakeMirrorWriter{
			integrateFunc: func(ctx context.Context, fromBundleIdx uint64, bundles iter.Seq2[*api.EntryBundle, error]) (uint64, []byte, error) {
				pendingCPRoot, err := base64.StdEncoding.DecodeString(testPendingCPRoot)
				return 0, pendingCPRoot, err
			},
			updateCheckpointFunc: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
				cp, err := f(nil)
				gotUpdatedCP = cp
				return err
			},
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return 0, nil },
		},
		cpSource: func(ctx context.Context) ([]byte, error) { return []byte(testPendingCPZero), nil },
	}

	// 1. First call with no ticket and uploadStart=0, uploadEnd=0.
	// This should request the initial mirror info/ticket and return ErrConflict.
	nextEntry, pendingSize, ticket, cosigs, err := mt.AddEntries(ctx, 0, 0, nil, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("first call got err: %v, want ErrConflict", err)
	}
	if nextEntry != 0 {
		t.Errorf("first call got nextEntry %d, want 0", nextEntry)
	}
	if pendingSize != 0 {
		t.Errorf("first call got pendingSize %d, want 0", pendingSize)
	}
	if len(ticket) == 0 {
		t.Fatalf("first call got empty ticket, want a valid ticket")
	}
	if len(cosigs) != 0 {
		t.Errorf("first call got cosigs, want none")
	}

	// 2. Second call with the ticket returned from the first call.
	// This should succeed and return the cosignature.
	nextEntry, pendingSize, _, cosigs, err = mt.AddEntries(ctx, 0, 0, ticket, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if err != nil {
		t.Fatalf("second call got err: %v, want nil", err)
	}
	if nextEntry != 0 {
		t.Errorf("second call got nextEntry %d, want 0", nextEntry)
	}
	if pendingSize != 0 {
		t.Errorf("second call got pendingSize %d, want 0", pendingSize)
	}
	if len(cosigs) == 0 {
		t.Errorf("second call got empty cosigs, want non-empty")
	}
	if wantCP := append([]byte(testPendingCPZero), cosigs...); !bytes.Equal(gotUpdatedCP, wantCP) {
		t.Errorf("got updated CP %s, want %s", gotUpdatedCP, wantCP)
	}
}

func mustGenerateKey(origin string) (note.Signer, note.Verifier) {
	sk, vk, err := fnote.GenerateMLDSAKey(origin)
	if err != nil {
		panic(fmt.Errorf("Failed to generate key for %q: %v", origin, err))
	}
	s, err := fnote.NewSignerForCosignatureV1(sk)
	if err != nil {
		panic(fmt.Errorf("Failed to instantiate signer: %v", err))
	}
	v, err := fnote.NewVerifierForCosignatureV1(vk)
	if err != nil {
		panic(fmt.Errorf("Failed to instantiate verifier: %v", err))
	}
	return s, v
}

func mustSignCP(origin string, size uint64, root string, s note.Signer) []byte {
	raw, err := note.Sign(&note.Note{Text: fmt.Sprintf("%s\n%d\n%s\n", origin, size, root)}, s)
	if err != nil {
		panic(fmt.Errorf("Failed to sign note: %v", err))
	}
	return raw
}

func TestMirrorTarget_AddEntries_VerifySubtreeProof(t *testing.T) {
	const (
		testIntegratedSize = uint64(256)
		testUploadStart    = uint64(256)
		testUploadEnd      = testPendingCPSize
	)

	pkg := &MirrorPackage{
		Entries: [][]byte{[]byte("entry1"), []byte("entry2")},
		Proof:   [][]byte{[]byte("proof1")},
	}

	for _, tc := range []struct {
		name      string
		verifyErr error
		wantErr   error
	}{
		{
			name:      "success",
			verifyErr: nil,
			wantErr:   nil,
		},
		{
			name:      "proof verification failure",
			verifyErr: errors.New("oh noes, proof verification failed"),
			wantErr:   ErrInvalidProof,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			var verifyCalled bool
			var gotStart, gotEnd, gotSize uint64
			var gotProof [][]byte
			var gotRoot []byte

			mt := &MirrorTarget{
				logVerifier: testLogVerifier,
				signer:      testMirrorSigner,
				writer: &fakeMirrorWriter{
					integrateFunc: func(ctx context.Context, from uint64, bundles iter.Seq2[*api.EntryBundle, error]) (uint64, []byte, error) {
						for _, err := range bundles {
							if err != nil {
								return 0, nil, err
							}
						}
						pendingCPRoot, err := base64.StdEncoding.DecodeString(testPendingCPRoot)
						return testUploadEnd, pendingCPRoot, err
					},
				},
				reader: &fakeLogReader{
					sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
				},
				cpSource: func(ctx context.Context) ([]byte, error) { return []byte(testPendingCP), nil },
				verifySubtreeProof: func(hasher merkle.LogHasher, start, end, size uint64, proof [][]byte, subRoot, root []byte) error {
					verifyCalled = true
					gotStart = start
					gotEnd = end
					gotSize = size
					gotProof = proof
					gotRoot = root
					return tc.verifyErr
				},
			}

			validTicket, err := mt.seal([]byte(testPendingCP))
			if err != nil {
				t.Fatalf("seal failed: %v", err)
			}

			firstCall := true
			nextFunc := func() (*MirrorPackage, error) {
				if firstCall {
					firstCall = false
					return pkg, nil
				}
				return nil, io.EOF
			}

			_, _, _, _, err = mt.AddEntries(ctx, testUploadStart, testUploadEnd, validTicket, nextFunc)
			if (err != nil) != (tc.wantErr != nil) {
				t.Fatalf("got error: %v, want error: %v", err, tc.wantErr)
			} else if err != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("unexpected error type: %v", err)
			}

			if !verifyCalled {
				t.Errorf("verifySubtreeProof was not called")
				return
			}

			if gotStart != testUploadStart {
				t.Errorf("verifySubtreeProof start: got %d, want %d", gotStart, testUploadStart)
			}
			if want := testUploadStart + uint64(len(pkg.Entries)); gotEnd != want {
				t.Errorf("verifySubtreeProof end: got %d, want %d", gotEnd, want)
			}
			if gotSize != testPendingCPSize {
				t.Errorf("verifySubtreeProof size: got %d, want %d", gotSize, testPendingCPSize)
			}
			wantRoot, err := base64.StdEncoding.DecodeString(testPendingCPRoot)
			if err != nil {
				t.Fatalf("failed to decode root hash: %v", err)
			}
			if !bytes.Equal(gotRoot, wantRoot) {
				t.Errorf("verifySubtreeProof root: got %x, want %x", gotRoot, wantRoot)
			}
			if len(gotProof) != 1 || !bytes.Equal(gotProof[0], pkg.Proof[0]) {
				t.Errorf("verifySubtreeProof proof: got %x, want %x", gotProof, pkg.Proof)
			}
		})
	}
}

func TestMirrorTarget_AddEntries_Unaligned_PadsFirstBundle(t *testing.T) {
	const (
		testIntegratedSize = uint64(270)
		testUploadStart    = uint64(270) // not aligned: 270 % 256 = 14
		testUploadEnd      = testPendingCPSize
	)

	var readEntryBundleCalled bool

	padEntries := testUploadStart % layout.EntryBundleWidth
	padBundleRaw := make([]byte, 2*padEntries)

	mt := &MirrorTarget{
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		writer: &fakeMirrorWriter{
			integrateFunc: func(ctx context.Context, fromBundleIdx uint64, bundles iter.Seq2[*api.EntryBundle, error]) (uint64, []byte, error) {
				if want := testUploadStart / layout.EntryBundleWidth; fromBundleIdx != want {
					return 0, nil, fmt.Errorf("got fromBundleIdx %d want %d", fromBundleIdx, want)
				}
				for b, err := range bundles {
					if err != nil {
						return 0, nil, err
					}
					if got, want := uint64(len(b.Entries)), testUploadStart%layout.EntryBundleWidth; got != want {
						return 0, nil, fmt.Errorf("got %d entries in bundle, want %d", got, want)
					}
				}
				pendingCPRoot, err := base64.StdEncoding.DecodeString(testPendingCPRoot)
				return testUploadEnd, pendingCPRoot, err
			},
			updateCheckpointFunc: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
				_, err := f(nil)
				return err
			},
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
			readEntryBundleFunc: func(ctx context.Context, index uint64, p uint8) ([]byte, error) {
				readEntryBundleCalled = true
				if got, want := index, testUploadStart/layout.EntryBundleWidth; got != want {
					t.Errorf("ReadEntryBundle index: got %d, want %d", got, want)
				}
				if got, want := p, uint8(testUploadStart%layout.EntryBundleWidth); got != want {
					t.Errorf("ReadEntryBundle p: got %d, want %d", got, want)
				}
				return padBundleRaw, nil
			},
		},
		cpSource: func(ctx context.Context) ([]byte, error) { return []byte(testPendingCP), nil },
	}

	validTicket, err := mt.seal([]byte(testPendingCP))
	if err != nil {
		t.Fatalf("seal failed: %v", err)
	}

	_, _, _, _, err = mt.AddEntries(t.Context(), testUploadStart, testUploadEnd, validTicket, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !readEntryBundleCalled {
		t.Errorf("ReadEntryBundle was not called")
	}
}

func TestMirrorTarget_AddEntries_NoPendingCheckpoint(t *testing.T) {
	const (
		testIntegratedSize = uint64(100)
	)
	ctx := t.Context()

	mt := &MirrorTarget{
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		writer: &fakeMirrorWriter{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
		cpSource: func(ctx context.Context) ([]byte, error) {
			return nil, nil // No pending checkpoint
		},
	}

	_, _, _, _, err := mt.AddEntries(ctx, testIntegratedSize, testIntegratedSize+10, nil, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if !errors.Is(err, ErrNoPendingCheckpoint) {
		t.Errorf("got error %v, want ErrNoPendingCheckpoint", err)
	}
}
