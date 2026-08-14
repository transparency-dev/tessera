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
	"strings"
	"testing"

	fnote "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle"
	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/witness/witness"
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

const (
	testPendingCPOrigin = "test-origin"
	testPendingCPSize   = uint64(512)
	testPendingCPRoot   = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="

	testMirrorOrigin = "test-mirror-origin"
)

var (
	testLogSigner, testLogVerifier       = mustGenerateKey(testPendingCPOrigin)
	testMirrorSigner, testMirrorVerifier = mustGenerateKey(testMirrorOrigin)
	testPendingCP                        = mustSignCP(testPendingCPOrigin, testPendingCPSize, testPendingCPRoot, testLogSigner)
)

func TestTicketRoundTrip(t *testing.T) {
	mt := &MirrorTarget{
		origin:      testPendingCPOrigin,
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
		origin:      testPendingCPOrigin,
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
		origin:      testPendingCPOrigin,
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
	drv := &fakeDriver{
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
	}
	mt, err := NewMirrorTarget(t.Context(), drv, &MirrorOptions{
		origin:      testPendingCPOrigin,
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		cpSource: func(ctx context.Context) ([]byte, error) {
			return []byte(testPendingCP), nil
		},
	})
	if err != nil {
		t.Fatalf("NewMirrorTarget failed: %v", err)
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
		t.Fatalf("got empty cosigs, want non-empty")
	}
	if wantCP := append([]byte(testPendingCP), cosigs...); !bytes.Equal(gotUpdatedCP, wantCP) {
		t.Errorf("got updated CP %s, want %s", gotUpdatedCP, wantCP)
	}
}

func TestMirrorTarget_AddEntries_ZeroCheckpoint(t *testing.T) {
	testPendingCPZero := mustSignCP(testPendingCPOrigin, 0, testPendingCPRoot, testLogSigner)
	ctx := t.Context()
	var gotUpdatedCP []byte

	drv := &fakeDriver{
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
	}
	mt, err := NewMirrorTarget(t.Context(), drv, &MirrorOptions{
		origin:      testPendingCPOrigin,
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		cpSource: func(ctx context.Context) ([]byte, error) {
			return []byte(testPendingCPZero), nil
		},
	})
	if err != nil {
		t.Fatalf("NewMirrorTarget failed: %v", err)
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

func mustGenerateKey(origin string) (fnote.SubtreeSigner, fnote.SubtreeVerifier) {
	sk, vk, err := fnote.GenerateMLDSAKey(origin)
	if err != nil {
		panic(fmt.Errorf("Failed to generate key for %q: %v", origin, err))
	}
	s, err := fnote.NewMLDSASigner(sk)
	if err != nil {
		panic(fmt.Errorf("Failed to instantiate signer: %v", err))
	}
	v, err := fnote.NewMLDSAVerifier(vk)
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
			ctx := t.Context()

			var verifyCalled bool
			var gotStart, gotEnd, gotSize uint64
			var gotProof [][]byte
			var gotRoot []byte

			drv := &fakeDriver{
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
			}
			mt, err := NewMirrorTarget(ctx, drv, &MirrorOptions{
				origin:      testPendingCPOrigin,
				logVerifier: testLogVerifier,
				signer:      testMirrorSigner,
				cpSource:    func(ctx context.Context) ([]byte, error) { return []byte(testPendingCP), nil },
			})
			if err != nil {
				t.Fatalf("NewMirrorTarget() failed: %v", err)
			}

			mt.verifySubtreeProof = func(hasher merkle.LogHasher, start, end, size uint64, proof [][]byte, subRoot, root []byte) error {
				verifyCalled = true
				gotStart = start
				gotEnd = end
				gotSize = size
				gotProof = proof
				gotRoot = root
				return tc.verifyErr
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

	drv := &fakeDriver{
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
			readEntryBundle: func(ctx context.Context, index uint64, p uint8) ([]byte, error) {
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
	}

	mt, err := NewMirrorTarget(t.Context(), drv, &MirrorOptions{
		origin:      testPendingCPOrigin,
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		cpSource:    func(ctx context.Context) ([]byte, error) { return []byte(testPendingCP), nil },
	})
	if err != nil {
		t.Fatalf("NewMirrorTarget() failed: %v", err)
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

	drv := &fakeDriver{
		writer: &fakeMirrorWriter{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
	}
	mt, err := NewMirrorTarget(ctx, drv, &MirrorOptions{
		origin:      testPendingCPOrigin,
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		cpSource: func(ctx context.Context) ([]byte, error) {
			return nil, nil // No pending checkpoint
		},
	})
	if err != nil {
		t.Fatalf("NewMirrorTarget() failed: %v", err)
	}

	_, _, _, _, err = mt.AddEntries(ctx, testIntegratedSize, testIntegratedSize+10, nil, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if !errors.Is(err, ErrNoPendingCheckpoint) {
		t.Errorf("got error %v, want ErrNoPendingCheckpoint", err)
	}
}

func TestMirrorTarget_AddEntries_UploadStartConflicts(t *testing.T) {
	const (
		testIntegratedSize = uint64(3072)
		testUploadEnd      = uint64(4096)
	)

	ctx := t.Context()
	testPendingCPCustom := mustSignCP(testPendingCPOrigin, testUploadEnd, testPendingCPRoot, testLogSigner)

	for _, tc := range []struct {
		name         string
		uploadStart  uint64
		wantConflict bool
	}{
		{
			name:         "exact start",
			uploadStart:  testIntegratedSize,
			wantConflict: false,
		},
		{
			name:         "re-upload within limit",
			uploadStart:  testIntegratedSize - maxExcessEntries,
			wantConflict: false,
		},
		{
			name:         "re-upload too far back",
			uploadStart:  testIntegratedSize - maxExcessEntries - 1,
			wantConflict: true,
		},
		{
			name:         "uploadStart > nextEntry",
			uploadStart:  testIntegratedSize + 256,
			wantConflict: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drv := &fakeDriver{
				writer: &fakeMirrorWriter{
					integrateFunc: func(ctx context.Context, fromBundleIdx uint64, bundles iter.Seq2[*api.EntryBundle, error]) (uint64, []byte, error) {
						for _, err := range bundles {
							if err != nil {
								return 0, nil, err
							}
						}
						pendingCPRoot, err := base64.StdEncoding.DecodeString(testPendingCPRoot)
						return testUploadEnd, pendingCPRoot, err
					},
					updateCheckpointFunc: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
						return nil
					},
				},
				reader: &fakeLogReader{
					sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
				},
			}
			mt, err := NewMirrorTarget(ctx, drv, &MirrorOptions{
				origin:      testPendingCPOrigin,
				logVerifier: testLogVerifier,
				signer:      testMirrorSigner,
				cpSource:    func(ctx context.Context) ([]byte, error) { return []byte(testPendingCPCustom), nil },
			})
			if err != nil {
				t.Fatalf("NewMirrorTarget() failed: %v", err)
			}

			validTicket, err := mt.seal([]byte(testPendingCPCustom))
			if err != nil {
				t.Fatalf("seal failed: %v", err)
			}

			_, _, _, _, err = mt.AddEntries(ctx, tc.uploadStart, testUploadEnd, validTicket, func() (*MirrorPackage, error) {
				return nil, io.EOF
			})

			if gotConflict := errors.Is(err, ErrConflict); gotConflict != tc.wantConflict {
				t.Fatalf("AddEntries got conflict %v, want conflict %v (err: %v)", gotConflict, tc.wantConflict, err)
			}
			if !tc.wantConflict && err != nil {
				t.Fatalf("AddEntries unexpected error: %v", err)
			}

		})
	}
}

func TestMirrorTarget_CustomOrigin(t *testing.T) {
	_, verifier := mustGenerateKey("verifier-name")
	signer, _ := mustGenerateKey("test-mirror")
	ctx := t.Context()

	for _, tc := range []struct {
		name       string
		origin     string
		wantOrigin string
	}{
		{
			name:       "defaults to verifier name",
			origin:     "",
			wantOrigin: verifier.Name(),
		},
		{
			name:       "respects custom origin",
			origin:     "custom-origin",
			wantOrigin: "custom-origin",
		},
		{
			name:       "respects same origin",
			origin:     verifier.Name(),
			wantOrigin: verifier.Name(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := NewMirrorOptions().
				WithLogVerifier(verifier).
				WithSigner(signer).
				WithCheckpointSource(func(ctx context.Context) ([]byte, error) {
					return nil, nil
				})
			if tc.origin != "" {
				opts = opts.WithOrigin(tc.origin)
			}

			mt, err := NewMirrorTarget(ctx, &fakeDriver{}, opts)
			if err != nil {
				t.Fatalf("NewMirrorTarget: %v", err)
			}
			if mt.origin != tc.wantOrigin {
				t.Errorf("got origin %q, want %q", mt.origin, tc.wantOrigin)
			}
		})
	}
}

func TestMirrorTarget_CustomOrigin_AddEntries(t *testing.T) {
	const (
		customOrigin = "custom-origin"
		verifierName = "verifier-name"
		testSize     = uint64(512)
		testRoot     = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
	)

	ctx := t.Context()

	signer, verifier := mustGenerateKey(verifierName)
	cpRaw := mustSignCP(customOrigin, testSize, testRoot, signer)

	opts := NewMirrorOptions().
		WithLogVerifier(verifier).
		WithSigner(testMirrorSigner).
		WithOrigin(customOrigin).
		WithCheckpointSource(func(ctx context.Context) ([]byte, error) {
			return cpRaw, nil
		})

	mt, err := NewMirrorTarget(ctx, &fakeDriver{}, opts)
	if err != nil {
		t.Fatalf("NewMirrorTarget failed: %v", err)
	}

	mt.writer = &fakeMirrorWriter{
		sizeFunc: func(ctx context.Context) (uint64, error) { return testSize, nil },
		updateCheckpointFunc: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
			return nil
		},
	}
	mt.reader = &fakeLogReader{
		sizeFunc: func(ctx context.Context) (uint64, error) { return testSize, nil },
	}

	_, pendingSize, _, _, err := mt.AddEntries(ctx, 0, 0, nil, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("got error %v, want ErrConflict", err)
	}
	if pendingSize != testSize {
		t.Errorf("expected pending size %d, got %d", testSize, pendingSize)
	}
}

type fakeDriver struct {
	writer MirrorWriter
	reader LogReader
}

func (f *fakeDriver) MirrorWriter(ctx context.Context, opts *MirrorOptions) (MirrorWriter, LogReader, error) {
	w := f.writer
	if w == nil {
		w = &fakeMirrorWriter{}
	}
	r := f.reader
	if r == nil {
		r = &fakeLogReader{}
	}
	return w, r, nil
}

func TestMirrorTarget_SignSubtree(t *testing.T) {
	const treeSize = uint64(8)
	ctx := t.Context()

	// Build a small Merkle tree with 8 leaves.
	rf := &compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}
	nodes := make(map[compact.NodeID][]byte)
	cr := rf.NewEmptyRange(0)
	for i := range treeSize {
		leafHash := rfc6962.DefaultHasher.HashLeaf(fmt.Appendf(nil, "entry-%d", i))
		if err := cr.Append(leafHash, func(id compact.NodeID, hash []byte) {
			nodes[id] = hash
		}); err != nil {
			t.Fatalf("cr.Append: %v", err)
		}
	}
	rootHash, err := cr.GetRootHash(nil)
	if err != nil {
		t.Fatalf("GetRootHash: %v", err)
	}

	// Build subtree [0, 4) root.
	subCR := rf.NewEmptyRange(0)
	for i := range uint64(4) {
		leafHash := rfc6962.DefaultHasher.HashLeaf(fmt.Appendf(nil, "entry-%d", i))
		if err := subCR.Append(leafHash, nil); err != nil {
			t.Fatalf("subCR.Append: %v", err)
		}
	}
	validSubRoot, err := subCR.GetRootHash(nil)
	if err != nil {
		t.Fatalf("subCR.GetRootHash: %v", err)
	}

	// Consistency proof for [0, 4) in tree of size 8.
	proofNodes, err := proof.SubtreeConsistency(0, 4, treeSize)
	if err != nil {
		t.Fatalf("proof.SubtreeConsistency: %v", err)
	}
	validProof := make([][]byte, len(proofNodes.IDs))
	for i, id := range proofNodes.IDs {
		validProof[i] = nodes[id]
	}

	otherMirrorSigner, _ := mustGenerateKey("other-mirror-signer")
	otherOriginSigner, _ := mustGenerateKey("other-origin")

	opts := NewMirrorOptions().
		WithLogVerifier(testLogVerifier).
		WithSigner(testMirrorSigner).
		WithOrigin(testPendingCPOrigin).
		WithCheckpointSource(func(ctx context.Context) ([]byte, error) {
			return nil, nil
		})

	mt, err := NewMirrorTarget(ctx, &fakeDriver{}, opts)
	if err != nil {
		t.Fatalf("NewMirrorTarget failed: %v", err)
	}

	for _, test := range []struct {
		name      string
		start     uint64
		end       uint64
		subRoot   []byte
		proof     [][]byte
		cp        []byte
		wantErr   error
		wantCosig bool
	}{
		{
			name:      "success",
			start:     0,
			end:       4,
			subRoot:   validSubRoot,
			proof:     validProof,
			cp:        mustCosignCP(t, testPendingCPOrigin, treeSize, rootHash, testLogSigner, testMirrorSigner),
			wantCosig: true,
		},
		{
			name:    "checkpoint missing mirror cosignature",
			start:   0,
			end:     4,
			subRoot: validSubRoot,
			proof:   validProof,
			cp:      mustCosignCP(t, testPendingCPOrigin, treeSize, rootHash, testLogSigner),
			wantErr: witness.ErrNoWitnessSignature,
		},
		{
			name:    "checkpoint signed by wrong mirror key",
			start:   0,
			end:     4,
			subRoot: validSubRoot,
			proof:   validProof,
			cp:      mustCosignCP(t, testPendingCPOrigin, treeSize, rootHash, testLogSigner, otherMirrorSigner),
			wantErr: witness.ErrNoWitnessSignature,
		},
		{
			name:    "origin mismatch",
			start:   0,
			end:     4,
			subRoot: validSubRoot,
			proof:   validProof,
			cp:      mustCosignCP(t, "other-origin", treeSize, rootHash, otherOriginSigner, testMirrorSigner),
			wantErr: witness.ErrUnknownLog,
		},
		{
			name:    "invalid subtree range - end > cp.Size",
			start:   0,
			end:     16,
			subRoot: validSubRoot,
			proof:   validProof,
			cp:      mustCosignCP(t, testPendingCPOrigin, treeSize, rootHash, testLogSigner, testMirrorSigner),
			wantErr: witness.ErrSubtreeRangeInvalid,
		},
		{
			name:    "invalid subtree range - unaligned",
			start:   1,
			end:     3,
			subRoot: validSubRoot,
			proof:   validProof,
			cp:      mustCosignCP(t, testPendingCPOrigin, treeSize, rootHash, testLogSigner, testMirrorSigner),
			wantErr: witness.ErrSubtreeRangeInvalid,
		},
		{
			name:    "invalid proof - corrupted hash",
			start:   0,
			end:     4,
			subRoot: validSubRoot,
			proof:   [][]byte{[]byte("corrupted-proof-hash-of-32-bytes")},
			cp:      mustCosignCP(t, testPendingCPOrigin, treeSize, rootHash, testLogSigner, testMirrorSigner),
			wantErr: witness.ErrInvalidProof,
		},
		{
			name:    "invalid proof - wrong subtree root",
			start:   0,
			end:     4,
			subRoot: make([]byte, 32),
			proof:   validProof,
			cp:      mustCosignCP(t, testPendingCPOrigin, treeSize, rootHash, testLogSigner, testMirrorSigner),
			wantErr: witness.ErrInvalidProof,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := mt.SignSubtree(ctx, test.start, test.end, test.subRoot, test.proof, test.cp)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("got error %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if test.wantCosig {
				if len(got) == 0 {
					t.Fatalf("got empty cosig, want non-empty")
				}
				sigLine := strings.TrimSpace(string(got))
				parts := strings.Split(sigLine, " ")
				if len(parts) != 3 || parts[0] != "—" {
					t.Fatalf("unexpected cosig format: %q", sigLine)
				}
				sigWithHash, err := base64.StdEncoding.DecodeString(parts[2])
				if err != nil {
					t.Fatalf("failed to decode sig base64: %v", err)
				}
				if len(sigWithHash) < 4 {
					t.Fatalf("sig too short: %d bytes", len(sigWithHash))
				}
				sig := sigWithHash[4:]
				if !testMirrorVerifier.VerifySubtree(0, testPendingCPOrigin, test.start, test.end, test.subRoot, sig) {
					t.Errorf("VerifySubtree failed for generated cosignature")
				}
			} else {
				if len(got) != 0 {
					t.Fatalf("got non-empty cosig, want empty")
				}
			}
		})
	}
}

func TestMirrorTarget_SubtreeWitness_NilSigner(t *testing.T) {
	opts := NewMirrorOptions().
		WithLogVerifier(testLogVerifier).
		WithOrigin(testPendingCPOrigin).
		WithCheckpointSource(func(ctx context.Context) ([]byte, error) {
			return nil, nil
		})
	_, err := NewMirrorTarget(t.Context(), &fakeDriver{}, opts)
	if err == nil {
		t.Fatalf("NewMirrorTarget with nil signer: got nil err, want error")
	}
}

func buildTestTiles(t *testing.T) (cp256Raw []byte, cp512Raw []byte, root256 []byte, root512 []byte, fetcher func(ctx context.Context, level, index uint64, p uint8) ([]byte, error)) {
	t.Helper()
	hasher := rfc6962.DefaultHasher
	crf := &compact.RangeFactory{Hash: hasher.HashChildren}

	cr0 := crf.NewEmptyRange(0)
	tile0Nodes := make([][]byte, 256)
	for i := range 256 {
		h := hasher.HashLeaf(fmt.Appendf(nil, "entry-%d", i))
		tile0Nodes[i] = h
		if err := cr0.Append(h, nil); err != nil {
			t.Fatalf("cr0.Append: %v", err)
		}
	}
	r0, err := cr0.GetRootHash(nil)
	if err != nil {
		t.Fatalf("cr0.GetRootHash: %v", err)
	}

	cr1 := crf.NewEmptyRange(0)
	tile1Nodes := make([][]byte, 256)
	for i := range 256 {
		h := hasher.HashLeaf(fmt.Appendf(nil, "entry-%d", 256+i))
		tile1Nodes[i] = h
		if err := cr1.Append(h, nil); err != nil {
			t.Fatalf("cr1.Append: %v", err)
		}
	}
	r1, err := cr1.GetRootHash(nil)
	if err != nil {
		t.Fatalf("cr1.GetRootHash: %v", err)
	}

	r512 := hasher.HashChildren(r0, r1)

	tile0Raw, err := (&api.HashTile{Nodes: tile0Nodes}).MarshalText()
	if err != nil {
		t.Fatalf("tile0.MarshalText: %v", err)
	}
	tile1Raw, err := (&api.HashTile{Nodes: tile1Nodes}).MarshalText()
	if err != nil {
		t.Fatalf("tile1.MarshalText: %v", err)
	}
	tileL1Raw, err := (&api.HashTile{Nodes: [][]byte{r0, r1}}).MarshalText()
	if err != nil {
		t.Fatalf("tileL1.MarshalText: %v", err)
	}

	cp256 := mustSignCP(testPendingCPOrigin, 256, base64.StdEncoding.EncodeToString(r0), testLogSigner)
	cp512 := mustSignCP(testPendingCPOrigin, 512, base64.StdEncoding.EncodeToString(r512), testLogSigner)

	fetcher = func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
		if level == 0 && index == 0 {
			return tile0Raw, nil
		}
		if level == 0 && index == 1 {
			return tile1Raw, nil
		}
		if level == 1 && index == 0 {
			return tileL1Raw, nil
		}
		return nil, fmt.Errorf("tile not found: level %d, index %d, p %d", level, index, p)
	}

	return cp256, cp512, r0, r512, fetcher
}

func TestMirrorTarget_AddEntries_SubsequentUpload_WithConsistencyProof(t *testing.T) {
	ctx := t.Context()
	cp256, cp512, root256, r512, tileFetcher := buildTestTiles(t)

	var currentPublishedCP []byte
	pendingCPToReturn := cp256

	drv := &fakeDriver{
		writer: &fakeMirrorWriter{
			integrateFunc: func(ctx context.Context, fromBundleIdx uint64, bundles iter.Seq2[*api.EntryBundle, error]) (uint64, []byte, error) {
				for _, err := range bundles {
					if err != nil {
						return 0, nil, err
					}
				}
				if fromBundleIdx == 0 {
					return 256, root256, nil
				}
				return 512, r512, nil
			},
			updateCheckpointFunc: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
				newCP, err := f(currentPublishedCP)
				if err != nil {
					return err
				}
				currentPublishedCP = newCP
				return nil
			},
		},
		reader: &fakeLogReader{
			readCheckpoint: func(ctx context.Context) ([]byte, error) {
				return currentPublishedCP, nil
			},
			readTile: tileFetcher,
			sizeFunc: func(ctx context.Context) (uint64, error) {
				if currentPublishedCP == nil {
					return 0, nil
				}
				return 256, nil
			},
		},
	}

	mt, err := NewMirrorTarget(ctx, drv, &MirrorOptions{
		origin:      testPendingCPOrigin,
		logVerifier: testLogVerifier,
		signer:      testMirrorSigner,
		cpSource: func(ctx context.Context) ([]byte, error) {
			return pendingCPToReturn, nil
		},
	})
	if err != nil {
		t.Fatalf("NewMirrorTarget failed: %v", err)
	}

	// 1. Initial upload: 0 -> 256
	ticket256, err := mt.seal(cp256)
	if err != nil {
		t.Fatalf("seal failed: %v", err)
	}
	nextEntry, pendingSize, _, cosigs1, err := mt.AddEntries(ctx, 0, 256, ticket256, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if err != nil {
		t.Fatalf("AddEntries (0->256) failed: %v", err)
	}
	if nextEntry != 256 || pendingSize != 256 {
		t.Errorf("AddEntries (0->256) got nextEntry=%d, pendingSize=%d; want 256, 256", nextEntry, pendingSize)
	}
	if len(cosigs1) == 0 {
		t.Fatalf("AddEntries (0->256) got empty cosigs")
	}
	if mt.oldSize.Load() != 256 {
		t.Errorf("mt.oldSize = %d, want 256", mt.oldSize.Load())
	}

	// 2. Subsequent upload: 256 -> 512
	pendingCPToReturn = cp512
	ticket512, err := mt.seal(cp512)
	if err != nil {
		t.Fatalf("seal failed: %v", err)
	}
	nextEntry, pendingSize, _, cosigs2, err := mt.AddEntries(ctx, 256, 512, ticket512, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if err != nil {
		t.Fatalf("AddEntries (256->512) failed: %v", err)
	}
	if nextEntry != 512 || pendingSize != 512 {
		t.Errorf("AddEntries (256->512) got nextEntry=%d, pendingSize=%d; want 512, 512", nextEntry, pendingSize)
	}
	if len(cosigs2) == 0 {
		t.Fatalf("AddEntries (256->512) got empty cosigs")
	}
	if mt.oldSize.Load() != 512 {
		t.Errorf("mt.oldSize = %d, want 512", mt.oldSize.Load())
	}

	if wantCP := append([]byte(cp512), cosigs2...); !bytes.Equal(currentPublishedCP, wantCP) {
		t.Errorf("currentPublishedCP = %s, want %s", currentPublishedCP, wantCP)
	}
}

func TestMirrorTarget_PublishCheckpoint_RetryStale(t *testing.T) {
	ctx := t.Context()
	_, cp512, root256, _, tileFetcher := buildTestTiles(t)

	for _, test := range []struct {
		desc           string
		initialOldSize uint64
		storedCP       []byte
	}{
		{
			desc:           "oldSize 0 but storage has size 256 checkpoint",
			initialOldSize: 0,
			storedCP:       mustCosignCP(t, testPendingCPOrigin, 256, root256, testLogSigner, testMirrorSigner),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			currentPublishedCP := test.storedCP

			drv := &fakeDriver{
				writer: &fakeMirrorWriter{
					updateCheckpointFunc: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
						newCP, err := f(currentPublishedCP)
						if err != nil {
							return err
						}
						currentPublishedCP = newCP
						return nil
					},
				},
				reader: &fakeLogReader{
					readCheckpoint: func(ctx context.Context) ([]byte, error) {
						return currentPublishedCP, nil
					},
					readTile: tileFetcher,
				},
			}

			mt, err := NewMirrorTarget(ctx, drv, &MirrorOptions{
				origin:      testPendingCPOrigin,
				logVerifier: testLogVerifier,
				signer:      testMirrorSigner,
				cpSource: func(ctx context.Context) ([]byte, error) {
					return cp512, nil
				},
			})
			if err != nil {
				t.Fatalf("NewMirrorTarget failed: %v", err)
			}

			mt.oldSize.Store(test.initialOldSize)

			sigs, pubSize, err := mt.publishCheckpoint(ctx, cp512, 512)
			if err != nil {
				t.Fatalf("publishCheckpoint failed: %v", err)
			}
			if pubSize != 512 {
				t.Errorf("pubSize = %d, want 512", pubSize)
			}
			if len(sigs) == 0 {
				t.Errorf("got empty signatures, want cosignature")
			}
			if mt.oldSize.Load() != 512 {
				t.Errorf("mt.oldSize = %d, want 512", mt.oldSize.Load())
			}

			if wantCP := append([]byte(cp512), sigs...); !bytes.Equal(currentPublishedCP, wantCP) {
				t.Errorf("currentPublishedCP = %s, want %s", currentPublishedCP, wantCP)
			}
		})
	}
}

func TestMirrorTarget_PublishCheckpoint_Errors(t *testing.T) {
	ctx := t.Context()
	cp256, cp512, root256, root512, tileFetcher := buildTestTiles(t)

	errTileFetch := errors.New("tile fetch error")
	errStorageWrite := errors.New("storage write error")

	for _, test := range []struct {
		desc           string
		initialOldSize uint64
		storedCP       []byte
		newCP          []byte
		newCPSize      uint64
		readTile       func(ctx context.Context, level, index uint64, p uint8) ([]byte, error)
		updateCP       func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error
		wantErr        error
		wantErrSubstr  string
	}{
		{
			desc:           "consistency proof fetch error",
			initialOldSize: 256,
			storedCP:       mustCosignCP(t, testPendingCPOrigin, 256, root256, testLogSigner, testMirrorSigner),
			newCP:          cp512,
			newCPSize:      512,
			readTile: func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
				return nil, errTileFetch
			},
			wantErrSubstr: "failed to get consistency proof",
		},
		{
			desc:           "invalid consistency proof from corrupt tile",
			initialOldSize: 256,
			storedCP:       mustCosignCP(t, testPendingCPOrigin, 256, root256, testLogSigner, testMirrorSigner),
			newCP:          cp512,
			newCPSize:      512,
			readTile: func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
				if level == 1 && index == 0 {
					corruptTile := &api.HashTile{Nodes: [][]byte{root256, bytes.Repeat([]byte{0xff}, 32)}}
					return corruptTile.MarshalText()
				}
				return tileFetcher(ctx, level, index, p)
			},
			wantErr: witness.ErrInvalidProof,
		},
		{
			desc:           "storage UpdateCheckpoint error",
			initialOldSize: 0,
			storedCP:       nil,
			newCP:          cp256,
			newCPSize:      256,
			readTile:       tileFetcher,
			updateCP: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
				return errStorageWrite
			},
			wantErr: errStorageWrite,
		},
		{
			desc:           "tree size regression - newCP smaller than storage CP",
			initialOldSize: 0,
			storedCP:       mustCosignCP(t, testPendingCPOrigin, 512, root512, testLogSigner, testMirrorSigner),
			newCP:          cp256,
			newCPSize:      256,
			readTile:       tileFetcher,
			wantErrSubstr:  "failed to get consistency proof",
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			currentPublishedCP := test.storedCP

			drv := &fakeDriver{
				writer: &fakeMirrorWriter{
					updateCheckpointFunc: func(ctx context.Context, f func(oldCP []byte) (newCP []byte, err error)) error {
						if test.updateCP != nil {
							return test.updateCP(ctx, f)
						}
						newCP, err := f(currentPublishedCP)
						if err != nil {
							return err
						}
						currentPublishedCP = newCP
						return nil
					},
				},
				reader: &fakeLogReader{
					readCheckpoint: func(ctx context.Context) ([]byte, error) {
						return currentPublishedCP, nil
					},
					readTile: test.readTile,
				},
			}

			mt, err := NewMirrorTarget(ctx, drv, &MirrorOptions{
				origin:      testPendingCPOrigin,
				logVerifier: testLogVerifier,
				signer:      testMirrorSigner,
				cpSource: func(ctx context.Context) ([]byte, error) {
					return test.newCP, nil
				},
			})
			if err != nil {
				t.Fatalf("NewMirrorTarget failed: %v", err)
			}

			mt.oldSize.Store(test.initialOldSize)

			_, _, err = mt.publishCheckpoint(ctx, test.newCP, test.newCPSize)
			if err == nil {
				t.Fatalf("publishCheckpoint succeeded, want error")
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("publishCheckpoint error = %v, want error wrapping %v", err, test.wantErr)
			}
			if test.wantErrSubstr != "" && !strings.Contains(err.Error(), test.wantErrSubstr) {
				t.Errorf("publishCheckpoint error = %v, want substring %q", err, test.wantErrSubstr)
			}
		})
	}
}

func TestNewMirrorTarget_OptionsValidation(t *testing.T) {
	ctx := t.Context()

	for _, test := range []struct {
		desc        string
		driver      Driver
		opts        *MirrorOptions
		wantErrText string
	}{
		{
			desc:        "nil options",
			driver:      &fakeDriver{},
			opts:        nil,
			wantErrText: "opts cannot be nil",
		},
		{
			desc:        "missing log verifier",
			driver:      &fakeDriver{},
			opts:        NewMirrorOptions().WithSigner(testMirrorSigner).WithCheckpointSource(func(ctx context.Context) ([]byte, error) { return nil, nil }),
			wantErrText: "WithLogVerifier must be set",
		},
		{
			desc:        "missing signer",
			driver:      &fakeDriver{},
			opts:        NewMirrorOptions().WithLogVerifier(testLogVerifier).WithCheckpointSource(func(ctx context.Context) ([]byte, error) { return nil, nil }),
			wantErrText: "WithSigner must be set",
		},
		{
			desc:        "missing checkpoint source",
			driver:      &fakeDriver{},
			opts:        NewMirrorOptions().WithLogVerifier(testLogVerifier).WithSigner(testMirrorSigner),
			wantErrText: "WithCheckpointSource must be set",
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			_, err := NewMirrorTarget(ctx, test.driver, test.opts)
			if err == nil {
				t.Fatalf("NewMirrorTarget succeeded, want error")
			}
			if !strings.Contains(err.Error(), test.wantErrText) {
				t.Errorf("NewMirrorTarget error = %v, want substring %q", err, test.wantErrText)
			}
		})
	}
}

// mustCosignCP is a helper to sign checkpoints with multiple signers.
func mustCosignCP(t *testing.T, origin string, size uint64, root []byte, signers ...note.Signer) []byte {
	t.Helper()
	raw, err := note.Sign(&note.Note{Text: fmt.Sprintf("%s\n%d\n%s\n", origin, size, base64.StdEncoding.EncodeToString(root))}, signers...)
	if err != nil {
		t.Fatalf("Failed to sign note: %v", err)
	}
	return raw
}
