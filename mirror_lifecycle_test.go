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
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	"testing"

	"github.com/transparency-dev/tessera/api"
)

type fakeMirrorWriter struct {
	integrateFunc func(ctx context.Context, from uint64, bundles iter.Seq[api.EntryBundle]) (uint64, []byte, error)
	sizeFunc      func(ctx context.Context) (uint64, error)
}

func (f *fakeMirrorWriter) IntegrateBundles(ctx context.Context, from uint64, bundles iter.Seq[api.EntryBundle]) (uint64, []byte, error) {
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

type fakeLogReader struct {
	sizeFunc func(ctx context.Context) (uint64, error)
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
	return nil, nil
}
func (f *fakeLogReader) NextIndex(ctx context.Context) (uint64, error) { return 0, nil }

const (
	testPendingCPOrigin = "test-origin"
	testPendingCPSize   = uint64(512)
	testPendingCPRoot   = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
)

var testPendingCP = fmt.Sprintf("%s\n%d\n%s\n— test-sig\n", testPendingCPOrigin, testPendingCPSize, testPendingCPRoot)

func newTestMirrorTarget(size uint64) *MirrorTarget {
	return &MirrorTarget{
		writer: &fakeMirrorWriter{
			sizeFunc: func(ctx context.Context) (uint64, error) { return size, nil },
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return size, nil },
		},
		cpSource: func(ctx context.Context) ([]byte, error) {
			return []byte(testPendingCP), nil
		},
	}
}

func TestTicketRoundTrip(t *testing.T) {
	mt := newTestMirrorTarget(0)

	ticket, err := mt.sealTicket([]byte(testPendingCP))
	if err != nil {
		t.Fatalf("sealTicket failed: %v", err)
	}

	p, err := mt.openTicket(ticket)
	if err != nil {
		t.Fatalf("openTicket failed: %v", err)
	}

	if !bytes.Equal(p, []byte(testPendingCP)) {
		t.Errorf("openTicket: got %s, want %s", p, testPendingCP)
	}
}

func TestTicketTampering(t *testing.T) {
	mt := newTestMirrorTarget(0)

	ticket, err := mt.sealTicket([]byte(testPendingCP))
	if err != nil {
		t.Fatalf("sealTicket failed: %v", err)
	}

	for i := 0; i < len(ticket); i++ {
		b := ticket[i]
		ticket[i] = b ^ 0xff
		if _, err := mt.openTicket(ticket); err == nil {
			t.Errorf("openTicket: tampering did not fail")
		}
		ticket[i] = b
	}
}

func TestMirrorTarget_AddEntries_NoTicket(t *testing.T) {
	const (
		testIntegratedSize = uint64(100)
	)
	ctx := context.Background()
	mt := newTestMirrorTarget(testIntegratedSize)

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

func TestMirrorTarget_AddEntries_ValidTicket(t *testing.T) {
	const (
		testUploadStart    = uint64(256)
		testUploadEnd      = uint64(512)
		testIntegratedSize = uint64(256)
	)
	ctx := context.Background()
	mt := &MirrorTarget{
		ticketKey: make([]byte, sha256.Size),
		writer: &fakeMirrorWriter{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
			integrateFunc: func(ctx context.Context, from uint64, bundles iter.Seq[api.EntryBundle]) (uint64, []byte, error) {
				pendingCPRoot, err := base64.StdEncoding.DecodeString(testPendingCPRoot)
				return testUploadEnd, pendingCPRoot, err
			},
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
		cpSource: func(ctx context.Context) ([]byte, error) { return []byte(testPendingCP), nil },
	}

	validTicket, err := mt.sealTicket([]byte(testPendingCP))
	if err != nil {
		t.Fatalf("sealTicket failed: %v", err)
	}
	nextEntry, pendingSize, _, cosigs, err := mt.AddEntries(ctx, testUploadStart, testUploadEnd, validTicket, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
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
}
