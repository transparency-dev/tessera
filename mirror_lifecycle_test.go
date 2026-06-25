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
	"context"
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
	testPendingCPSize   = uint64(200)
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

func TestMirrorTarget_AddEntries_RangeConflict(t *testing.T) {
	const (
		testUploadStart    = uint64(100)
		testUploadEnd      = uint64(250)
		testPendingSize    = uint64(200)
		testIntegratedSize = uint64(150)
	)
	ctx := context.Background()
	mt := &MirrorTarget{
		writer: &fakeMirrorWriter{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
		cpSource: func(ctx context.Context) ([]byte, error) { return []byte(testPendingCP), nil },
	}

	validTicket, err := mt.sealTicket(ctx, &ticket{PendingCP: []byte(testPendingCP)})
	if err != nil {
		t.Fatalf("sealTicket failed: %v", err)
	}

	// testUploadEnd  != pendingSize -> conflict
	_, _, _, _, err = mt.AddEntries(ctx, testUploadStart, testUploadEnd, validTicket, func() (*MirrorPackage, error) {
		return nil, io.EOF
	})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("want ErrConflict, got %v", err)
	}
}

func TestMirrorTarget_AddEntries_CompleteUpload(t *testing.T) {
	const (
		testIntegratedSize = uint64(100)
		testUploadStart    = uint64(100)
		testUploadEnd      = uint64(200)
	)

	ctx := context.Background()
	mt := &MirrorTarget{
		writer: &fakeMirrorWriter{
			integrateFunc: func(ctx context.Context, from uint64, bundles iter.Seq[api.EntryBundle]) (uint64, []byte, error) {
				// Consume iterator
				for range bundles {
				}
				decodedRoot, err := base64.StdEncoding.DecodeString(testPendingCPRoot)
				if err != nil {
					return 0, nil, fmt.Errorf("TEST ERROR: %v", err)
				}
				return testPendingCPSize, decodedRoot, nil
			},
		},
		reader: &fakeLogReader{
			sizeFunc: func(ctx context.Context) (uint64, error) { return testIntegratedSize, nil },
		},
		cpSource: func(ctx context.Context) ([]byte, error) { return []byte(testPendingCP), nil },
	}

	validTicket, err := mt.sealTicket(ctx, &ticket{PendingCP: []byte(testPendingCP)})
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
