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

package handler

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/mirror"
)

type mockMirror struct {
	addCheckpointFunc func(ctx context.Context, oldSize uint64, proof [][]byte, cp []byte) error
	addEntriesFunc    func(ctx context.Context, logOrigin string, uploadStart, uploadEnd uint64, ticket []byte, next func() (*mirror.Package, error)) ([]byte, error)
}

func (m *mockMirror) AddCheckpoint(ctx context.Context, oldSize uint64, proof [][]byte, cp []byte) error {
	if m.addCheckpointFunc != nil {
		return m.addCheckpointFunc(ctx, oldSize, proof, cp)
	}
	return nil
}

func (m *mockMirror) AddEntries(ctx context.Context, logOrigin string, uploadStart, uploadEnd uint64, ticket []byte, next func() (*mirror.Package, error)) ([]byte, error) {
	if m.addEntriesFunc != nil {
		return m.addEntriesFunc(ctx, logOrigin, uploadStart, uploadEnd, ticket, next)
	}
	return []byte("— dummy-cosig\n"), nil
}

func TestAddCheckpoint(t *testing.T) {
	const cpOld = 100

	mock := &mockMirror{
		addCheckpointFunc: func(ctx context.Context, oldSize uint64, proof [][]byte, cp []byte) error {
			if oldSize != cpOld {
				return fmt.Errorf("want oldSize %d, got %d", cpOld, oldSize)
			}
			if len(proof) != 1 {
				return fmt.Errorf("want 1 proof hash, got %d", len(proof))
			}
			return nil
		},
	}
	h := New(mock)

	cp := "example-log\n123\nSGVsbG8sIHdvcmxkIQ==\n\n"
	proofHash := base64.StdEncoding.EncodeToString(make([]byte, 32))
	body := fmt.Sprintf("old %d\n%s\n\n%s", cpOld, proofHash, cp)

	req := httptest.NewRequest(http.MethodPost, "/add-checkpoint", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("want status 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAddEntries(t *testing.T) {
	const (
		testOrigin      = "example-log"
		testTicket      = "ticket"
		testUploadStart = 100
		testUploadEnd   = 110
	)

	mock := &mockMirror{
		addEntriesFunc: func(ctx context.Context, logOrigin string, uploadStart, uploadEnd uint64, ticket []byte, next func() (*mirror.Package, error)) ([]byte, error) {
			if logOrigin != testOrigin {
				return nil, fmt.Errorf("want logOrigin %s, got %s", testOrigin, logOrigin)
			}
			if uploadStart != testUploadStart || uploadEnd != testUploadEnd {
				return nil, fmt.Errorf("want range %d-%d, got %d-%d", testUploadStart, testUploadEnd, uploadStart, uploadEnd)
			}

			pkg, err := next()
			if err != nil {
				return nil, fmt.Errorf("failed to read next package: %v", err)
			}
			wantNumEntries := testUploadEnd - testUploadStart
			if len(pkg.Entries) != wantNumEntries {
				return nil, fmt.Errorf("want %d entries in first package, got %d", wantNumEntries, len(pkg.Entries))
			}

			// Try to read one more, expecting EOF
			_, err = next()
			if err == nil {
				return nil, fmt.Errorf("expected EOF, got nil")
			}

			return []byte("— test-cosig\n"), nil
		},
	}
	h := New(mock)

	var body bytes.Buffer

	// Write preamble
	_ = binary.Write(&body, binary.BigEndian, uint16(len(testOrigin)))
	_, _ = body.WriteString(testOrigin)
	_ = binary.Write(&body, binary.BigEndian, uint64(testUploadStart))
	_ = binary.Write(&body, binary.BigEndian, uint64(testUploadEnd))
	_ = binary.Write(&body, binary.BigEndian, uint16(len(testTicket)))
	_, _ = body.WriteString(testTicket)

	// Write entry package
	for i := range testUploadEnd - testUploadStart {
		entry := fmt.Appendf(nil, "entry-%d", i)
		_ = binary.Write(&body, binary.BigEndian, uint16(len(entry)))
		_, _ = body.Write(entry)
	}
	_ = body.WriteByte(1)               // num proof hashes
	_, _ = body.Write(make([]byte, 32)) // 1 hash

	req := httptest.NewRequest(http.MethodPost, "/add-entries", &body)
	req.Header.Add("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("want status 200, got %d: %s", w.Code, w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte("— test-cosig\n")) {
		t.Errorf("response does not contain expected cosignature: %s", w.Body.String())
	}
}
