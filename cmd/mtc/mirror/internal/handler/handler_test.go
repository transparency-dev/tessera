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
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/transparency-dev/tessera"
)

func TestAddEntries(t *testing.T) {
	const (
		testOrigin      = "example-log"
		testTicket      = "ticket"
		testUploadStart = 100
		testUploadEnd   = 110
	)

	mock := &mockTarget{
		addEntriesFunc: func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) ([]byte, error) {
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
	mux := NewMirrorMux()
	if err := mux.AddTarget(testOrigin, mock); err != nil {
		t.Fatalf("AddTarget() failed: %v", err)
	}
	h := New(mux, nil)

	var rawBody bytes.Buffer

	// Write preamble
	_ = binary.Write(&rawBody, binary.BigEndian, uint16(len(testOrigin)))
	_, _ = rawBody.WriteString(testOrigin)
	_ = binary.Write(&rawBody, binary.BigEndian, uint64(testUploadStart))
	_ = binary.Write(&rawBody, binary.BigEndian, uint64(testUploadEnd))
	_ = binary.Write(&rawBody, binary.BigEndian, uint16(len(testTicket)))
	_, _ = rawBody.WriteString(testTicket)

	// Write entry package
	for i := range testUploadEnd - testUploadStart {
		entry := fmt.Appendf(nil, "entry-%d", i)
		_ = binary.Write(&rawBody, binary.BigEndian, uint16(len(entry)))
		_, _ = rawBody.Write(entry)
	}
	_ = rawBody.WriteByte(1)               // num proof hashes
	_, _ = rawBody.Write(make([]byte, 32)) // 1 hash
	validPayload := rawBody.Bytes()

	tests := []struct {
		name       string
		encoding   string
		body       func() io.Reader
		wantStatus int
		wantCosig  bool
	}{
		{
			name:     "uncompressed",
			encoding: "",
			body: func() io.Reader {
				return bytes.NewReader(validPayload)
			},
			wantStatus: http.StatusOK,
			wantCosig:  true,
		},
		{
			name:     "gzip",
			encoding: "gzip",
			body: func() io.Reader {
				var b bytes.Buffer
				zw := gzip.NewWriter(&b)
				_, _ = zw.Write(validPayload)
				_ = zw.Close()
				return &b
			},
			wantStatus: http.StatusOK,
			wantCosig:  true,
		},
		{
			name:     "invalid gzip",
			encoding: "gzip",
			body: func() io.Reader {
				return strings.NewReader("not-a-gzip-stream")
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:     "unsupported encoding",
			encoding: "br",
			body: func() io.Reader {
				return strings.NewReader("dummy")
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/add-entries", tc.body())
			req.Header.Add("Content-Type", "application/octet-stream")
			if tc.encoding != "" {
				req.Header.Add("Content-Encoding", tc.encoding)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Errorf("want status %d, got %d: %s", tc.wantStatus, w.Code, w.Body.String())
			}
			if tc.wantCosig {
				if !bytes.Contains(w.Body.Bytes(), []byte("— test-cosig\n")) {
					t.Errorf("response does not contain expected cosignature: %s", w.Body.String())
				}
				if got := w.Header().Get("Accept-Encoding"); got != "gzip" {
					t.Errorf("want Accept-Encoding gzip, got %q", got)
				}
			}
		})
	}
}

type mockTarget struct {
	addEntriesFunc    func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) ([]byte, error)
}

func (m *mockTarget) AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) ([]byte, error) {
	if m.addEntriesFunc != nil {
		return m.addEntriesFunc(ctx, uploadStart, uploadEnd, ticket, next)
	}
	return []byte("— dummy-cosig\n"), nil
}
