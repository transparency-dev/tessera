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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/cmd/mtc/log"
	tposix "github.com/transparency-dev/tessera/storage/posix"
	"golang.org/x/crypto/cryptobyte"
	"golang.org/x/crypto/cryptobyte/asn1"
	"golang.org/x/mod/sumdb/note"
)

func setupTestLog(t *testing.T) *log.MTCLog {
	t.Helper()
	ctx := t.Context()
	storageDir := t.TempDir()

	driver, err := tposix.New(ctx, tposix.Config{Path: storageDir})
	if err != nil {
		t.Fatalf("Failed to initialize POSIX storage: %v", err)
	}

	sk := "PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
	signer, err := note.NewSigner(sk)
	if err != nil {
		t.Fatalf("Failed to create test signer: %v", err)
	}

	opts := tessera.NewAppendOptions().WithCheckpointSigner(signer)
	appender, _, reader, err := tessera.NewAppender(ctx, driver, opts)
	if err != nil {
		t.Fatalf("Failed to initialize Tessera appender: %v", err)
	}

	mtcLog, err := log.NewMTCLog(ctx, appender, log.NewOptions().
		WithTesseraReader(reader).
		WithAwaiterPollInterval(20*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to initialize MTC log: %v", err)
	}
	return mtcLog
}

func dummySeq(data string) []byte {
	var b cryptobyte.Builder
	b.AddASN1(asn1.SEQUENCE, func(b *cryptobyte.Builder) {
		b.AddBytes([]byte(data))
	})
	return b.BytesOrPanic()
}

func dummyTag(tag asn1.Tag, data []byte) []byte {
	var b cryptobyte.Builder
	b.AddASN1(tag, func(b *cryptobyte.Builder) {
		b.AddBytes(data)
	})
	return b.BytesOrPanic()
}

func validEntry() log.TBSCertificateLogEntry {
	return log.TBSCertificateLogEntry{
		Version:                   0,
		Issuer:                    dummySeq("issuer"),
		Validity:                  dummySeq("validity"),
		Subject:                   dummySeq("subject"),
		SubjectPublicKeyAlgorithm: dummySeq("algo"),
		SubjectPublicKeyInfoHash:  make([]byte, sha256.Size),
	}
}

func TestAddTBSHandler(t *testing.T) {
	mtcLog := setupTestLog(t)

	validJSON, err := json.Marshal(validEntry())
	if err != nil {
		t.Fatalf("failed to marshal valid entry: %v", err)
	}

	missingIssuer := validEntry()
	missingIssuer.Issuer = nil
	missingIssuerJSON, _ := json.Marshal(missingIssuer)

	badHashSize := validEntry()
	badHashSize.SubjectPublicKeyInfoHash = make([]byte, 10)
	badHashSizeJSON, _ := json.Marshal(badHashSize)

	largeEntry := validEntry()
	largeEntry.Extensions = dummyTag(asn1.Tag(3).ContextSpecific().Constructed(), bytes.Repeat([]byte("a"), 128*1024))
	largeJSON, _ := json.Marshal(largeEntry)

	tests := []struct {
		name       string
		body       func() io.Reader
		addFunc    addTBS
		wantStatus int
		wantBody   string
	}{
		{
			name:       "success",
			body:       func() io.Reader { return bytes.NewReader(validJSON) },
			wantStatus: http.StatusCreated,
			wantBody:   `{"index":0,"mtcProof":{}}`,
		},
		{
			name:       "malformed json",
			body:       func() io.Reader { return strings.NewReader("not-a-json-payload") },
			wantStatus: http.StatusBadRequest,
			wantBody:   "Invalid TBSCertificateLogEntry JSON payload",
		},
		{
			name:       "invalid entry fields missing issuer",
			body:       func() io.Reader { return bytes.NewReader(missingIssuerJSON) },
			wantStatus: http.StatusBadRequest,
			wantBody:   "Invalid TBSCertificateLogEntry",
		},
		{
			name:       "invalid entry fields wrong hash size",
			body:       func() io.Reader { return bytes.NewReader(badHashSizeJSON) },
			wantStatus: http.StatusBadRequest,
			wantBody:   "subjectPublicKeyInfoHash must be 32 bytes",
		},
		{
			name: "add function error",
			body: func() io.Reader { return bytes.NewReader(validJSON) },
			addFunc: func(ctx context.Context, entry log.TBSCertificateLogEntry) (*log.AddTBSRsp, error) {
				return nil, errors.New("storage error")
			},
			wantStatus: http.StatusInternalServerError,
			wantBody:   "Could not add entry to log",
		},
		{
			name:       "payload too large",
			body:       func() io.Reader { return bytes.NewReader(largeJSON) },
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/add-tbs", tc.body())
			w := httptest.NewRecorder()

			var h http.Handler
			if tc.addFunc != nil {
				h = http.MaxBytesHandler(addTBSHandler(tc.addFunc), maxAddTBSRequestBodyBytes)
			} else {
				h = New(mtcLog)
			}
			h.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Errorf("want status %d, got %d: %s", tc.wantStatus, w.Code, w.Body.String())
			}
			if tc.wantBody != "" && !strings.Contains(w.Body.String(), tc.wantBody) {
				t.Errorf("want body containing %q, got %q", tc.wantBody, w.Body.String())
			}
		})
	}
}

func TestNew_Routes(t *testing.T) {
	mtcLog := setupTestLog(t)
	h := New(mtcLog)

	tests := []struct {
		name       string
		method     string
		path       string
		body       func() io.Reader
		wantStatus int
	}{
		{
			name:       "POST /add-tbs valid method routes correctly",
			method:     http.MethodPost,
			path:       "/add-tbs",
			body:       func() io.Reader { return strings.NewReader("bad json") },
			wantStatus: http.StatusBadRequest, // Proves routing reached addTBSHandler
		},
		{
			name:       "GET /add-tbs invalid method",
			method:     http.MethodGet,
			path:       "/add-tbs",
			body:       func() io.Reader { return nil },
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "GET /proof-to-landmark valid method",
			method:     http.MethodGet,
			path:       "/proof-to-landmark",
			body:       func() io.Reader { return nil },
			wantStatus: http.StatusNotImplemented,
		},
		{
			name:       "POST /proof-to-landmark invalid method",
			method:     http.MethodPost,
			path:       "/proof-to-landmark",
			body:       func() io.Reader { return nil },
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "unknown route",
			method:     http.MethodGet,
			path:       "/unknown",
			body:       func() io.Reader { return nil },
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, tc.body())
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Errorf("%s %s: want status %d, got %d: %s", tc.method, tc.path, tc.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}
