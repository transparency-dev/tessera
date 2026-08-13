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
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/witness/witness"
)

func TestAddEntries(t *testing.T) {
	const (
		testOrigin      = "example-log"
		testTicket      = "ticket"
		testUploadStart = 100
		testUploadEnd   = 110
	)

	mock := &mockTarget{
		addEntriesFunc: func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (nextIdx uint64, newSize uint64, newTicket []byte, cosigs []byte, err error) {
			if uploadStart != testUploadStart || uploadEnd != testUploadEnd {
				return 0, 0, nil, nil, fmt.Errorf("want range %d-%d, got %d-%d", testUploadStart, testUploadEnd, uploadStart, uploadEnd)
			}

			pkg, err := next()
			if err != nil {
				return 0, 0, nil, nil, fmt.Errorf("failed to read next package: %v", err)
			}
			wantNumEntries := testUploadEnd - testUploadStart
			if len(pkg.Entries) != wantNumEntries {
				return 0, 0, nil, nil, fmt.Errorf("want %d entries in first package, got %d", wantNumEntries, len(pkg.Entries))
			}

			// Try to read one more, expecting EOF
			_, err = next()
			if err == nil {
				return 0, 0, nil, nil, fmt.Errorf("expected EOF, got nil")
			}

			return testUploadEnd, testUploadEnd, nil, []byte("— test-cosig\n"), nil
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
	addEntriesFunc  func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (nextIdx uint64, curSize uint64, newTicket []byte, cosigs []byte, err error)
	signSubtreeFunc func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error)
}

func (m *mockTarget) AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (nextIdx uint64, curSize uint64, newTicket []byte, cosigs []byte, err error) {
	if m.addEntriesFunc != nil {
		return m.addEntriesFunc(ctx, uploadStart, uploadEnd, ticket, next)
	}
	return uploadEnd, 0, nil, nil, nil
}

func (m *mockTarget) SignSubtree(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
	if m.signSubtreeFunc != nil {
		return m.signSubtreeFunc(ctx, start, end, subRoot, proof, cp)
	}
	return nil, nil
}

func TestAddEntries_StatusCodes(t *testing.T) {
	const (
		testOrigin      = "example-log"
		testUploadStart = 10
		testUploadEnd   = 20
		testNextIdx     = 100
		testPendingSize = 200
		testNewTicket   = "ticket-bytes"
		testCosig       = "— test-cosig\n"
	)

	for _, test := range []struct {
		name       string
		origin     string
		mockTarget *mockTarget
		wantStatus int
		wantBody   string
	}{
		{
			name:   "200 ok",
			origin: testOrigin,
			mockTarget: &mockTarget{
				addEntriesFunc: func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (uint64, uint64, []byte, []byte, error) {
					return testPendingSize, testPendingSize, []byte(testNewTicket), []byte(testCosig), nil
				},
			},
			wantStatus: http.StatusOK,
			wantBody:   testCosig,
		}, {
			name:   "202 partial upload",
			origin: testOrigin,
			mockTarget: &mockTarget{
				addEntriesFunc: func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (uint64, uint64, []byte, []byte, error) {
					return testUploadStart + 5, testPendingSize, []byte(testNewTicket), nil, nil
				},
			},
			wantStatus: http.StatusAccepted,
			wantBody:   fmt.Sprintf("%d\n%d\n%s\n", testPendingSize, testUploadStart+5, base64.StdEncoding.EncodeToString([]byte(testNewTicket))),
		}, {
			name:   "400 truncated body (no entries saved)",
			origin: testOrigin,
			mockTarget: &mockTarget{
				addEntriesFunc: func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (uint64, uint64, []byte, []byte, error) {
					return uploadStart, testPendingSize, nil, nil, nil
				},
			},
			wantStatus: http.StatusBadRequest,
		}, {
			name:       "404 unknown log",
			origin:     "unknown-origin",
			mockTarget: &mockTarget{},
			wantStatus: http.StatusNotFound,
		}, {
			name:   "409 conflict",
			origin: testOrigin,
			mockTarget: &mockTarget{
				addEntriesFunc: func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (uint64, uint64, []byte, []byte, error) {
					return testNextIdx, testPendingSize, []byte(testNewTicket), nil, tessera.ErrConflict
				},
			},
			wantStatus: http.StatusConflict,
			wantBody:   fmt.Sprintf("%d\n%d\n%s\n", testPendingSize, testNextIdx, base64.StdEncoding.EncodeToString([]byte(testNewTicket))),
		}, {
			name:   "422 no pending checkpoint",
			origin: testOrigin,
			mockTarget: &mockTarget{
				addEntriesFunc: func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (uint64, uint64, []byte, []byte, error) {
					return 0, 0, nil, nil, tessera.ErrNoPendingCheckpoint
				},
			},
			wantStatus: http.StatusUnprocessableEntity,
		}, {
			name:   "422 unprocessable entity",
			origin: testOrigin,
			mockTarget: &mockTarget{
				addEntriesFunc: func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (uint64, uint64, []byte, []byte, error) {
					return testNextIdx, testPendingSize, []byte(testNewTicket), nil, tessera.ErrInvalidProof
				},
			},
			wantStatus: http.StatusUnprocessableEntity,
		}, {
			name:   "500 internal server error",
			origin: testOrigin,
			mockTarget: &mockTarget{
				addEntriesFunc: func(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) (uint64, uint64, []byte, []byte, error) {
					return 0, 0, nil, nil, errors.New("db explosion")
				},
			},
			wantStatus: http.StatusInternalServerError,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := NewMirrorMux()
			_ = mux.AddTarget(testOrigin, test.mockTarget)
			h := New(mux, nil)

			var rawBody bytes.Buffer
			_ = binary.Write(&rawBody, binary.BigEndian, uint16(len(test.origin)))
			_, _ = rawBody.WriteString(test.origin)
			_ = binary.Write(&rawBody, binary.BigEndian, uint64(testUploadStart))
			_ = binary.Write(&rawBody, binary.BigEndian, uint64(testUploadEnd))
			_ = binary.Write(&rawBody, binary.BigEndian, uint16(0))

			req := httptest.NewRequest(http.MethodPost, "/add-entries", &rawBody)
			req.Header.Add("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != test.wantStatus {
				t.Errorf("want status %d, got %d: %s", test.wantStatus, w.Code, w.Body.String())
			}
			if test.wantStatus == http.StatusConflict || test.wantStatus == http.StatusAccepted {
				if got := w.Header().Get("Content-Type"); got != "text/x.tlog.mirror-info" {
					t.Errorf("want Content-Type text/x.tlog.mirror-info, got %q", got)
				}
			}
			if test.wantBody != "" {
				if got := w.Body.String(); got != test.wantBody {
					t.Errorf("want body %q, got %q", test.wantBody, got)
				}
			}
		})
	}
}

func TestSignSubtree_StatusCodes(t *testing.T) {
	const (
		testOrigin = "example-log"
		testStart  = uint64(0)
		testEnd    = uint64(4)
		testCosig  = "— test-mirror-cosig\n"
	)
	testSubRoot := []byte("subroot-bytes-32-bytes-long!!!")
	testProof := [][]byte{[]byte("proof-hash-1"), []byte("proof-hash-2")}
	testCP := fmt.Appendf(nil, "%s\n8\n%s\n", testOrigin, base64.StdEncoding.EncodeToString([]byte("roothash-bytes-32-bytes-long!!")))

	formatReqBody := func(start, end uint64, subRoot []byte, proof [][]byte, cp []byte) []byte {
		var b bytes.Buffer
		fmt.Fprintf(&b, "subtree %d %d\n", start, end)
		fmt.Fprintf(&b, "%s\n", base64.StdEncoding.EncodeToString(subRoot))
		for _, p := range proof {
			fmt.Fprintf(&b, "%s\n", base64.StdEncoding.EncodeToString(p))
		}
		fmt.Fprintf(&b, "\n")
		b.Write(cp)
		return b.Bytes()
	}

	for _, test := range []struct {
		name       string
		body       func() io.Reader
		mockTarget *mockTarget
		wantStatus int
		wantBody   string
	}{
		{
			name: "200 ok",
			body: func() io.Reader {
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, testCP))
			},
			mockTarget: &mockTarget{
				signSubtreeFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
					if start != testStart || end != testEnd {
						return nil, fmt.Errorf("got start=%d end=%d, want %d %d", start, end, testStart, testEnd)
					}
					if !bytes.Equal(subRoot, testSubRoot) {
						return nil, fmt.Errorf("got subRoot %x, want %x", subRoot, testSubRoot)
					}
					if len(proof) != len(testProof) {
						return nil, fmt.Errorf("got %d proof hashes, want %d", len(proof), len(testProof))
					}
					for i, p := range proof {
						if !bytes.Equal(p, testProof[i]) {
							return nil, fmt.Errorf("proof[%d] = %x, want %x", i, p, testProof[i])
						}
					}
					if !bytes.Equal(cp, testCP) {
						return nil, fmt.Errorf("got cp %q, want %q", cp, testCP)
					}
					return []byte(testCosig), nil
				},
			},
			wantStatus: http.StatusOK,
			wantBody:   testCosig,
		},
		{
			name: "400 malformed request body - invalid range line",
			body: func() io.Reader {
				return strings.NewReader("not-a-valid-subtree-range-header\n")
			},
			mockTarget: &mockTarget{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "400 malformed request body - invalid base64 subroot",
			body: func() io.Reader {
				return strings.NewReader(fmt.Sprintf("subtree %d %d\nnot-valid-base64!\n\n%s", testStart, testEnd, testCP))
			},
			mockTarget: &mockTarget{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "400 malformed request body - invalid base64 proof",
			body: func() io.Reader {
				return strings.NewReader(fmt.Sprintf("subtree %d %d\n%s\nnot-valid-base64!\n\n%s", testStart, testEnd, base64.StdEncoding.EncodeToString(testSubRoot), testCP))
			},
			mockTarget: &mockTarget{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "400 invalid subtree range from target",
			body: func() io.Reader {
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, testCP))
			},
			mockTarget: &mockTarget{
				signSubtreeFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
					return nil, witness.ErrSubtreeRangeInvalid
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "403 no witness signature",
			body: func() io.Reader {
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, testCP))
			},
			mockTarget: &mockTarget{
				signSubtreeFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
					return nil, witness.ErrNoWitnessSignature
				},
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "404 unknown log (from mux)",
			body: func() io.Reader {
				unknownCP := []byte("unknown-log-origin\n8\n47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\n")
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, unknownCP))
			},
			mockTarget: &mockTarget{},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "404 unknown log (from target)",
			body: func() io.Reader {
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, testCP))
			},
			mockTarget: &mockTarget{
				signSubtreeFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
					return nil, witness.ErrUnknownLog
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "422 invalid proof",
			body: func() io.Reader {
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, testCP))
			},
			mockTarget: &mockTarget{
				signSubtreeFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
					return nil, witness.ErrInvalidProof
				},
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "429 pushback",
			body: func() io.Reader {
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, testCP))
			},
			mockTarget: &mockTarget{
				signSubtreeFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
					return nil, witness.ErrPushback
				},
			},
			wantStatus: http.StatusTooManyRequests,
		},
		{
			name: "501 not implemented",
			body: func() io.Reader {
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, testCP))
			},
			mockTarget: &mockTarget{
				signSubtreeFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
					return nil, witness.ErrNotImplemented
				},
			},
			wantStatus: http.StatusNotImplemented,
		},
		{
			name: "500 internal server error",
			body: func() io.Reader {
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, testCP))
			},
			mockTarget: &mockTarget{
				signSubtreeFunc: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
					return nil, errors.New("something went wrong")
				},
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "500 malformed checkpoint in request",
			body: func() io.Reader {
				badCP := []byte("not a checkpoint")
				return bytes.NewReader(formatReqBody(testStart, testEnd, testSubRoot, testProof, badCP))
			},
			mockTarget: &mockTarget{},
			wantStatus: http.StatusInternalServerError,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := NewMirrorMux()
			_ = mux.AddTarget(testOrigin, test.mockTarget)
			h := New(mux, nil)

			req := httptest.NewRequest(http.MethodPost, "/sign-subtree", test.body())
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != test.wantStatus {
				t.Errorf("want status %d, got %d: %s", test.wantStatus, w.Code, w.Body.String())
			}
			if test.wantBody != "" {
				if got := w.Body.String(); got != test.wantBody {
					t.Errorf("want body %q, got %q", test.wantBody, got)
				}
			}
		})
	}
}

func TestMirrorMux_SignSubtree(t *testing.T) {
	const (
		origin1 = "log-1"
		origin2 = "log-2"
	)
	cp1 := fmt.Appendf(nil, "%s\n10\n47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\n", origin1)
	cp2 := fmt.Appendf(nil, "%s\n20\n47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\n", origin2)
	subRoot := []byte("subroot")
	proof := [][]byte{[]byte("p1")}

	for _, test := range []struct {
		name        string
		cp          []byte
		target1Func func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error)
		target2Func func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error)
		wantCosig   string
		wantErr     error
		wantAnyErr  bool
	}{
		{
			name: "dispatches to target 1",
			cp:   cp1,
			target1Func: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
				return []byte("cosig-1"), nil
			},
			target2Func: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
				t.Fatalf("target2 should not be called")
				return nil, nil
			},
			wantCosig: "cosig-1",
		},
		{
			name: "dispatches to target 2",
			cp:   cp2,
			target1Func: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
				t.Fatalf("target1 should not be called")
				return nil, nil
			},
			target2Func: func(ctx context.Context, start, end uint64, subRoot []byte, proof [][]byte, cp []byte) ([]byte, error) {
				return []byte("cosig-2"), nil
			},
			wantCosig: "cosig-2",
		},
		{
			name:    "unknown origin returns ErrUnknownLog",
			cp:      []byte("unknown-log\n5\n47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\n"),
			wantErr: ErrUnknownLog,
		},
		{
			name:       "malformed checkpoint returns error",
			cp:         []byte("not a valid checkpoint"),
			wantAnyErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := NewMirrorMux()
			_ = mux.AddTarget(origin1, &mockTarget{signSubtreeFunc: test.target1Func})
			_ = mux.AddTarget(origin2, &mockTarget{signSubtreeFunc: test.target2Func})

			got, err := mux.SignSubtree(t.Context(), 0, 4, subRoot, proof, test.cp)
			if test.wantAnyErr {
				if err == nil {
					t.Fatalf("got nil error, want non-nil error")
				}
				return
			}
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("got error %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(got) != test.wantCosig {
				t.Errorf("got cosig %q, want %q", string(got), test.wantCosig)
			}
		})
	}
}
