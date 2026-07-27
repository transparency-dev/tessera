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
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/transparency-dev/tessera/cmd/mtc/log"
)

const (
	// maxAddTBSRequestBodyBytes is the maximum allowed HTTP request body size
	// (128 KiB) for JSON submissions. This accommodates base64 encoding and JSON
	// formatting overhead for certificates up to 64 KiB binary size.
	maxAddTBSRequestBodyBytes = 128 << 10
)

type addTBS func(context.Context, log.TBSCertificateLogEntry) (*log.AddTBSRsp, error)

// New returns a new http.Handler for the mtc-tlog service.
func New(mtcLog *log.MTCLog) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /add-tbs", http.MaxBytesHandler(addTBSHandler(mtcLog.AddTBS), maxAddTBSRequestBodyBytes))
	mux.HandleFunc("GET /proof-to-landmark", func(w http.ResponseWriter, r *http.Request) {
		// TODO parse request
		// TODO write response
		if _, err := mtcLog.ProofToLandmark(r.Context(), 0); err != nil {
			slog.ErrorContext(r.Context(), "Failed to fetch inclusion proof to landmark", slog.Any("error", err))
		}
		http.Error(w, "not implemented", http.StatusNotImplemented)
	})
	return mux
}

// addTBSHandler returns a handler which logs a TBSCertificateLogEntry.
//
// This handler:
//
//   - Accepts JSON-Encoded TBSCertificateLogentry, serializes it in
//     DER format, encapsulates it in a TLS encoded MTCLogEntry, and logs it
//     using the argument add function.
//   - Returns an AddTBSRsp JSON payload containing an index and an MTCProof.
func addTBSHandler(add addTBS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := r.Body.Close(); err != nil {
				slog.ErrorContext(r.Context(), "resp.Body.Close()", slog.Any("error", err))
			}
		}()

		var entry log.TBSCertificateLogEntry
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			slog.WarnContext(r.Context(), "rejection: malformed JSON submission", slog.Any("error", err))
			http.Error(w, "Invalid TBSCertificateLogEntry JSON payload", http.StatusBadRequest)
			return
		}
		if err := entry.Validate(); err != nil {
			slog.WarnContext(r.Context(), "rejection: invalid TBSCertificateLogEntry fields", slog.Any("error", err))
			http.Error(w, fmt.Sprintf("Invalid TBSCertificateLogEntry: %v", err.Error()), http.StatusBadRequest)
			return
		}

		rsp, err := add(r.Context(), entry)
		if err != nil {
			slog.ErrorContext(r.Context(), "failed to add entry to MTC log", slog.Any("error", err))
			http.Error(w, "Could not add entry to log", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(rsp); err != nil {
			slog.ErrorContext(r.Context(), "failed to write response", slog.Any("error", err))
		}
	})
}
