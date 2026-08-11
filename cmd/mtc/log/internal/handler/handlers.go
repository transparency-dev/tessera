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
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/transparency-dev/tessera/cmd/mtc/log"
)

const (
	// maxAddTBSRequestBodyBytes is the maximum allowed HTTP request body size
	// (128 KiB) for JSON submissions. This accommodates base64 encoding and JSON
	// formatting overhead for certificates up to 64 KiB binary size.
	maxAddTBSRequestBodyBytes = 128 << 10
)

type addTBS func(context.Context, log.TBSCertificateLogEntry) (*log.AddTBSRsp, error)
type proofToLandmark func(context.Context, uint64) ([]byte, time.Duration, error)

// New returns a new http.Handler for the mtc-tlog service.
func New(mtcLog *log.MTCLog) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /add-tbs", http.MaxBytesHandler(addTBSHandler(mtcLog.AddTBS), maxAddTBSRequestBodyBytes))
	mux.Handle("GET /proof-to-landmark", proofToLandmarkHandler(mtcLog.ProofToLandmark))
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
				slog.ErrorContext(r.Context(), "r.Body.Close()", slog.Any("error", err))
			}
		}()

		var entry log.TBSCertificateLogEntry
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			slog.DebugContext(r.Context(), "rejection: malformed JSON submission", slog.Any("error", err))
			http.Error(w, "Invalid TBSCertificateLogEntry JSON payload", http.StatusBadRequest)
			return
		}
		if err := entry.Validate(); err != nil {
			slog.DebugContext(r.Context(), "rejection: invalid TBSCertificateLogEntry fields", slog.Any("error", err))
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

func proofToLandmarkHandler(fn proofToLandmark) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		indexStr := r.URL.Query().Get("index")
		if indexStr == "" {
			slog.DebugContext(r.Context(), "rejection: missing index query parameter")
			http.Error(w, "missing 'index' query parameter", http.StatusBadRequest)
			return
		}
		index, err := strconv.ParseUint(indexStr, 10, 64)
		if err != nil {
			slog.DebugContext(r.Context(), "rejection: invalid index query parameter", slog.Any("error", err))
			http.Error(w, "invalid 'index' query parameter", http.StatusBadRequest)
			return
		}

		proof, retryAfter, err := fn(r.Context(), index)
		if err != nil {
			switch {
			case errors.Is(err, log.ErrTooOld):
				slog.DebugContext(r.Context(), "rejection: requested index precedes active landmarks", slog.Uint64("index", index), slog.Any("error", err))
				http.Error(w, err.Error(), http.StatusGone)
				return
			case errors.Is(err, log.ErrExceedsTreeSize):
				slog.DebugContext(r.Context(), "rejection: requested index exceeds tree size", slog.Uint64("index", index), slog.Any("error", err))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			default:
				slog.ErrorContext(r.Context(), "failed to fetch inclusion proof to landmark", slog.Uint64("index", index), slog.Any("error", err))
				http.Error(w, "Could not fetch inclusion proof to landmark", http.StatusInternalServerError)
				return
			}
		}

		if retryAfter > 0 {
			retrySeconds := max(1, int(math.Ceil(retryAfter.Seconds())))
			w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
			w.WriteHeader(http.StatusAccepted)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(log.ProofToLandmarkRsp{MTCProof: proof}); err != nil {
			slog.ErrorContext(r.Context(), "failed to write response", slog.Any("error", err))
		}
	})
}
