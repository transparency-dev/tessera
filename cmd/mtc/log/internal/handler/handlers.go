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
	"log/slog"
	"net/http"

	"github.com/transparency-dev/tessera/cmd/mtc/log"
)

type addTBS func(context.Context, log.TBSCertificateLogEntry) (uint64, log.MTCProof, error)

// New returns a new http.Handler for the mtc-tlog service.
func New(mtcLog *log.MTCLog) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /add-tbs", addTBSHandler(mtcLog.AddTBS))
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

// addTBSHandler returns a handler which logs a DER-encoded TBSCertificateLogEntry.
func addTBSHandler(add addTBS) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO: parse request
		// TODO: write response
		if _, _, err := add(r.Context(), log.TBSCertificateLogEntry{}); err != nil {
			slog.ErrorContext(r.Context(), "Failed to add entry to MTC log", slog.Any("error", err))
		}
		http.Error(w, "not implemented", http.StatusNotImplemented)
	}
}
