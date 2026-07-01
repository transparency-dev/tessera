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
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/witness/witness"
)

const (
	// maxOriginLen is the maximum length of a valid log origin.
	// This value comes from the MLDSA cosigner spec.
	maxOriginLen = 255
	// maxTicketSize is the maximum length of a tlog-mirror ticket.
	maxTicketSize = 1<<16 - 1
)

// New returns a new http.Handler for the tlog-mirror service, based on the provided mux and witness.
func New(m *MirrorMux, w *witness.Witness) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /add-checkpoint", witness.NewHTTPHandler(w).AddCheckpoint)
	mux.HandleFunc("POST /add-entries", addEntries(m))
	return mux
}

func addEntries(m *MirrorMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// SPEC: When sending responses, mirrors SHOULD send an Accept-Encoding header that includes gzip
		w.Header().Add("Accept-Encoding", "gzip")

		// SPEC: The request body MUST have Content-Type of application/octet-stream ...
		if t := strings.ToLower(r.Header.Get("Content-Type")); t != "application/octet-stream" {
			http.Error(w, fmt.Sprintf("invalid Content-Type %q", t), http.StatusBadRequest)
			return
		}
		defer func() {
			_ = r.Body.Close()
		}()
		// SPEC: Mirrors MUST support receiving Content-Encoding: gzip in add-entries requests
		reader := r.Body
		switch ce := strings.ToLower(r.Header.Get("Content-Encoding")); ce {
		case "":
			// No Content-Encoding header, nothing to do.
			break
		case "gzip":
			var err error
			reader, err = gzip.NewReader(reader)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid gzip request body: %v", err), http.StatusBadRequest)
				return
			}
			defer func() {
				// Note that this only closes the gzip reader, it doesn't close the underlying HTTP body reader.
				_ = reader.Close()
			}()
		default:
			// Log unknown encodings and treat as malformed request
			slog.DebugContext(r.Context(), "Unknown Content-Encoding", slog.String("encoding", ce))
			http.Error(w, "Unsupported content encoding", http.StatusBadRequest)
			return
		}
		req, err := parseAddEntriesPreamble(reader)
		if err != nil {
			slog.DebugContext(r.Context(), "Failed to parse request preamble", slog.Any("err", err))
			http.Error(w, "Invalid request preamble", http.StatusBadRequest)
			return
		}

		nextEntry, pendingSize, ticket, cosigs, err := m.AddEntries(r.Context(), req.logOrigin, req.uploadStart, req.uploadEnd, req.ticket, req.NextPackage)
		switch {
		case errors.Is(err, ErrUnknownLog):
			// SPEC: If log_origin is not a known log, the mirror MUST respond with a "404 Not Found" HTTP status code.
			http.Error(w, err.Error(), http.StatusNotFound)
			return

		case errors.Is(err, tessera.ErrConflict):
			// SPEC: When sending a "409 Conflict" or "202 Accepted" response, the response body MUST have a Content-Type of text/x.tlog.mirror-info and consist of three lines, each followed by a newline (U+000A):
			// The tree size of a valid pending checkpoint, in decimal
			// The next entry, in decimal
			// An opaque, possibly zero length, ticket value, encoded in base64
			w.Header().Set("Content-Type", "text/x.tlog.mirror-info")
			w.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprintf(w, "%d\n%d\n%s\n", pendingSize, nextEntry, base64.StdEncoding.EncodeToString(ticket))
			return

		case errors.Is(err, tessera.ErrNoPendingCheckpoint):
			http.Error(w, err.Error(), http.StatusBadRequest)
			return

		case errors.Is(err, tessera.ErrInvalidProof):
			// SPEC: If this (subtree consistency proof) verification process fails, it MUST respond with a "422 Unprocessable Entity"
			// HTTP status code and end processing.
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return

		case err != nil:
			slog.DebugContext(r.Context(), "Failed to add entries", slog.String("origin", req.logOrigin), slog.Any("err", err))
			http.Error(w, "Failed to add entries", http.StatusInternalServerError)
			return

		case nextEntry == pendingSize:
			// SPEC: If next_entry == upload_end, and no cosignatures are provided by the mirror,
			// the mirror MUST respond with a "200 OK" status code and an empty response body.
			// If next_entry == upload_end, and cosignatures are provided by the mirror,
			// the mirror MUST respond with a "200 OK" status code and the cosignatures.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(cosigs) // We don't care about failures here.
			return

		case nextEntry == req.uploadStart:
			// SPEC: If no entry package was authenticated and saved before the body ended (for example, the request header itself was malformed, or the first package's bytes were truncated mid-package), the mirror MUST respond with a "400 Bad Request" HTTP status code.
			http.Error(w, "truncated or malformed body: no entry packages saved", http.StatusBadRequest)
			return

		case nextEntry > req.uploadStart:
			// SPEC: If at least one entry package was authenticated and saved, but the mirror has not yet received all packages covering [upload_start, upload_end), the mirror MUST respond with a "202 Accepted" response carrying the advanced next entry.
			w.Header().Set("Content-Type", "text/x.tlog.mirror-info")
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, "%d\n%d\n%s\n", pendingSize, nextEntry, base64.StdEncoding.EncodeToString(ticket)) // We don't care about failures here.
			return

		default:
			// This should never happen.
			slog.ErrorContext(r.Context(), "LOGIC ERROR: invalid state", slog.Uint64("nextEntry", nextEntry), slog.Uint64("uploadStart", req.uploadStart), slog.Uint64("pendingSize", pendingSize))
			http.Error(w, "invalid state", http.StatusInternalServerError)
			return
		}
	}
}

// addEntriesRequest represents the body of a request to the tlog-mirror add-entries endpoint.
type addEntriesRequest struct {
	logOrigin   string
	uploadStart uint64
	uploadEnd   uint64
	ticket      []byte
	body        io.Reader

	// start is the index of the next entry in the stream of entry packages.
	// Initially it will be equal to uploadStart, and will be moved forwards
	// by calls to NextPackage().
	start uint64
}

// NextPackage returns the next entry package in the request, if any.
//
// Once all packages from the request have been consumed, this func will continue
// to return io.EOF.
//
// Not thread-safe.
func (a *addEntriesRequest) NextPackage() (*tessera.MirrorPackage, error) {
	if a.start >= a.uploadEnd {
		return nil, io.EOF
	}

	// Calculate how many entries in this package
	end := min(a.uploadEnd, (a.start/256+1)*256)
	numEntries := int(end - a.start)

	// Now parse the package.
	//
	// SPEC: The package MUST contain the following values, concatenated.
	//         - The log entries in [start, end), each with a big-endian uint16 length prefix,
	//         - 1 byte, encoding an 8-bit unsigned integer, num_hashes, which MUST be at most 63,
	//         - num_hashes subtree consistency proof hash values.

	// First the entries themselves.
	var entries [][]byte
	for range numEntries {
		var entryLen uint16
		if err := binary.Read(a.body, binary.BigEndian, &entryLen); err != nil {
			return nil, fmt.Errorf("failed to read entry length: %v", err)
		}
		entry := make([]byte, entryLen)
		if _, err := io.ReadFull(a.body, entry); err != nil {
			return nil, fmt.Errorf("failed to read entry: %v", err)
		}
		entries = append(entries, entry)
	}

	// Now the proof length.
	var numHashes uint8
	if err := binary.Read(a.body, binary.BigEndian, &numHashes); err != nil {
		return nil, fmt.Errorf("failed to read num_hashes: %v", err)
	}
	if numHashes > 63 {
		return nil, fmt.Errorf("too many hashes: %d", numHashes)
	}

	// Finally, the proof itself.
	var proofs [][]byte
	for i := 0; i < int(numHashes); i++ {
		hash := make([]byte, 32) // Proof is comprised of SHA256 hashes.
		if _, err := io.ReadFull(a.body, hash); err != nil {
			return nil, fmt.Errorf("failed to read hash: %v", err)
		}
		proofs = append(proofs, hash)
	}

	// Update next expected entry index.
	a.start = end
	return &tessera.MirrorPackage{Entries: entries, Proof: proofs}, nil
}

// parseAddEntriesPreamble consumes the body of the Add-Entries request.
//
// This func effectively takes ownership of the provided Reader, it MUST NOT be
// further used by the caller.
//
// Returns a struct which represents the request, and provides streaming access to the entry packages.
func parseAddEntriesPreamble(r io.Reader) (*addEntriesRequest, error) {
	// SPEC: The request body MUST ... contain the following values, concatenated:
	//         2 bytes, encoding a big-endian uint16: log_origin_size
	//         log_origin_size bytes, containing the log origin: log_origin
	//         8 bytes, encoding a big-endian uint64: upload_start
	//         8 bytes, encoding a big-endian uint64: upload_end
	//         2 bytes, encoding a big-endian uint16: ticket_size
	//         ticket_size bytes, containing an opaque ticket value, described below
	//         A sequence of entry packages
	var logOriginSize uint16
	if err := binary.Read(r, binary.BigEndian, &logOriginSize); err != nil {
		return nil, err
	}
	if logOriginSize > maxOriginLen {
		return nil, errors.New("log origin too long")
	}
	logOrigin := make([]byte, logOriginSize)
	if _, err := io.ReadFull(r, logOrigin); err != nil {
		return nil, err
	}

	var uploadStart uint64
	if err := binary.Read(r, binary.BigEndian, &uploadStart); err != nil {
		return nil, err
	}

	var uploadEnd uint64
	if err := binary.Read(r, binary.BigEndian, &uploadEnd); err != nil {
		return nil, err
	}

	var ticketSize uint16
	if err := binary.Read(r, binary.BigEndian, &ticketSize); err != nil {
		return nil, err
	}
	if ticketSize > maxTicketSize {
		return nil, errors.New("ticket too large")
	}
	var ticket []byte
	if ticketSize > 0 {
		ticket = make([]byte, ticketSize)
		if _, err := io.ReadFull(r, ticket); err != nil {
			return nil, err
		}
	}

	// SPEC: upload_start MUST be less or equal to upload_end
	if uploadStart > uploadEnd {
		return nil, fmt.Errorf("uploadStart (%d) > uploadEnd (%d)", uploadStart, uploadEnd)
	}

	return &addEntriesRequest{
		logOrigin:   string(logOrigin),
		uploadStart: uploadStart,
		uploadEnd:   uploadEnd,
		ticket:      ticket,
		body:        r,
		start:       uploadStart,
	}, nil
}
