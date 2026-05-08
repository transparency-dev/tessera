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
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"log/slog"

	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/mirror"
	"github.com/transparency-dev/tessera/internal/parse"
)

const (
	// maxOriginLen is the maximum length of a valid log origin.
	// This value comes from the MLDSA cosigner spec.
	maxOriginLen = 255
	// maxTicketSize is the maximum length of a tlog-mirror ticket.
	maxTicketSize = 1<<16 - 1
)

// Mirror is the interface that the handler uses to interact with the mirror's state.
type Mirror interface {
	AddCheckpoint(ctx context.Context, oldSize uint64, proof [][]byte, cp []byte) error
	AddEntries(ctx context.Context, logOrigin string, uploadStart, uploadEnd uint64, ticket []byte, next func() (*mirror.Package, error)) ([]byte, error)
}

// New returns a new http.Handler for the mirror service.
func New(m Mirror) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /add-checkpoint", addCheckpoint(m))
	mux.HandleFunc("POST /add-entries", addEntries(m))
	return mux
}

func addCheckpoint(m Mirror) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// SPEC: The mirror implements a [tlog-]witness's add-checkpoint endpoint.
		//       MUST be a sequence of:
		//         - an old size line,
		//         - zero or more consistency proof lines,
		//         - and an empty line,
		//         - followed by a checkpoint.

		reader := bufio.NewReader(r.Body)

		// 1. Read old size line.
		oldLine, err := reader.ReadString('\n')
		if err != nil {
			http.Error(w, "missing old size", http.StatusBadRequest)
			return
		}
		oldLine = strings.TrimSpace(oldLine)
		if !strings.HasPrefix(oldLine, "old ") {
			http.Error(w, "invalid old size line", http.StatusBadRequest)
			return
		}
		oldSize, err := strconv.ParseUint(strings.TrimPrefix(oldLine, "old "), 10, 64)
		if err != nil {
			http.Error(w, "invalid old size", http.StatusBadRequest)
			return
		}

		// 2. Read consistency proof lines until an empty line.
		var proof [][]byte
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				http.Error(w, "unexpected EOF while reading proof", http.StatusBadRequest)
				return
			}
			line = strings.TrimSpace(line)
			if line == "" {
				break
			}
			p, err := base64.StdEncoding.DecodeString(line)
			if err != nil {
				http.Error(w, "invalid proof line", http.StatusBadRequest)
				return
			}
			proof = append(proof, p)
		}

		// 3. Remaining data is the checkpoint.
		cp, err := io.ReadAll(reader)
		if err != nil {
			http.Error(w, "failed to read checkpoint", http.StatusBadRequest)
			return
		}

		if _, _, _, err := parse.CheckpointUnsafe(cp); err != nil {
			http.Error(w, fmt.Sprintf("invalid checkpoint: %v", err), http.StatusBadRequest)
			return
		}

		if err := m.AddCheckpoint(r.Context(), oldSize, proof, cp); err != nil {
			slog.ErrorContext(r.Context(), "AddCheckpoint failed", slog.Any("error", err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		// TODO(al): Maybe return a tlog-witness only cosignature here from a separate key?
	}
}

func addEntries(m Mirror) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// SPEC: The request body MUST have Content-Type of application/octet-stream ...
		if t := strings.ToLower(r.Header.Get("Content-Type")); t != "application/octet-stream" {
			http.Error(w, fmt.Sprintf("invalid Content-Type %q", t), http.StatusBadRequest)
			return
		}
		req, err := parseAddEntriesPreamble(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to parse header: %v", err), http.StatusBadRequest)
			return
		}

		cosigs, err := m.AddEntries(r.Context(), req.logOrigin, req.uploadStart, req.uploadEnd, req.ticket, req.NextPackage)
		if err != nil {
			// TODO(al): Handle Conflict (409) if it's a conflict error.
			slog.ErrorContext(r.Context(), "AddEntries failed", slog.Any("error", err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(cosigs)
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
func (a *addEntriesRequest) NextPackage() (*mirror.Package, error) {
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
	return &mirror.Package{Entries: entries, Proof: proofs}, nil
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
