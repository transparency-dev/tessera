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
	"log/slog"
	"net/http"
	"strings"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/internal/witness"
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
	AddCheckpoint(ctx context.Context, origin string, oldSize uint64, proof [][]byte, cp []byte) ([]byte, uint64, error)
	AddEntries(ctx context.Context, origin string, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) ([]byte, error)
}

// New returns a new http.Handler for the mirror service.
func New(m Mirror) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /add-checkpoint", addCheckpoint(m))
	mux.HandleFunc("POST /add-entries", addEntries(m))
	return mux
}

func addCheckpoint(m Mirror) http.HandlerFunc {
	const maxRequestBodyBytes = 64 << 10

	return func(w http.ResponseWriter, r *http.Request) {
		origin, oldSize, proof, cp, err := parseBody(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
		if err != nil {
			slog.InfoContext(r.Context(), "Invalid witness request", slog.Any("error", err.Error()))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		sc, body, contentType, err := handleCheckpointUpdate(r.Context(), m, origin, oldSize, cp, proof)
		if err != nil {
			slog.InfoContext(r.Context(), "Witness update failed", slog.Any("error", err.Error()))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if contentType != "" {
			w.Header().Add("Content-Type", contentType)
		}
		w.WriteHeader(sc)
		if len(body) > 0 {
			if _, err := w.Write(body); err != nil {
				slog.InfoContext(r.Context(), "Witness failed to write response", slog.Any("error", err.Error()))
			}
		}
	}
}

// handleCheckpointUpdate submits the provided checkpoint to the witness and interprets any errors which may result.
//
// Returns an appropriate HTTP status code, response body, and Content Type representing the outcome.
func handleCheckpointUpdate(ctx context.Context, m Mirror, origin string, oldSize uint64, cp []byte, proof [][]byte) (int, []byte, string, error) {
	sigs, trustedSize, updateErr := m.AddCheckpoint(ctx, origin, oldSize, proof, cp)
	// Finally, handle any "soft" error from the update:
	if updateErr != nil {
		switch {
		case errors.Is(updateErr, witness.ErrCheckpointStale):
			return http.StatusConflict, fmt.Appendf(nil, "%d\n", trustedSize), "text/x.tlog.size", nil
		case errors.Is(updateErr, witness.ErrUnknownLog):
			return http.StatusNotFound, nil, "", nil
		case errors.Is(updateErr, witness.ErrNoValidSignature):
			return http.StatusForbidden, nil, "", nil
		case errors.Is(updateErr, witness.ErrOldSizeInvalid):
			return http.StatusBadRequest, nil, "", nil
		case errors.Is(updateErr, witness.ErrInvalidProof):
			return http.StatusUnprocessableEntity, nil, "", nil
		case errors.Is(updateErr, witness.ErrRootMismatch):
			return http.StatusConflict, nil, "", nil
		default:
			return http.StatusInternalServerError, nil, "", updateErr
		}
	}

	return http.StatusOK, sigs, "", nil
}

// parseBody reads the incoming request and parses into constituent parts.
//
// The request body MUST be a sequence of
// - a previous size line,
// - zero or more consistency proof lines,
// - and an empty line,
// - followed by a checkpoint.
func parseBody(r io.Reader) (string, uint64, [][]byte, []byte, error) {
	b := bufio.NewReader(r)
	sizeLine, err := b.ReadString('\n')
	if err != nil {
		return "", 0, nil, nil, err
	}
	var size uint64
	if n, err := fmt.Sscanf(strings.TrimSuffix(sizeLine, "\n"), "old %d", &size); err != nil || n != 1 {
		return "", 0, nil, nil, err
	}
	proof := [][]byte{}
	for {
		l, err := b.ReadString('\n')
		if err != nil {
			return "", 0, nil, nil, err
		}
		l = strings.TrimSuffix(l, "\n")
		if len(l) == 0 {
			break
		}
		hash, err := base64.StdEncoding.DecodeString(l)
		if err != nil {
			return "", 0, nil, nil, err
		}
		proof = append(proof, hash)
	}
	cp, err := io.ReadAll(b)
	if err != nil {
		return "", 0, nil, nil, err
	}
	s := strings.SplitN(string(cp), "\n", 2)
	if len(s) != 2 {
		return "", 0, nil, nil, errors.New("invalid checkpoint")
	}

	origin := s[0]
	return origin, size, proof, cp, nil
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
			// TODO(al): Handle various errors and respond accordingly.
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
