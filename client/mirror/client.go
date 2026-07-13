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

// Package mirror provides client support for pushing checkpoints and entries
// to a mirror server compliant with the [tlog-mirror spec].
//
// [tlog-mirror spec]: https://c2sp.org/tlog-mirror
package mirror

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
)

// PackageProverFunc computes and returns the proof hashes required by the mirror
// for the specified entry package covering the index interval [start, end)
// in a tree of the specified size.
type PackageProverFunc func(ctx context.Context, start, end, size uint64) ([][]byte, error)

// Options holds the configuration for a tlog-mirror Client.
type Options struct {
	mirrorURL               *url.URL
	httpClient              *http.Client
	logOrigin               string
	tileFetcher             client.TileFetcherFunc
	bundleFetcher           client.EntryBundleFetcherFunc
	mirrorCheckpointFetcher client.CheckpointFetcherFunc
	packageProver           PackageProverFunc
}

// NewOptions returns a new Options object with default values.
func NewOptions() *Options {
	return &Options{
		httpClient: http.DefaultClient,
	}
}

// WithMirrorURL sets the mirror URL.
func (o *Options) WithMirrorURL(mirrorURL *url.URL) *Options {
	o.mirrorURL = mirrorURL
	return o
}

// WithHTTPClient sets the HTTP client.
func (o *Options) WithHTTPClient(httpClient *http.Client) *Options {
	o.httpClient = httpClient
	return o
}

// WithLogOrigin sets the log origin.
func (o *Options) WithLogOrigin(logOrigin string) *Options {
	o.logOrigin = logOrigin
	return o
}

// WithTileFetcher sets the tile fetcher.
func (o *Options) WithTileFetcher(tileFetcher client.TileFetcherFunc) *Options {
	o.tileFetcher = tileFetcher
	return o
}

// WithBundleFetcher sets the entry bundle fetcher.
func (o *Options) WithBundleFetcher(bundleFetcher client.EntryBundleFetcherFunc) *Options {
	o.bundleFetcher = bundleFetcher
	return o
}

// WithMirrorCheckpointFetcher sets the mirror checkpoint fetcher.
func (o *Options) WithMirrorCheckpointFetcher(mirrorCheckpointFetcher client.CheckpointFetcherFunc) *Options {
	o.mirrorCheckpointFetcher = mirrorCheckpointFetcher
	return o
}

// WithPackageProver sets the package prover.
func (o *Options) WithPackageProver(packageProver PackageProverFunc) *Options {
	o.packageProver = packageProver
	return o
}

// validate checks that the Options are valid.
func (o *Options) validate() error {
	if o.mirrorURL == nil {
		return errors.New("mirror URL is required")
	}
	if o.httpClient == nil {
		return errors.New("HTTP client is required")
	}
	if o.logOrigin == "" {
		return errors.New("log origin is required")
	}
	if o.tileFetcher == nil {
		return errors.New("tile fetcher is required")
	}
	if o.bundleFetcher == nil {
		return errors.New("bundle fetcher is required")
	}
	if o.mirrorCheckpointFetcher == nil {
		return errors.New("mirror checkpoint fetcher is required")
	}
	return nil
}

// Client is a push-based client designed to synchronize entries and checkpoints
// from a source log to a tlog-mirror compliant server.
// TODO(roger2hk): Should multiple mirrors in one client be supported?
type Client struct {
	opts *Options

	// TODO(roger2hk): Add a mutex just in case client is used across multiple goroutines.
	// State of the mirror.
	oldSize uint64
	ticket  []byte
}

// NewClient creates a new Client with the provided options.
func NewClient(_ context.Context, opts *Options) (*Client, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	// Use default subtree consistency proof if not provided.
	if opts.packageProver == nil {
		opts.packageProver = func(ctx context.Context, start, end, size uint64) ([][]byte, error) {
			pb, err := client.NewProofBuilder(ctx, size, opts.tileFetcher)
			if err != nil {
				return nil, err
			}
			return pb.SubtreeConsistencyProof(ctx, start, end)
		}
	}

	return &Client{opts: opts}, nil
}

// ErrConflict is returned by tlog-mirror client operations when the mirror returns a 409 Conflict.
type ErrConflict struct {
	PendingSize uint64
	NextEntry   uint64
	Ticket      []byte
}

func (e ErrConflict) Error() string {
	return fmt.Sprintf("mirror sync conflict: pending size %d, next entry %d, ticket length %d", e.PendingSize, e.NextEntry, len(e.Ticket))
}

// parseConflict parses the text/x.tlog.mirror-info body of a 409 Conflict response or a 202 Accepted response.
// Format:
//   - The tree size of a valid pending checkpoint, in decimal
//   - The next entry, in decimal
//   - An opaque, possibly zero length, ticket value, encoded in base64
func parseConflict(r io.Reader) error {
	all, err := io.ReadAll(&io.LimitedReader{N: 10 << 10, R: r})
	if err != nil {
		return err
	}

	bits := bytes.Split(all, []byte("\n"))
	if len(bits) != 4 {
		return fmt.Errorf("got %d fields, want 4", len(bits))
	}
	pendingSize, err := strconv.ParseUint(string(bits[0]), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid pending tree size %q: %w", bits[0], err)
	}

	nextEntry, err := strconv.ParseUint(string(bits[1]), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid next entry %q: %w", bits[1], err)
	}

	ticket, err := base64.StdEncoding.DecodeString(string(bits[2]))
	if err != nil {
		return fmt.Errorf("invalid base64 ticket: %w", err)
	}

	return ErrConflict{
		PendingSize: pendingSize,
		NextEntry:   nextEntry,
		Ticket:      ticket,
	}
}

// pushEntries streams entry packages and their proofs to the mirror's /add-entries endpoint.
// It returns the mirror's cosignatures on success.
func (c *Client) pushEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pr, pw := io.Pipe()
	defer func() {
		_ = pr.Close()
	}()

	go c.streamEntries(ctx, uploadStart, uploadEnd, ticket, pw)

	u, err := c.opts.mirrorURL.Parse("add-entries")
	if err != nil {
		return nil, fmt.Errorf("failed to parse add-entries URL: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), pr)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := c.opts.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s failed: %v", u, err)
	}
	defer func() {
		// Drain any remaining response body to enable HTTP keep-alive/connection reuse.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusAccepted {
		return nil, parseConflict(resp.Body)
	}

	if resp.StatusCode == http.StatusUnprocessableEntity {
		return nil, ErrConflict{}
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("add-entries failed with status %d: %s", resp.StatusCode, string(body))
	}

	cosigs, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read cosignatures from response body: %v", err)
	}

	return cosigs, nil
}

// streamEntries serializes, compresses, and writes the log origin, upload range, ticket,
// and the corresponding entry packages with their proofs to the pipe writer.
// It closes the pipe writer with an error if any operation fails, or nil on success.
func (c *Client) streamEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, pw *io.PipeWriter) {
	gw := gzip.NewWriter(pw)
	defer func() {
		_ = gw.Close()
		_ = pw.Close()
	}()

	// 2 bytes, encoding a big-endian uint16: log_origin_size
	if err := binary.Write(gw, binary.BigEndian, uint16(len(c.opts.logOrigin))); err != nil {
		_ = pw.CloseWithError(err)
		return
	}
	// log_origin_size bytes, containing the log origin: log_origin
	if _, err := gw.Write([]byte(c.opts.logOrigin)); err != nil {
		_ = pw.CloseWithError(err)
		return
	}
	// 8 bytes, encoding a big-endian uint64: upload_start
	if err := binary.Write(gw, binary.BigEndian, uploadStart); err != nil {
		_ = pw.CloseWithError(err)
		return
	}
	// 8 bytes, encoding a big-endian uint64: upload_end
	if err := binary.Write(gw, binary.BigEndian, uploadEnd); err != nil {
		_ = pw.CloseWithError(err)
		return
	}
	// 2 bytes, encoding a big-endian uint16: ticket_size
	if err := binary.Write(gw, binary.BigEndian, uint16(len(ticket))); err != nil {
		_ = pw.CloseWithError(err)
		return
	}
	// ticket_size bytes, containing an opaque ticket value
	if len(ticket) > 0 {
		if _, err := gw.Write(ticket); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
	}
	// A sequence of entry packages
	curr := uploadStart
	for curr < uploadEnd {
		pkgEnd := min(uploadEnd, (curr/256+1)*256)
		numEntries := pkgEnd - curr

		bundleIndex := curr / 256
		bundle, err := client.GetEntryBundle(ctx, c.opts.bundleFetcher, bundleIndex, uploadEnd)
		if err != nil {
			_ = pw.CloseWithError(fmt.Errorf("failed to fetch entry bundle %d: %w", bundleIndex, err))
			return
		}

		startIdx := curr % 256
		endIdx := startIdx + numEntries

		for i := startIdx; i < endIdx; i++ {
			entry := bundle.Entries[i]
			if err := binary.Write(gw, binary.BigEndian, uint16(len(entry))); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			if _, err := gw.Write(entry); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}

		proof, err := c.opts.packageProver(ctx, bundleIndex*layout.EntryBundleWidth, pkgEnd, uploadEnd)
		if err != nil {
			_ = pw.CloseWithError(fmt.Errorf("failed to generate proof [%d, %d): %w", bundleIndex*layout.EntryBundleWidth, pkgEnd, err))
			return
		}

		if err := binary.Write(gw, binary.BigEndian, uint8(len(proof))); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		for _, h := range proof {
			if _, err := gw.Write(h); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}

		curr = pkgEnd
	}
}

// pushCheckpoint sends a new checkpoint and its consistency proof to the mirror's /add-checkpoint endpoint.
func (c *Client) pushCheckpoint(ctx context.Context, oldSize, targetSize uint64, checkpointRaw []byte) error {
	var proof [][]byte
	if oldSize > 0 {
		pb, err := client.NewProofBuilder(ctx, targetSize, c.opts.tileFetcher)
		if err != nil {
			return fmt.Errorf("failed to create ProofBuilder: %v", err)
		}
		proof, err = pb.ConsistencyProof(ctx, oldSize, targetSize)
		if err != nil {
			return fmt.Errorf("failed to generate consistency proof: %v", err)
		}
	}

	reqBody := c.buildCheckpointRequestBody(oldSize, proof, checkpointRaw)

	u, err := c.opts.mirrorURL.Parse("add-checkpoint")
	if err != nil {
		return fmt.Errorf("failed to parse add-checkpoint URL: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), reqBody)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain") // add-checkpoint uses standard line-oriented payload

	resp, err := c.opts.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s failed: %v", u, err)
	}
	defer func() {
		// Drain any remaining response body to enable HTTP keep-alive/connection reuse.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read body from witness at %q: %v", u.String(), err)
	}

	if resp.StatusCode == http.StatusConflict {
		// The witness MUST check that the old size matches the size of the latest checkpoint it cosigned
		// for the checkpoint's origin (or zero if it never cosigned a checkpoint for that origin).
		// If it doesn't match, the witness MUST respond with a "409 Conflict" HTTP status code.
		// The response body MUST consist of the tree size of the latest cosigned checkpoint in decimal,
		// followed by a newline (U+000A). The response MUST have a Content-Type of text/x.tlog.size
		if resp.Header.Get("Content-Type") == "text/x.tlog.size" {
			bodyStr := strings.TrimSpace(string(respBody))
			newWitSize, err := strconv.ParseUint(bodyStr, 10, 64)
			if err != nil {
				return fmt.Errorf("witness at %q replied with x.tlog.size but body %q could not be parsed as decimal", u.String(), bodyStr)
			}
			slog.InfoContext(ctx, "Witness replied with x.tlog.size different than our hint", slog.String("url", u.String()), slog.Uint64("reply", newWitSize), slog.Uint64("hinted", oldSize))

			return ErrConflict{
				PendingSize: newWitSize,
			}
		}
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("add-checkpoint failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// buildCheckpointRequestBody formats the request body for the witness.
// The request body MUST be a sequence of
// - a previous size line,
// - zero or more consistency proof lines,
// - and an empty line,
// - followed by a [checkpoint][].
func (c *Client) buildCheckpointRequestBody(oldSize uint64, proof [][]byte, checkpointRaw []byte) io.Reader {
	var b bytes.Buffer
	fmt.Fprintf(&b, "old %d\n", oldSize)

	// Preallocate for 32 byte SHA256 nodes.
	dst := make([]byte, base64.StdEncoding.EncodedLen(32))

	for _, p := range proof {
		base64.StdEncoding.Encode(dst, p)
		b.Write(dst)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.Write(checkpointRaw)
	return &b
}

// Sync synchronizes all entries and the checkpoint from the source log to the mirror
// up to the specified targetSize. It returns the mirror's cosignatures on success.
func (c *Client) Sync(ctx context.Context, targetCheckpointRaw []byte, targetSize uint64) ([]byte, error) {
	var conflict ErrConflict
	nextEntry := c.oldSize

	// Push the checkpoint with the old size (0 if not provided).
	for c.oldSize < targetSize {
		err := c.pushCheckpoint(ctx, c.oldSize, targetSize, targetCheckpointRaw)
		if err == nil {
			nextEntry = c.oldSize
			c.oldSize = targetSize
			break
		}
		if !errors.As(err, &conflict) {
			return nil, fmt.Errorf("failed to push checkpoint: %v", err)
		}
		c.oldSize = max(c.oldSize, conflict.PendingSize)
	}

	// Push entries up to target size in packages of 256, handling concurrent conflicts and retries.
	uploadEnd := max(targetSize, c.oldSize)

	var cosigs []byte
	for {
		var err error
		cosigs, err = c.pushEntries(ctx, nextEntry, uploadEnd, c.ticket)
		if err == nil {
			break
		}

		if !errors.As(err, &conflict) {
			return nil, fmt.Errorf("sync failed during entry upload: %v", err)
		}

		uploadEnd = conflict.PendingSize
		nextEntry = conflict.NextEntry
		c.ticket = conflict.Ticket
		c.oldSize = conflict.PendingSize

		if uploadEnd < targetSize {
			return nil, fmt.Errorf("mirror size reverted to %d, which is smaller than target %d", uploadEnd, targetSize)
		}
	}

	return cosigs, nil
}
