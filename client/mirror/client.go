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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/transparency-dev/tessera/client"
)

// PackageProver is an interface for generating proof hashes required for
// the entry package covering the index interval [start, end).
type PackageProver interface {
	// PackageProof computes and returns the proof hashes required by
	// the mirror for the specified entry package.
	PackageProof(ctx context.Context, start, end uint64) ([][]byte, error)
}

// Options holds the configuration for a tlog-mirror Client.
type Options struct {
	mirrorURL               *url.URL
	httpClient              *http.Client
	logOrigin               string
	tileFetcher             client.TileFetcherFunc
	bundleFetcher           client.EntryBundleFetcherFunc
	mirrorCheckpointFetcher client.CheckpointFetcherFunc
	prover                  PackageProver
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

// WithProver sets the package prover.
func (o *Options) WithProver(prover PackageProver) *Options {
	o.prover = prover
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
	if o.prover == nil {
		return errors.New("prover is required")
	}
	return nil
}

// Client is a push-based client designed to synchronize entries and checkpoints
// from a source log to a tlog-mirror compliant server.
// TODO(roger2hk): Should multiple mirrors in one client be supported?
type Client struct {
	opts *Options
}

// NewClient creates a new Client with the provided options.
func NewClient(_ context.Context, opts Options) (*Client, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	return &Client{opts: &opts}, nil
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

// parseConflict parses the text/x.tlog.mirror-info body of a 409 Conflict response.
// Format:
//   - The tree size of a valid pending checkpoint, in decimal
//   - The next entry, in decimal
//   - An opaque, possibly zero length, ticket value, encoded in base64
func parseConflict(r io.Reader) error {
	// TODO(roger2hk): Implement this.

	return errors.New("TODO")
}

// pushEntries streams entry packages and their proofs to the mirror's /add-entries endpoint.
// nolint:unused
func (c *Client) pushEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte) error {
	pr, _ := io.Pipe()

	// TODO(roger2hk): Implement streaming entries.

	u, err := c.opts.mirrorURL.Parse("add-entries")
	if err != nil {
		return fmt.Errorf("failed to parse add-entries URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), pr)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := c.opts.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s failed: %w", u, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusConflict {
		return parseConflict(resp.Body)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add-entries failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// pushCheckpoint sends a new checkpoint and its consistency proof to the mirror's /add-checkpoint endpoint.
// nolint:unused
func (c *Client) pushCheckpoint(ctx context.Context, oldSize uint64, proof [][]byte, checkpointRaw []byte) ([]byte, error) {
	// TODO(roger2hk): Implement checkpoint.
	var reqBody io.Reader

	u, err := c.opts.mirrorURL.Parse("add-checkpoint")
	if err != nil {
		return nil, fmt.Errorf("failed to parse add-checkpoint URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain") // add-checkpoint uses standard line-oriented payload

	resp, err := c.opts.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s failed: %w", u, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("add-checkpoint failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	cosigs, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read cosignatures from response body: %w", err)
	}

	return cosigs, nil
}

// Sync synchronizes all entries and the checkpoint from the source log to the mirror
// up to the specified targetSize. It returns the mirror's cosignatures on success.
func (c *Client) Sync(ctx context.Context, targetCheckpointRaw []byte, targetSize uint64) ([]byte, error) {
	// TODO(roger2hk):
	// 1. Get the mirror's current state by querying it with upload_start=0, upload_end=0 (guaranteed to conflict).
	// 2. If the mirror's pending checkpoint is smaller than target size, update it first.
	// 3. Push entries up to target size in packages of 256, handling concurrent conflicts and retries.

	return nil, errors.New("WIP")
}
