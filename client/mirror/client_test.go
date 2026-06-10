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

package mirror

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/transparency-dev/tessera/client"
)

func TestParseConflict(t *testing.T) {
	for _, tc := range []struct {
		desc    string
		body    string
		wantErr bool
		want    ErrConflict
	}{
		{
			desc: "valid conflict with ticket",
			body: "123\n456\ndGlja2V0LXZhbHVl\n", // dGlja2V0LXZhbHVl is base64 for "ticket-value"
			want: ErrConflict{
				PendingSize: 123,
				NextEntry:   456,
				Ticket:      []byte("ticket-value"),
			},
		},
		{
			desc: "valid conflict with empty ticket",
			body: "999\n1000\n\n",
			want: ErrConflict{
				PendingSize: 999,
				NextEntry:   1000,
				Ticket:      []byte{},
			},
		},
		{
			desc:    "missing newline at the end",
			body:    "123\n456\ndGlja2V0LXZhbHVl",
			wantErr: true,
		},
		{
			desc:    "invalid pending size",
			body:    "abc\n456\ndGlja2V0\n",
			wantErr: true,
		},
		{
			desc:    "invalid next entry",
			body:    "123\nxyz\ndGlja2V0\n",
			wantErr: true,
		},
		{
			desc:    "invalid base64 ticket",
			body:    "123\n456\nnot-base64-%\n",
			wantErr: true,
		},
		{
			desc:    "too few lines",
			body:    "123\n456\n",
			wantErr: true,
		},
		{
			desc:    "empty body",
			body:    "",
			wantErr: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := parseConflict(bytes.NewBufferString(tc.body))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseConflict() succeeded, want error")
				}
				var ec ErrConflict
				if errors.As(err, &ec) {
					t.Fatalf("parseConflict() returned ErrConflict %v, want parsing error", ec)
				}
				return
			}
			if err == nil {
				t.Fatalf("parseConflict() returned nil, want ErrConflict")
			}
			var ec ErrConflict
			if !errors.As(err, &ec) {
				t.Fatalf("parseConflict() returned error %v, want ErrConflict", err)
			}
			if ec.PendingSize != tc.want.PendingSize {
				t.Errorf("PendingSize = %d, want %d", ec.PendingSize, tc.want.PendingSize)
			}
			if ec.NextEntry != tc.want.NextEntry {
				t.Errorf("NextEntry = %d, want %d", ec.NextEntry, tc.want.NextEntry)
			}
			if !bytes.Equal(ec.Ticket, tc.want.Ticket) {
				t.Errorf("Ticket = %q, want %q", string(ec.Ticket), string(tc.want.Ticket))
			}
		})
	}
}

func TestBuildCheckpointRequestBody(t *testing.T) {
	tests := []struct {
		desc          string
		oldSize       uint64
		proof         [][]byte
		checkpointRaw []byte
		want          string
	}{
		{
			desc:          "empty proof and empty checkpoint",
			oldSize:       0,
			proof:         nil,
			checkpointRaw: nil,
			want:          "old 0\n\n",
		},
		{
			desc:          "empty proof and non-empty checkpoint",
			oldSize:       123,
			proof:         nil,
			checkpointRaw: []byte("some-checkpoint-data"),
			want:          "old 123\n\nsome-checkpoint-data",
		},
		{
			desc:    "single 32-byte proof line",
			oldSize: 5,
			proof: [][]byte{
				bytes.Repeat([]byte{1}, 32),
			},
			checkpointRaw: []byte("checkpoint"),
			want:          "old 5\n" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)) + "\n\ncheckpoint",
		},
		{
			desc:    "multiple 32-byte proof lines",
			oldSize: 10,
			proof: [][]byte{
				bytes.Repeat([]byte{1}, 32),
				bytes.Repeat([]byte{2}, 32),
			},
			checkpointRaw: []byte("checkpoint"),
			want: "old 10\n" +
				base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)) + "\n" +
				base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32)) + "\n\ncheckpoint",
		},
	}

	c := &Client{}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			gotReader := c.buildCheckpointRequestBody(tc.oldSize, tc.proof, tc.checkpointRaw)
			gotBytes, err := io.ReadAll(gotReader)
			if err != nil {
				t.Fatalf("failed to read body: %v", err)
			}
			if got := string(gotBytes); got != tc.want {
				t.Errorf("buildCheckpointRequestBody() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNewOptions tests that NewOptions returns an Options struct with correct default values.
func TestNewOptions(t *testing.T) {
	opts := NewOptions()
	if opts.httpClient != http.DefaultClient {
		t.Errorf("NewOptions().httpClient = %v, want http.DefaultClient", opts.httpClient)
	}
	if opts.mirrorURL != nil {
		t.Errorf("NewOptions().mirrorURL = %v, want nil", opts.mirrorURL)
	}
	if opts.logOrigin != "" {
		t.Errorf("NewOptions().logOrigin = %q, want empty", opts.logOrigin)
	}
	if opts.tileFetcher != nil {
		t.Errorf("NewOptions().tileFetcher = %v, want nil", opts.tileFetcher)
	}
	if opts.bundleFetcher != nil {
		t.Errorf("NewOptions().bundleFetcher = %v, want nil", opts.bundleFetcher)
	}
	if opts.mirrorCheckpointFetcher != nil {
		t.Errorf("NewOptions().mirrorCheckpointFetcher = %v, want nil", opts.mirrorCheckpointFetcher)
	}
	if opts.packageProver != nil {
		t.Errorf("NewOptions().packageProver = %v, want nil", opts.packageProver)
	}
}

// TestOptionsBuilders tests that each of the With... builder methods correctly sets the corresponding field in Options.
func TestOptionsBuilders(t *testing.T) {
	u, err := url.Parse("https://example.com")
	if err != nil {
		t.Fatalf("failed to parse test URL: %v", err)
	}
	httpClient := &http.Client{}
	logOrigin := "test-origin"
	var tileFetcher client.TileFetcherFunc = func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
		return nil, nil
	}
	var bundleFetcher client.EntryBundleFetcherFunc = func(ctx context.Context, index uint64, size uint8) ([]byte, error) {
		return nil, nil
	}
	var mirrorCheckpointFetcher client.CheckpointFetcherFunc = func(ctx context.Context) ([]byte, error) {
		return nil, nil
	}
	var packageProver PackageProverFunc = func(ctx context.Context, start, end uint64) ([][]byte, error) {
		return nil, nil
	}

	opts := NewOptions().
		WithMirrorURL(u).
		WithHTTPClient(httpClient).
		WithLogOrigin(logOrigin).
		WithTileFetcher(tileFetcher).
		WithBundleFetcher(bundleFetcher).
		WithMirrorCheckpointFetcher(mirrorCheckpointFetcher).
		WithPackageProver(packageProver)

	if opts.mirrorURL != u {
		t.Errorf("WithMirrorURL() = %v, want %v", opts.mirrorURL, u)
	}
	if opts.httpClient != httpClient {
		t.Errorf("WithHTTPClient() = %v, want %v", opts.httpClient, httpClient)
	}
	if opts.logOrigin != logOrigin {
		t.Errorf("WithLogOrigin() = %q, want %q", opts.logOrigin, logOrigin)
	}
	if opts.tileFetcher == nil {
		t.Errorf("WithTileFetcher() tileFetcher is nil")
	}
	if opts.bundleFetcher == nil {
		t.Errorf("WithBundleFetcher() bundleFetcher is nil")
	}
	if opts.mirrorCheckpointFetcher == nil {
		t.Errorf("WithMirrorCheckpointFetcher() mirrorCheckpointFetcher is nil")
	}
	if opts.packageProver == nil {
		t.Errorf("WithPackageProver() packageProver is nil")
	}
}

// TestNewClientValidation tests that NewClient validates the provided Options and returns errors when required fields are missing.
func TestNewClientValidation(t *testing.T) {
	u, _ := url.Parse("https://example.com")
	httpClient := &http.Client{}
	logOrigin := "test-origin"
	var tileFetcher client.TileFetcherFunc = func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) { return nil, nil }
	var bundleFetcher client.EntryBundleFetcherFunc = func(ctx context.Context, index uint64, size uint8) ([]byte, error) { return nil, nil }
	var mirrorCheckpointFetcher client.CheckpointFetcherFunc = func(ctx context.Context) ([]byte, error) { return nil, nil }
	var packageProver PackageProverFunc = func(ctx context.Context, start, end uint64) ([][]byte, error) { return nil, nil }

	tests := []struct {
		desc    string
		setup   func() *Options
		wantErr string
	}{
		{
			desc: "valid options",
			setup: func() *Options {
				return NewOptions().
					WithMirrorURL(u).
					WithHTTPClient(httpClient).
					WithLogOrigin(logOrigin).
					WithTileFetcher(tileFetcher).
					WithBundleFetcher(bundleFetcher).
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher).
					WithPackageProver(packageProver)
			},
		},
		{
			desc: "missing mirror URL",
			setup: func() *Options {
				return NewOptions().
					WithHTTPClient(httpClient).
					WithLogOrigin(logOrigin).
					WithTileFetcher(tileFetcher).
					WithBundleFetcher(bundleFetcher).
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher).
					WithPackageProver(packageProver)
			},
			wantErr: "mirror URL is required",
		},
		{
			desc: "missing HTTP client",
			setup: func() *Options {
				opts := NewOptions().
					WithMirrorURL(u).
					WithLogOrigin(logOrigin).
					WithTileFetcher(tileFetcher).
					WithBundleFetcher(bundleFetcher).
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher).
					WithPackageProver(packageProver)
				opts.httpClient = nil
				return opts
			},
			wantErr: "HTTP client is required",
		},
		{
			desc: "missing log origin",
			setup: func() *Options {
				return NewOptions().
					WithMirrorURL(u).
					WithHTTPClient(httpClient).
					WithTileFetcher(tileFetcher).
					WithBundleFetcher(bundleFetcher).
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher).
					WithPackageProver(packageProver)
			},
			wantErr: "log origin is required",
		},
		{
			desc: "missing tile fetcher",
			setup: func() *Options {
				return NewOptions().
					WithMirrorURL(u).
					WithHTTPClient(httpClient).
					WithLogOrigin(logOrigin).
					WithBundleFetcher(bundleFetcher).
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher).
					WithPackageProver(packageProver)
			},
			wantErr: "tile fetcher is required",
		},
		{
			desc: "missing bundle fetcher",
			setup: func() *Options {
				return NewOptions().
					WithMirrorURL(u).
					WithHTTPClient(httpClient).
					WithLogOrigin(logOrigin).
					WithTileFetcher(tileFetcher).
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher).
					WithPackageProver(packageProver)
			},
			wantErr: "bundle fetcher is required",
		},
		{
			desc: "missing mirror checkpoint fetcher",
			setup: func() *Options {
				return NewOptions().
					WithMirrorURL(u).
					WithHTTPClient(httpClient).
					WithLogOrigin(logOrigin).
					WithTileFetcher(tileFetcher).
					WithBundleFetcher(bundleFetcher).
					WithPackageProver(packageProver)
			},
			wantErr: "mirror checkpoint fetcher is required",
		},
		{
			desc: "missing package prover",
			setup: func() *Options {
				return NewOptions().
					WithMirrorURL(u).
					WithHTTPClient(httpClient).
					WithLogOrigin(logOrigin).
					WithTileFetcher(tileFetcher).
					WithBundleFetcher(bundleFetcher).
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher)
			},
			wantErr: "package prover is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			opts := tc.setup()
			ctx := context.Background()
			client, err := NewClient(ctx, opts)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("NewClient() succeeded, want error containing %q", tc.wantErr)
				}
				if got, want := err.Error(), tc.wantErr; got != want {
					t.Errorf("NewClient() err = %q, want %q", got, want)
				}
				if client != nil {
					t.Errorf("NewClient() returned non-nil client on error: %v", client)
				}
			} else {
				if err != nil {
					t.Fatalf("NewClient() failed: %v", err)
				}
				if client == nil {
					t.Errorf("NewClient() returned nil client on success")
				}
			}
		})
	}
}
