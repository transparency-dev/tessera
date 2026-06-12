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
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

// fakeMirror is a mock implementation of a tlog-mirror compliant server.
type fakeMirror struct {
	t                   *testing.T
	origin              string
	ticket              []byte
	targetCheckpoint    []byte
	expectedCosigs      []byte
	numEntries          int
	proofHashes         [][]byte
	addEntriesStatus    int
	addCheckpointStatus int
}

// newFakeMirror returns a fakeMirror initialized with default mock options for testing.
func newFakeMirror(t *testing.T, origin string) *fakeMirror {
	return &fakeMirror{
		t:                   t,
		origin:              origin,
		ticket:              []byte("ticket-value"),
		targetCheckpoint:    []byte("checkpoint-raw-bytes"),
		expectedCosigs:      []byte("mock-cosignatures"),
		numEntries:          5,
		proofHashes:         [][]byte{bytes.Repeat([]byte{1}, 32)},
		addEntriesStatus:    http.StatusOK,
		addCheckpointStatus: http.StatusOK,
	}
}

// ServeHTTP handles incoming HTTP requests to mock mirror endpoints (/add-entries, /add-checkpoint).
func (fm *fakeMirror) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/add-entries":
		if r.Method != http.MethodPost {
			fm.t.Errorf("Method = %q, want %q", r.Method, http.MethodPost)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		gr, err := gzip.NewReader(r.Body)
		if err != nil {
			fm.t.Errorf("gzip.NewReader failed: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer func() {
			_ = gr.Close()
		}()

		var originLen uint16
		if err := binary.Read(gr, binary.BigEndian, &originLen); err != nil {
			fm.t.Errorf("failed to read origin length: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotOrigin := make([]byte, originLen)
		if _, err := io.ReadFull(gr, gotOrigin); err != nil {
			fm.t.Errorf("failed to read origin: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if string(gotOrigin) != fm.origin {
			fm.t.Errorf("origin = %q, want %q", string(gotOrigin), fm.origin)
		}

		var start uint64
		if err := binary.Read(gr, binary.BigEndian, &start); err != nil {
			fm.t.Errorf("failed to read start: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var end uint64
		if err := binary.Read(gr, binary.BigEndian, &end); err != nil {
			fm.t.Errorf("failed to read end: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var gotTicketLen uint16
		if err := binary.Read(gr, binary.BigEndian, &gotTicketLen); err != nil {
			fm.t.Errorf("failed to read ticket length: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotTicket := make([]byte, gotTicketLen)
		if _, err := io.ReadFull(gr, gotTicket); err != nil {
			fm.t.Errorf("failed to read ticket: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if start == 0 && end == 0 {
			w.WriteHeader(http.StatusConflict)
			ticketB64 := base64.StdEncoding.EncodeToString(fm.ticket)
			_, _ = fmt.Fprintf(w, "0\n0\n%s\n", ticketB64)
			return
		}

		if start != 0 || end != uint64(fm.numEntries) {
			fm.t.Errorf("got start/end = %d/%d, want 0/%d", start, end, fm.numEntries)
		}
		if !bytes.Equal(gotTicket, fm.ticket) {
			fm.t.Errorf("got ticket = %q, want %q", string(gotTicket), string(fm.ticket))
		}

		for i := range fm.numEntries {
			var entryLen uint16
			if err := binary.Read(gr, binary.BigEndian, &entryLen); err != nil {
				fm.t.Errorf("failed to read entry %d length: %v", i, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			entry := make([]byte, entryLen)
			if _, err := io.ReadFull(gr, entry); err != nil {
				fm.t.Errorf("failed to read entry %d: %v", i, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			expectedEntry := []byte(fmt.Sprintf("entry-%d", i))
			if !bytes.Equal(entry, expectedEntry) {
				fm.t.Errorf("entry %d = %q, want %q", i, string(entry), string(expectedEntry))
			}
		}

		var numHashes uint8
		if err := binary.Read(gr, binary.BigEndian, &numHashes); err != nil {
			fm.t.Errorf("failed to read numHashes: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if int(numHashes) != len(fm.proofHashes) {
			fm.t.Errorf("numHashes = %d, want %d", numHashes, len(fm.proofHashes))
		}
		for i, expectedHash := range fm.proofHashes {
			gotHash := make([]byte, 32)
			if _, err := io.ReadFull(gr, gotHash); err != nil {
				fm.t.Errorf("failed to read hash %d: %v", i, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if !bytes.Equal(gotHash, expectedHash) {
				fm.t.Errorf("hash %d = %x, want %x", i, gotHash, expectedHash)
			}
		}

		if fm.addEntriesStatus != http.StatusOK {
			w.WriteHeader(fm.addEntriesStatus)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fm.expectedCosigs)

	case "/add-checkpoint":
		if r.Method != http.MethodPost {
			fm.t.Errorf("Method = %q, want %q", r.Method, http.MethodPost)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			fm.t.Errorf("failed to read body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		expectedBody := fmt.Sprintf("old 0\n\n%s", string(fm.targetCheckpoint))
		if string(bodyBytes) != expectedBody {
			fm.t.Errorf("body = %q, want %q", string(bodyBytes), expectedBody)
		}
		if fm.addCheckpointStatus != http.StatusOK {
			w.WriteHeader(fm.addCheckpointStatus)
			return
		}
		w.WriteHeader(http.StatusOK)

	default:
		fm.t.Errorf("Unexpected path: %q", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

// TestSync runs the happy path synchronization flow, verifying client options validation,
// checkpoint updating, and entry streaming.
func TestSync(t *testing.T) {
	origin := "test-origin"
	fm := newFakeMirror(t, origin)
	server := httptest.NewServer(fm)
	defer server.Close()

	u, err := url.Parse(server.URL + "/")
	if err != nil {
		t.Fatalf("failed to parse server URL: %v", err)
	}

	tileFetcher := func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
		return nil, errors.New("tile fetcher should not be called")
	}

	bundleFetcher := func(ctx context.Context, bundleIndex uint64, p uint8) ([]byte, error) {
		var buf bytes.Buffer
		for i := range fm.numEntries {
			entry := fmt.Appendf(nil, "entry-%d", i)
			_ = binary.Write(&buf, binary.BigEndian, uint16(len(entry)))
			buf.Write(entry)
		}
		return buf.Bytes(), nil
	}

	mirrorCheckpointFetcher := func(ctx context.Context) ([]byte, error) {
		return nil, errors.New("mirror checkpoint fetcher should not be called")
	}

	packageProver := func(ctx context.Context, start, end uint64) ([][]byte, error) {
		return fm.proofHashes, nil
	}

	opts := NewOptions().
		WithMirrorURL(u).
		WithLogOrigin(origin).
		WithTileFetcher(tileFetcher).
		WithBundleFetcher(bundleFetcher).
		WithMirrorCheckpointFetcher(mirrorCheckpointFetcher).
		WithPackageProver(packageProver)

	c, err := NewClient(context.Background(), opts)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}

	cosigs, err := c.Sync(context.Background(), fm.targetCheckpoint, uint64(fm.numEntries))
	if err != nil {
		t.Fatalf("Sync() failed: %v", err)
	}

	if !bytes.Equal(cosigs, fm.expectedCosigs) {
		t.Errorf("Sync() returned cosigs = %q, want %q", string(cosigs), string(fm.expectedCosigs))
	}
}
