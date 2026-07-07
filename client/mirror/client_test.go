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
	var packageProver PackageProverFunc = func(ctx context.Context, start, end, size uint64) ([][]byte, error) {
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
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher)
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
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher)
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
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher)
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
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher)
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
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher)
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
					WithMirrorCheckpointFetcher(mirrorCheckpointFetcher)
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
					WithBundleFetcher(bundleFetcher)
			},
			wantErr: "mirror checkpoint fetcher is required",
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
	t                      *testing.T
	origin                 string
	ticket                 []byte
	targetCheckpoint       []byte
	expectedCosigs         []byte
	numEntries             int
	proofHashes            [][]byte
	addEntriesStatus       int
	addCheckpointStatus    int
	initialPendingSize     uint64
	initialNextEntry       uint64
	initialStatus          int
	addEntriesExpectations []addEntriesExpectation
	addEntriesCallCount    int
}

type addEntriesExpectation struct {
	start  uint64
	end    uint64
	ticket []byte
	status int
	body   string // used if status == StatusConflict
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
			status := http.StatusConflict
			if fm.initialStatus != 0 {
				status = fm.initialStatus
			}
			if status == http.StatusConflict || status == http.StatusAccepted {
				w.Header().Set("Content-Type", "text/x.tlog.mirror-info")
			}
			w.WriteHeader(status)
			switch status {
			case http.StatusConflict:
				ticketB64 := base64.StdEncoding.EncodeToString(fm.ticket)
				_, _ = fmt.Fprintf(w, "%d\n%d\n%s\n", fm.initialPendingSize, fm.initialNextEntry, ticketB64)
			case http.StatusBadRequest:
				_, _ = w.Write([]byte("no pending checkpoint"))
			}
			return
		}

		var exp addEntriesExpectation
		if len(fm.addEntriesExpectations) > 0 {
			idx := fm.addEntriesCallCount
			if idx >= len(fm.addEntriesExpectations) {
				idx = len(fm.addEntriesExpectations) - 1
			}
			exp = fm.addEntriesExpectations[idx]
			fm.addEntriesCallCount++
		} else {
			exp = addEntriesExpectation{
				start:  0,
				end:    uint64(fm.numEntries),
				ticket: fm.ticket,
				status: fm.addEntriesStatus,
			}
		}

		if start != exp.start || end != exp.end {
			fm.t.Errorf("got start/end = %d/%d, want %d/%d", start, end, exp.start, exp.end)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !bytes.Equal(gotTicket, exp.ticket) {
			fm.t.Errorf("got ticket = %q, want %q", string(gotTicket), string(exp.ticket))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		numExpectedEntries := int(end - start)
		for i := range numExpectedEntries {
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
			expectedEntry := fmt.Appendf(nil, "entry-%d", i+int(start))
			if !bytes.Equal(entry, expectedEntry) {
				fm.t.Errorf("entry %d = %q, want %q", i, string(entry), string(expectedEntry))
				w.WriteHeader(http.StatusBadRequest)
				return
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
			w.WriteHeader(http.StatusBadRequest)
			return
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
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		// Consume the rest of the gzipped body (including the gzip footer) to prevent client write hangs.
		_, _ = io.Copy(io.Discard, gr)

		if exp.status != http.StatusOK {
			if exp.status == http.StatusConflict || exp.status == http.StatusAccepted {
				w.Header().Set("Content-Type", "text/x.tlog.mirror-info")
			}
			w.WriteHeader(exp.status)
			if exp.status == http.StatusConflict || exp.status == http.StatusAccepted {
				_, _ = w.Write([]byte(exp.body))
			}
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
		lines := bytes.SplitN(bodyBytes, []byte("\n\n"), 2)
		if len(lines) != 2 {
			fm.t.Errorf("invalid checkpoint request body format")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		header := lines[0]
		bodyCheckpoint := lines[1]

		headerLines := bytes.Split(header, []byte("\n"))
		if len(headerLines) == 0 {
			fm.t.Errorf("empty header in checkpoint request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var oldSize uint64
		if _, err := fmt.Sscanf(string(headerLines[0]), "old %d", &oldSize); err != nil {
			fm.t.Errorf("invalid old size header: %q", string(headerLines[0]))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if oldSize != fm.initialPendingSize {
			fm.t.Errorf("oldSize = %d, want %d", oldSize, fm.initialPendingSize)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if oldSize > 0 && len(headerLines) <= 1 {
			fm.t.Errorf("expected consistency proof lines in header")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if !bytes.Equal(bodyCheckpoint, fm.targetCheckpoint) {
			fm.t.Errorf("body checkpoint = %q, want %q", string(bodyCheckpoint), string(fm.targetCheckpoint))
			w.WriteHeader(http.StatusBadRequest)
			return
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

	packageProver := func(ctx context.Context, start, end, size uint64) ([][]byte, error) {
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
		t.Fatalf("NewClient() = _, %v, want _, nil", err)
	}

	cosigs, err := c.Sync(context.Background(), fm.targetCheckpoint, uint64(fm.numEntries))
	if err != nil {
		t.Fatalf("Sync() = _, %v, want _, nil", err)
	}

	if !bytes.Equal(cosigs, fm.expectedCosigs) {
		t.Errorf("Sync() = %q, want %q", string(cosigs), string(fm.expectedCosigs))
	}
}

func TestSync_ErrorsAndEdgeCases(t *testing.T) {
	origin := "test-origin"

	tests := []struct {
		desc                   string
		initialPendingSize     uint64
		initialNextEntry       uint64
		initialStatus          int
		addEntriesExpectations []addEntriesExpectation
		addCheckpointStatus    int
		tileFetcher            client.TileFetcherFunc
		wantErr                string
	}{
		{
			desc:               "initial status query returns unexpected success",
			initialPendingSize: 0,
			initialNextEntry:   0,
			initialStatus:      http.StatusOK,
			wantErr:            "unexpected success when querying mirror status with size 0",
		},
		{
			desc:               "initial status query returns generic error",
			initialPendingSize: 0,
			initialNextEntry:   0,
			initialStatus:      http.StatusInternalServerError,
			wantErr:            "failed to retrieve mirror status",
		},
		{
			desc:               "consistency proof fails",
			initialPendingSize: 1,
			initialNextEntry:   1,
			tileFetcher: func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
				return nil, errors.New("tile fetcher error")
			},
			wantErr: "failed to generate consistency proof",
		},
		{
			desc:               "consistency proof succeeds and checkpoint push succeeds",
			initialPendingSize: 1,
			initialNextEntry:   1,
			tileFetcher: func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
				if p == 5 {
					return make([]byte, 5*32), nil
				}
				return nil, fmt.Errorf("unexpected tile request: level=%d, index=%d, p=%d", level, index, p)
			},
			addEntriesExpectations: []addEntriesExpectation{
				{
					start:  1,
					end:    5,
					ticket: []byte("ticket-value"),
					status: http.StatusOK,
				},
			},
		},
		{
			desc:                "pushing new checkpoint fails",
			initialPendingSize:  1,
			initialNextEntry:    1,
			addCheckpointStatus: http.StatusInternalServerError,
			tileFetcher: func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
				if p == 5 {
					return make([]byte, 5*32), nil
				}
				return nil, fmt.Errorf("unexpected tile request: level=%d, index=%d, p=%d", level, index, p)
			},
			wantErr: "failed to push new checkpoint",
		},
		{
			desc: "entry upload fails with generic error",
			addEntriesExpectations: []addEntriesExpectation{
				{
					start:  0,
					end:    5,
					ticket: []byte("ticket-value"),
					status: http.StatusInternalServerError,
				},
			},
			wantErr: "sync failed during entry upload",
		},
		{
			desc:               "entry upload encounters conflict and retries successfully",
			initialPendingSize: 0,
			initialNextEntry:   0,
			addEntriesExpectations: []addEntriesExpectation{
				{
					start:  0,
					end:    5,
					ticket: []byte("ticket-value"),
					status: http.StatusConflict,
					body:   "5\n2\ndGlja2V0LW5ldw==\n", // pending size = 5, next entry = 2, ticket = "ticket-new"
				},
				{
					start:  2,
					end:    5,
					ticket: []byte("ticket-new"),
					status: http.StatusOK,
				},
			},
		},
		{
			desc:               "initial status query returns no pending checkpoint (bootstrap)",
			initialPendingSize: 0,
			initialNextEntry:   0,
			initialStatus:      http.StatusBadRequest,
			addEntriesExpectations: []addEntriesExpectation{
				{
					start:  0,
					end:    5,
					ticket: nil,
					status: http.StatusOK,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			fm := newFakeMirror(t, origin)
			fm.initialPendingSize = tc.initialPendingSize
			fm.initialNextEntry = tc.initialNextEntry
			fm.initialStatus = tc.initialStatus
			fm.addEntriesExpectations = tc.addEntriesExpectations
			if tc.addCheckpointStatus != 0 {
				fm.addCheckpointStatus = tc.addCheckpointStatus
			}

			server := httptest.NewServer(fm)
			defer server.Close()

			u, err := url.Parse(server.URL + "/")
			if err != nil {
				t.Fatalf("failed to parse server URL: %v", err)
			}

			tf := tc.tileFetcher
			if tf == nil {
				tf = func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
					return nil, errors.New("tile fetcher should not be called")
				}
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

			packageProver := func(ctx context.Context, start, end, size uint64) ([][]byte, error) {
				return fm.proofHashes, nil
			}

			opts := NewOptions().
				WithMirrorURL(u).
				WithLogOrigin(origin).
				WithTileFetcher(tf).
				WithBundleFetcher(bundleFetcher).
				WithMirrorCheckpointFetcher(mirrorCheckpointFetcher).
				WithPackageProver(packageProver)

			c, err := NewClient(context.Background(), opts)
			if err != nil {
				t.Fatalf("NewClient() = _, %v, want _, nil", err)
			}

			cosigs, err := c.Sync(context.Background(), fm.targetCheckpoint, uint64(fm.numEntries))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("Sync() = _, nil, want error containing %q", tc.wantErr)
				}
				if !bytes.Contains([]byte(err.Error()), []byte(tc.wantErr)) {
					t.Errorf("Sync() = _, %v, want error containing %q", err, tc.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("Sync() = _, %v, want _, nil", err)
			}

			if !bytes.Equal(cosigs, fm.expectedCosigs) {
				t.Errorf("Sync() = %q, want %q", string(cosigs), string(fm.expectedCosigs))
			}
		})
	}
}
