// Copyright 2025 The Tessera authors. All Rights Reserved.
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

package tessera

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/witness/config"
	"github.com/transparency-dev/witness/persistence/inmemory"
	"github.com/transparency-dev/witness/witness"
	"golang.org/x/mod/sumdb/note"
)

func TestMemoize(t *testing.T) {
	// Set up an AddFn which will increment a counter every time it's called, and return that in the Index.
	i := uint64(0)
	deleg := func() (Index, error) {
		i++
		return Index{
			Index: i,
		}, nil
	}
	add := func(_ context.Context, _ *Entry) IndexFuture {
		return deleg
	}

	// Create a single future (for a single Entry), and convince ourselves that the counter is being incremented
	// each time the future is being invoked.
	f1 := add(nil, nil)
	a, _ := f1()
	b, _ := f1()
	if a.Index == b.Index {
		t.Fatalf("a(=%d) == b(=%d)", a.Index, b.Index)
	}

	// Now create an AddFn which memoizes the result of the delegate, like we do in NewAppender, and assert that
	// repeated calls to the future work as expected; only incrementing the counter once.
	add = func(_ context.Context, _ *Entry) IndexFuture {
		return memoizeFuture(deleg)
	}
	f2 := add(nil, nil)
	c, _ := f2()
	d, _ := f2()

	if c.Index != d.Index {
		t.Fatalf("c(=%d) != d(=%d)", c.Index, d.Index)
	}
}

const testSignerKey = "PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"

func TestAppendOptionsValid(t *testing.T) {
	for _, test := range []struct {
		name            string
		opts            *AppendOptions
		wantErrContains string
	}{
		{
			name: "Valid",
			opts: NewAppendOptions().WithCheckpointSigner(mustCreateSigner(t, testSignerKey)),
		}, {
			name: "Valid: CheckpointRepublishInterval == CheckpointInterval",
			opts: NewAppendOptions().
				WithCheckpointSigner(mustCreateSigner(t, testSignerKey)).
				WithCheckpointInterval(10 * time.Second).
				WithCheckpointRepublishInterval(10 * time.Second),
		}, {
			name: "Error: CheckpointRepublishInterval < CheckpointInterval",
			opts: NewAppendOptions().
				WithCheckpointSigner(mustCreateSigner(t, testSignerKey)).
				WithCheckpointInterval(10 * time.Second).
				WithCheckpointRepublishInterval(9 * time.Second),
			wantErrContains: "WithCheckpointRepublishInterval",
		}, {
			name:            "Error: No CheckpointSigner",
			opts:            NewAppendOptions(),
			wantErrContains: "WithCheckpointSigner",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.opts.valid()
			switch gotErr, wantErr := err != nil, test.wantErrContains != ""; {
			case gotErr && !wantErr:
				t.Fatalf("Got unexpected error %q, want no error", err)
			case !gotErr && wantErr:
				t.Fatalf("Got no error, expected error")
			case gotErr:
				if !strings.Contains(err.Error(), test.wantErrContains) {
					t.Fatalf("Got err %q, want error containing %q", err.Error(), test.wantErrContains)
				}
			}
		})
	}
}

func TestMaxEntrySize(t *testing.T) {
	d := func(_ context.Context, e *Entry) IndexFuture {
		return func() (Index, error) {
			return Index{}, nil
		}
	}

	const limit = 128
	add := entrySizeLimitDecorator(d, limit)

	for _, test := range []struct {
		name    string
		size    uint
		wantErr bool
	}{
		{
			name: "< limit",
			size: limit - 1,
		}, {
			name: "== limit",
			size: limit,
		}, {
			name:    "> limit",
			size:    limit + 1,
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := add(t.Context(), NewEntry(make([]byte, test.size)))()
			if gotErr := err != nil; gotErr != test.wantErr {
				t.Fatalf("Got err %q, want err? %T", err, test.wantErr)
			}
		})
	}
}

func mustCreateSigner(t *testing.T, k string) note.Signer {
	t.Helper()
	s, err := note.NewSigner(k)
	if err != nil {
		t.Fatalf("Failed to create signer: %v", err)
	}
	return s
}

func TestShutdownBehavior(t *testing.T) {
	tests := []struct {
		name         string
		wantTreeSize uint64
		cpSize       uint64
		expectWait   bool
	}{
		{
			name:         "no work done",
			wantTreeSize: 0,
			expectWait:   false,
		},
		{
			name:         "wait for index 0",
			wantTreeSize: 1,
			cpSize:       0,
			expectWait:   true,
		},
		{
			name:         "already caught up",
			wantTreeSize: 1,
			cpSize:       1,
			expectWait:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			term := &terminator{
				readCheckpoint: func(ctx context.Context) ([]byte, error) {
					// Return a valid checkpoint string that parse.CheckpointUnsafe can parse.
					return fmt.Appendf(nil, "example.com\n%d\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n", test.cpSize), nil
				},
				shutdownTimeout: 10 * time.Millisecond,
			}
			term.wantTreeSize.Store(test.wantTreeSize)

			// If we've added an entry, then the terminator should wait for a checkpoint covering it.
			// Since we don't provide any checkpoints, we can detect this by waiting for it to timeout.
			err := term.Shutdown(t.Context())
			if gotTimeout := errors.Is(err, context.DeadlineExceeded); gotTimeout != test.expectWait {
				t.Fatalf("Expected timeout error from waiting for checkpoint to catch up: %v, got timeout: %v, err: %v", test.expectWait, gotTimeout, err)
			}
		})
	}
}

func TestAddUpdatesWantTreeSize(t *testing.T) {
	wantIdx := uint64(5)
	term := &terminator{
		delegate: func(_ context.Context, _ *Entry) IndexFuture {
			return func() (Index, error) {
				return Index{Index: wantIdx}, nil
			}
		},
	}

	f := term.Add(t.Context(), nil)
	if _, err := f(); err != nil {
		t.Fatal(err)
	}

	if got := term.wantTreeSize.Load(); got != wantIdx+1 {
		t.Fatalf("wantTreeSize should be %d after adding index %d, got %d", wantIdx+1, wantIdx, got)
	}
}

func TestWithMirrors(t *testing.T) {
	u, err := url.Parse("https://mirror.example.com")
	if err != nil {
		t.Fatalf("failed to parse url: %v", err)
	}
	wit, err := NewWitness("Wit1+55ee4561+AVhZSmQj9+SoL+p/nN0Hh76xXmF7QcHfytUrI1XfSClk", u)
	if err != nil {
		t.Fatalf("failed to create witness: %v", err)
	}
	mirrors := NewWitnessGroup(1, wit)

	for _, test := range []struct {
		desc           string
		mirrorOpts     *MirroringOptions
		expectTimeout  time.Duration
		expectFailOpen bool
	}{
		{
			desc:           "nil options",
			mirrorOpts:     nil,
			expectTimeout:  DefaultMirrorTimeout,
			expectFailOpen: false,
		},
		{
			desc: "custom options",
			mirrorOpts: &MirroringOptions{
				Timeout:  5 * time.Second,
				FailOpen: true,
			},
			expectTimeout:  5 * time.Second,
			expectFailOpen: true,
		},
		{
			desc: "zero timeout uses default",
			mirrorOpts: &MirroringOptions{
				Timeout:  0,
				FailOpen: true,
			},
			expectTimeout:  DefaultMirrorTimeout,
			expectFailOpen: true,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			opts := NewAppendOptions().WithMirrors(mirrors, test.mirrorOpts)
			if len(opts.mirrors.Components) != 1 {
				t.Errorf("expected 1 mirror component, got %d", len(opts.mirrors.Components))
			}
			if got, want := opts.mirrorOpts.Timeout, test.expectTimeout; got != want {
				t.Errorf("expected timeout %v, got %v", want, got)
			}
			if got, want := opts.mirrorOpts.FailOpen, test.expectFailOpen; got != want {
				t.Errorf("expected FailOpen %t, got %t", want, got)
			}
		})
	}
}

const (
	testWit1VKey = "Wit1+55ee4561+AVhZSmQj9+SoL+p/nN0Hh76xXmF7QcHfytUrI1XfSClk"
	testWit1SKey = "PRIVATE+KEY+Wit1+55ee4561+AeadRiG7XM4XiieCHzD8lxysXMwcViy5nYsoXURWGrlE"
)

func newWitnessHandler(t *testing.T, logVerifier note.Verifier, witnessSKey string) http.HandlerFunc {
	witnessSigner, err := f_note.NewSignerForCosignatureV1(witnessSKey)
	if err != nil {
		t.Fatalf("failed to create witness signer: %v", err)
	}

	p := inmemory.New()
	logCfg := config.Log{
		Origin:   "example.com/log/testdata",
		Verifier: logVerifier,
		VKey:     "example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx",
	}
	if err := p.AddLogs(t.Context(), []config.Log{logCfg}); err != nil {
		t.Fatalf("failed to add log config to persistence: %v", err)
	}

	wOpts := witness.Opts{
		Persistence: p,
		Signers:     []note.Signer{witnessSigner},
		VerifierForLog: func(ctx context.Context, origin string) (note.Verifier, bool, error) {
			if origin == "example.com/log/testdata" {
				return logVerifier, true, nil
			}
			return nil, false, nil
		},
	}
	witSvc, err := witness.New(t.Context(), wOpts)
	if err != nil {
		t.Fatalf("failed to create witness service: %v", err)
	}

	return witness.NewHTTPHandler(witSvc).AddCheckpoint
}

func TestCheckpointPublisher(t *testing.T) {
	logSigner := mustCreateSigner(t, testSignerKey)
	logVerifier, err := note.NewVerifier("example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx")
	if err != nil {
		t.Fatalf("failed to create log verifier: %v", err)
	}

	witnessServer := httptest.NewServer(newWitnessHandler(t, logVerifier, testWit1SKey))
	t.Cleanup(witnessServer.Close)

	witnessServerURL, err := url.Parse(witnessServer.URL)
	if err != nil {
		t.Fatalf("failed to parse witness server url: %v", err)
	}

	wit, err := NewWitness(testWit1VKey, witnessServerURL)
	if err != nil {
		t.Fatalf("failed to create witness: %v", err)
	}
	witnesses := NewWitnessGroup(1, wit)
	witVerifier, err := f_note.NewVerifierForCosignatureV1(testWit1VKey)
	if err != nil {
		t.Fatalf("failed to create witness verifier: %v", err)
	}

	dummyMirrors := NewWitnessGroup(1, wit)

	for _, test := range []struct {
		desc               string
		opts               *AppendOptions
		witnessFails       bool
		expectCosignatures []note.Verifier
		expectErr          bool
	}{
		{
			desc: "no witnesses, no mirrors",
			opts: NewAppendOptions().WithCheckpointSigner(logSigner),
		},
		{
			desc:               "witnesses only",
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(witnesses, nil),
			expectCosignatures: []note.Verifier{witVerifier},
		},
		{
			desc: "mirrors only",
			opts: NewAppendOptions().WithCheckpointSigner(logSigner).WithMirrors(dummyMirrors, nil),
		},
		{
			desc:               "witnesses and mirrors",
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(witnesses, nil).WithMirrors(dummyMirrors, nil),
			expectCosignatures: []note.Verifier{witVerifier},
		},
		{
			desc:         "witness fails, failOpen=false",
			opts:         NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(witnesses, &WitnessOptions{FailOpen: false}),
			witnessFails: true,
			expectErr:    true,
		},
		{
			desc:         "witness fails, failOpen=true",
			opts:         NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(witnesses, &WitnessOptions{FailOpen: true}),
			witnessFails: true,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			client := http.DefaultClient
			if test.witnessFails {
				failingWitnessServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "internal error", http.StatusInternalServerError)
				}))
				defer failingWitnessServer.Close()

				failingURL, _ := url.Parse(failingWitnessServer.URL)
				failingWit, _ := NewWitness(testWit1VKey, failingURL)
				failingWitnesses := NewWitnessGroup(1, failingWit)

				// Re-configure option to use failing witnesses
				wOpts := &WitnessOptions{FailOpen: test.opts.witnessOpts.FailOpen}
				test.opts.WithWitnesses(failingWitnesses, wOpts)
			}

			lr := &fakeLogReader{
				readCheckpoint: func(ctx context.Context) ([]byte, error) {
					return nil, errors.New("no checkpoint yet")
				},
			}

			publisher := test.opts.CheckpointPublisher(lr, client)
			cp, err := publisher(t.Context(), 5, []byte("12345678901234567890123456789012"))
			if (err != nil) != test.expectErr {
				t.Fatalf("expected error %v but got: %v", test.expectErr, err)
			}
			if err != nil {
				return
			}

			// Open checkpoint to verify signatures
			wantV := append([]note.Verifier{logVerifier}, test.expectCosignatures...)
			n, err := note.Open(cp, note.VerifierList(wantV...))
			if err != nil {
				t.Fatalf("failed to open signed checkpoint: %v", err)
			}

			// Check that all required verifiers signed it
			if len(n.Sigs) != len(wantV) {
				t.Logf("cp = %q", string(cp))
				t.Logf("n.Sigs = %+v", n.Sigs)
				t.Errorf("expected %d signatures, got %d", len(wantV), len(n.Sigs))
			}
		})
	}
}
