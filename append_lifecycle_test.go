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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle/rfc6962"
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

func TestWithWitnesses(t *testing.T) {
	wit := mustNewWitness(t, testWit1VKey, "https://witness.example.com")
	witnesses := NewWitnessGroup(1, wit)

	for _, test := range []struct {
		desc           string
		witnessOpts    *WitnessOptions
		expectTimeout  time.Duration
		expectFailOpen bool
		expectGreedy   bool
	}{
		{
			desc:           "nil options",
			witnessOpts:    nil,
			expectTimeout:  DefaultWitnessTimeout,
			expectFailOpen: false,
			expectGreedy:   false,
		},
		{
			desc: "custom options",
			witnessOpts: &WitnessOptions{
				Timeout:  5 * time.Second,
				FailOpen: true,
				Greedy:   true,
			},
			expectTimeout:  5 * time.Second,
			expectFailOpen: true,
			expectGreedy:   true,
		},
		{
			desc: "zero timeout uses default",
			witnessOpts: &WitnessOptions{
				Timeout:  0,
				FailOpen: true,
				Greedy:   true,
			},
			expectTimeout:  DefaultWitnessTimeout,
			expectFailOpen: true,
			expectGreedy:   true,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			opts := NewAppendOptions().WithWitnesses(witnesses, test.witnessOpts)
			if len(opts.witnesses.Components) != 1 {
				t.Errorf("expected 1 witness component, got %d", len(opts.witnesses.Components))
			}
			if got, want := opts.witnessOpts.Timeout, test.expectTimeout; got != want {
				t.Errorf("expected timeout %v, got %v", want, got)
			}
			if got, want := opts.witnessOpts.FailOpen, test.expectFailOpen; got != want {
				t.Errorf("expected FailOpen %t, got %t", want, got)
			}
			if got, want := opts.witnessOpts.Greedy, test.expectGreedy; got != want {
				t.Errorf("expected Greedy %t, got %t", want, got)
			}
		})
	}
}

const (
	testWit1VKey = "Wit1+55ee4561+AVhZSmQj9+SoL+p/nN0Hh76xXmF7QcHfytUrI1XfSClk"
	testWit1SKey = "PRIVATE+KEY+Wit1+55ee4561+AeadRiG7XM4XiieCHzD8lxysXMwcViy5nYsoXURWGrlE"
	testWit2VKey = "Wit2+85ecc407+AWVbwFJte9wMQIPSnEnj4KibeO6vSIOEDUTDp3o63c2x"
	testWit2SKey = "PRIVATE+KEY+Wit2+85ecc407+AfPTvxw5eUcqSgivo2vaiC7JPOMUZ/9baHPSDrWqgdGm"
	testWit3VKey = "Wit3+d3ed3be7+ASb6Uz1+fxAcXkMvDd7nGa3FjDce7LxIKmbbTCT0MpVn"
	testWit3SKey = "PRIVATE+KEY+Wit3+d3ed3be7+AR2Kg8k6ccBr5QXz5SHtnkOS4UGQGEQaWi6Gfr6Mm3X5"

	testMirrorVKey = "Mirror1+66ee4561+AVhZSmQj9+SoL+p/nN0Hh76xXmF7QcHfytUrI1XfSClk"
	testMirrorSKey = "PRIVATE+KEY+Mirror1+66ee4561+AeadRiG7XM4XiieCHzD8lxysXMwcViy5nYsoXURWGrlE"
)

func createCosignature(t *testing.T, baseNote *note.Note, witnessSKey string) []byte {
	t.Helper()
	witnessSigner, err := f_note.NewSignerForCosignatureV1(witnessSKey)
	if err != nil {
		t.Fatalf("failed to create witness signer: %v", err)
	}
	signedNote, err := note.Sign(baseNote, witnessSigner)
	if err != nil {
		t.Fatalf("failed to sign note: %v", err)
	}
	idx := strings.Index(string(signedNote), "\n— "+witnessSigner.Name()+" ")
	if idx < 0 {
		t.Fatalf("signature line not found in signed note")
	}
	return []byte(string(signedNote)[idx+1:])
}

func TestGatherCosignatures(t *testing.T) {
	logSigner := mustCreateSigner(t, testSignerKey)
	logVerifier, err := note.NewVerifier("example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx")
	if err != nil {
		t.Fatalf("failed to create log verifier: %v", err)
	}

	wit1 := mustNewWitness(t, testWit1VKey, "https://wit1.example.com")
	wit2 := mustNewWitness(t, testWit2VKey, "https://wit2.example.com")
	wit3 := mustNewWitness(t, testWit3VKey, "https://wit3.example.com")
	wit1Verifier, _ := f_note.NewVerifierForCosignatureV1(testWit1VKey)
	wit2Verifier, _ := f_note.NewVerifierForCosignatureV1(testWit2VKey)
	wit3Verifier, _ := f_note.NewVerifierForCosignatureV1(testWit3VKey)

	n := &note.Note{
		Text: "example.com/log/testdata\n5\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n",
	}
	signedCP, err := note.Sign(n, logSigner)
	if err != nil {
		t.Fatal(err)
	}

	sig1 := createCosignature(t, n, testWit1SKey)
	sig2 := createCosignature(t, n, testWit2SKey)
	sig3 := createCosignature(t, n, testWit3SKey)

	for _, test := range []struct {
		desc               string
		policy             WitnessGroup
		fetcher            func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte
		timeout            time.Duration
		failOpen           bool
		greedy             bool
		expectCosignatures []note.Verifier
		expectErr          bool
		expectFailedOpen   bool
	}{
		{
			desc:   "empty policy",
			policy: WitnessGroup{},
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte)
				close(ch)
				return ch
			},
		},
		{
			desc:   "non-greedy stops after quorum is satisfied (1 of 2)",
			policy: NewWitnessGroup(1, wit1, wit2),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 2)
				ch <- sig1
				ch <- sig2
				return ch
			},
			greedy:             false,
			expectCosignatures: []note.Verifier{wit1Verifier},
		},
		{
			desc:   "greedy gathers surplus signatures (2 of 3 required, 3 provided)",
			policy: NewWitnessGroup(2, wit1, wit2, wit3),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 3)
				ch <- sig1
				ch <- sig2
				ch <- sig3
				return ch
			},
			greedy:             true,
			expectCosignatures: []note.Verifier{wit1Verifier, wit2Verifier, wit3Verifier},
		},
		{
			desc:   "greedy succeeds when quorum is met and channel closes without further signatures (1 of 2 required, 1 provided)",
			policy: NewWitnessGroup(1, wit1, wit2),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				ch <- sig1
				close(ch)
				return ch
			},
			greedy:             true,
			expectCosignatures: []note.Verifier{wit1Verifier},
		},
		{
			desc:   "greedy fails when quorum is not met and channel closes (failOpen=false)",
			policy: NewWitnessGroup(2, wit1, wit2),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				ch <- sig1
				close(ch)
				return ch
			},
			greedy:    true,
			failOpen:  false,
			expectErr: true,
		},
		{
			desc:   "greedy fails open when quorum is not met and channel closes (failOpen=true)",
			policy: NewWitnessGroup(2, wit1, wit2),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				ch <- sig1
				close(ch)
				return ch
			},
			greedy:             true,
			failOpen:           true,
			expectFailedOpen:   true,
			expectCosignatures: []note.Verifier{wit1Verifier},
		},
		{
			desc:   "greedy fails when quorum is not met on timeout (failOpen=false)",
			policy: NewWitnessGroup(2, wit1, wit2),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				ch <- sig1
				return ch
			},
			timeout:   50 * time.Millisecond,
			greedy:    true,
			failOpen:  false,
			expectErr: true,
		},
		{
			desc:   "greedy fails open when quorum is not met on timeout (failOpen=true)",
			policy: NewWitnessGroup(2, wit1, wit2),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				ch <- sig1
				return ch
			},
			timeout:            50 * time.Millisecond,
			greedy:             true,
			failOpen:           true,
			expectFailedOpen:   true,
			expectCosignatures: []note.Verifier{wit1Verifier},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			ctx := t.Context()
			if test.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, test.timeout)
				defer cancel()
			}
			sigs, err := gatherCosignatures(ctx, "witness", test.fetcher, &test.policy, signedCP, 5, test.failOpen, test.greedy)
			switch {
			case test.expectFailedOpen:
				if !errors.Is(err, errFailedOpen) {
					t.Fatalf("expected errFailedOpen but got %v", err)
				}
			case test.expectErr:
				if err == nil || errors.Is(err, errFailedOpen) {
					t.Fatalf("expected error but got %v", err)
				}
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			}
			fullCP := append(slices.Clone(signedCP), sigs...)
			verifiers := append([]note.Verifier{logVerifier}, test.expectCosignatures...)
			parsedNote, err := note.Open(fullCP, note.VerifierList(verifiers...))
			if err != nil {
				t.Fatalf("failed to open note: %v", err)
			}
			if len(parsedNote.Sigs) != len(verifiers) {
				t.Errorf("expected %d signatures, got %d", len(verifiers), len(parsedNote.Sigs))
			}
		})
	}
}

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

	witnessServer1 := httptest.NewServer(newWitnessHandler(t, logVerifier, testWit1SKey))
	t.Cleanup(witnessServer1.Close)

	witnessServerURL1, err := url.Parse(witnessServer1.URL)
	if err != nil {
		t.Fatalf("failed to parse witness server 1 url: %v", err)
	}

	wit1, err := NewWitness(testWit1VKey, witnessServerURL1)
	if err != nil {
		t.Fatalf("failed to create witness 1: %v", err)
	}
	witnesses := NewWitnessGroup(1, wit1)
	wit1Verifier, err := f_note.NewVerifierForCosignatureV1(testWit1VKey)
	if err != nil {
		t.Fatalf("failed to create witness 1 verifier: %v", err)
	}

	witnessServer2 := httptest.NewServer(newWitnessHandler(t, logVerifier, testWit2SKey))
	t.Cleanup(witnessServer2.Close)

	witnessServerURL2, err := url.Parse(witnessServer2.URL)
	if err != nil {
		t.Fatalf("failed to parse witness server 2 url: %v", err)
	}

	wit2, err := NewWitness(testWit2VKey, witnessServerURL2)
	if err != nil {
		t.Fatalf("failed to create witness 2: %v", err)
	}
	wit2Verifier, err := f_note.NewVerifierForCosignatureV1(testWit2VKey)
	if err != nil {
		t.Fatalf("failed to create witness 2 verifier: %v", err)
	}

	multiWitnesses := NewWitnessGroup(1, wit1, wit2)

	mirrorServer := httptest.NewServer(newMirrorHandler(t, testMirrorSKey))
	t.Cleanup(mirrorServer.Close)

	mirrorServerURL, err := url.Parse(mirrorServer.URL)
	if err != nil {
		t.Fatalf("failed to parse mirror server url: %v", err)
	}

	m, err := NewWitness(testMirrorVKey, mirrorServerURL)
	if err != nil {
		t.Fatalf("failed to create mirror: %v", err)
	}
	mirrors := NewWitnessGroup(1, m)
	mirrorVerifier, err := f_note.NewVerifierForCosignatureV1(testMirrorVKey)
	if err != nil {
		t.Fatalf("failed to create mirror verifier: %v", err)
	}

	for _, test := range []struct {
		desc                  string
		opts                  *AppendOptions
		witnessFails          bool
		partialWitnessFails   bool
		expectCosignatures    []note.Verifier
		expectNumCosignatures int
		expectErr             bool
	}{
		{
			desc: "no witnesses, no mirrors",
			opts: NewAppendOptions().WithCheckpointSigner(logSigner),
		},
		{
			desc:               "witnesses only",
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(witnesses, &WitnessOptions{Timeout: time.Second}),
			expectCosignatures: []note.Verifier{wit1Verifier},
		},
		{
			desc:               "mirrors only",
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithMirrors(mirrors, &MirroringOptions{Timeout: time.Second}),
			expectCosignatures: []note.Verifier{mirrorVerifier},
		},
		{
			desc:               "witnesses and mirrors",
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(witnesses, &WitnessOptions{Timeout: time.Second}).WithMirrors(mirrors, &MirroringOptions{Timeout: time.Second}),
			expectCosignatures: []note.Verifier{wit1Verifier, mirrorVerifier},
		},
		{
			desc:         "witness fails, failOpen=false",
			opts:         NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(witnesses, &WitnessOptions{FailOpen: false, Timeout: time.Second}),
			witnessFails: true,
			expectErr:    true,
		},
		{
			desc:         "witness fails, failOpen=true",
			opts:         NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(witnesses, &WitnessOptions{FailOpen: true, Timeout: time.Second}),
			witnessFails: true,
		},
		{
			desc:                  "multi witnesses greedy=false",
			opts:                  NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(multiWitnesses, &WitnessOptions{Timeout: time.Second, Greedy: false}),
			expectNumCosignatures: 1,
		},
		{
			desc:               "multi witnesses greedy=true",
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(multiWitnesses, &WitnessOptions{Timeout: time.Second, Greedy: true}),
			expectCosignatures: []note.Verifier{wit1Verifier, wit2Verifier},
		},
		{
			desc:                "multi witnesses greedy=true with one failing witness",
			opts:                NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnesses(multiWitnesses, &WitnessOptions{Timeout: time.Second, Greedy: true}),
			partialWitnessFails: true,
			expectCosignatures:  []note.Verifier{wit1Verifier},
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
				test.opts.WithWitnesses(failingWitnesses, &test.opts.witnessOpts)
			}
			if test.partialWitnessFails {
				failingWitnessServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "internal error", http.StatusInternalServerError)
				}))
				defer failingWitnessServer.Close()

				failingURL, _ := url.Parse(failingWitnessServer.URL)
				failingWit, _ := NewWitness(testWit2VKey, failingURL)
				partiallyFailingWitnesses := NewWitnessGroup(1, wit1, failingWit)

				test.opts.WithWitnesses(partiallyFailingWitnesses, &test.opts.witnessOpts)
			}

			lr := newFakeLogReaderForTest(t)

			publisher, err := test.opts.CheckpointPublisherContext(t.Context(), lr, client)
			if err != nil {
				t.Fatalf("expected error %v but got: %v", test.expectErr, err)
			}
			cp, err := publisher(t.Context(), 5, []byte("12345678901234567890123456789012"))
			if (err != nil) != test.expectErr {
				t.Fatalf("expected error %v but got: %v", test.expectErr, err)
			}
			if err != nil {
				return
			}

			// Open checkpoint to verify signatures
			if test.expectNumCosignatures > 0 {
				allV := []note.Verifier{logVerifier, wit1Verifier, wit2Verifier, mirrorVerifier}
				n, err := note.Open(cp, note.VerifierList(allV...))
				if err != nil {
					t.Fatalf("failed to open signed checkpoint: %v", err)
				}
				if got, want := len(n.Sigs), 1+test.expectNumCosignatures; got != want {
					t.Errorf("expected %d signatures, got %d", want, got)
				}
			} else {
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
			}
		})
	}
}

func newFakeLogReaderForTest(t *testing.T) *fakeLogReader {
	hasher := rfc6962.DefaultHasher
	entries := [][]byte{
		[]byte("entry-0"),
		[]byte("entry-1"),
		[]byte("entry-2"),
		[]byte("entry-3"),
		[]byte("entry-4"),
	}

	h0 := hasher.HashLeaf(entries[0])
	h1 := hasher.HashLeaf(entries[1])
	h01 := hasher.HashChildren(h0, h1)
	h2 := hasher.HashLeaf(entries[2])
	h3 := hasher.HashLeaf(entries[3])
	h23 := hasher.HashChildren(h2, h3)
	h0123 := hasher.HashChildren(h01, h23)
	h4 := hasher.HashLeaf(entries[4])

	tileNodes := [][]byte{h0, h1, h01, h2, h3, h23, h0123, h4}
	var tileBuf bytes.Buffer
	for _, n := range tileNodes {
		tileBuf.Write(n)
	}
	tileBytes := tileBuf.Bytes()

	var bundleBuf bytes.Buffer
	for _, entry := range entries {
		_ = binary.Write(&bundleBuf, binary.BigEndian, uint16(len(entry)))
		bundleBuf.Write(entry)
	}
	bundleBytes := bundleBuf.Bytes()

	return &fakeLogReader{
		readCheckpoint: func(ctx context.Context) ([]byte, error) {
			return nil, os.ErrNotExist
		},
		readTile: func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
			if level == 0 && index == 0 {
				return tileBytes, nil
			}
			return nil, os.ErrNotExist
		},
		readEntryBundle: func(ctx context.Context, index uint64, p uint8) ([]byte, error) {
			if index == 0 {
				return bundleBytes, nil
			}
			return nil, os.ErrNotExist
		},
	}
}

func newMirrorHandler(t *testing.T, mirrorSKey string) http.HandlerFunc {
	mirrorSigner, err := f_note.NewSignerForCosignatureV1(mirrorSKey)
	if err != nil {
		t.Fatalf("failed to create mirror signer: %v", err)
	}
	logVerifier, err := note.NewVerifier("example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx")
	if err != nil {
		t.Fatalf("failed to create log verifier: %v", err)
	}

	var mu sync.Mutex
	var pendingCP []byte

	return func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/add-checkpoint") {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			parts := bytes.SplitN(body, []byte("\n\n"), 2)
			if len(parts) == 2 {
				mu.Lock()
				pendingCP = parts[1]
				mu.Unlock()
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/add-entries") {
			_, _ = io.Copy(io.Discard, r.Body)

			mu.Lock()
			cp := pendingCP
			mu.Unlock()

			if len(cp) == 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			// Open and parse the checkpoint note using log's verifier.
			n, err := note.Open(cp, note.VerifierList(logVerifier))
			if err != nil {
				t.Errorf("failed to open checkpoint in mock mirror: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			// Sign it with the mirror signer.
			signedNote, err := note.Sign(n, mirrorSigner)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Extract only the signature line we added.
			idx := strings.Index(string(signedNote), "\n— "+mirrorSigner.Name()+" ")
			if idx < 0 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			sigLine := string(signedNote)[idx+1:]

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(sigLine))
			return
		}
	}
}
func mustNewWitness(t *testing.T, vkey, urlStr string) Witness {
	url, err := url.Parse(urlStr)
	if err != nil {
		t.Fatalf("Failed to parse URL %s: %v", urlStr, err)
	}
	wit, err := NewWitness(vkey, url)
	if err != nil {
		t.Fatalf("failed to create witness: %v", err)
	}
	return wit
}
