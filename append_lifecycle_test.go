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
	"github.com/transparency-dev/formats/policy"
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
		name                   string
		opts                   *AppendOptions
		wantErrContains        string
		wantPublicationTimeout time.Duration
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
			name: "Valid: CheckpointPublicationTimeout < WitnessTimeout adjusts publication timeout",
			opts: NewAppendOptions().
				WithCheckpointSigner(mustCreateSigner(t, testSignerKey)).
				WithCheckpointPublicationTimeout(1*time.Second).
				WithWitnessPolicy(policy.TLogPolicy{Quorum: "none"}, &WitnessOptions{Timeout: 10 * time.Second}),
			wantPublicationTimeout: 10 * time.Second,
		}, {
			name: "Valid: CheckpointPublicationTimeout < MirrorTimeout adjusts publication timeout",
			opts: NewAppendOptions().
				WithCheckpointSigner(mustCreateSigner(t, testSignerKey)).
				WithCheckpointPublicationTimeout(1*time.Second).
				WithMirrorPolicy(policy.TLogPolicy{Quorum: "none"}, &MirroringOptions{Timeout: 15 * time.Second}),
			wantPublicationTimeout: 15 * time.Second,
		}, {
			name: "Valid: CheckpointPublicationTimeout adjusts to max of WitnessTimeout and MirrorTimeout",
			opts: NewAppendOptions().
				WithCheckpointSigner(mustCreateSigner(t, testSignerKey)).
				WithCheckpointPublicationTimeout(1*time.Second).
				WithWitnessPolicy(policy.TLogPolicy{Quorum: "none"}, &WitnessOptions{Timeout: 10 * time.Second}).
				WithMirrorPolicy(policy.TLogPolicy{Quorum: "none"}, &MirroringOptions{Timeout: 20 * time.Second}),
			wantPublicationTimeout: 20 * time.Second,
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
			err := test.opts.valid(t.Context())
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
			if test.wantPublicationTimeout > 0 && test.opts.CheckpointPublicationTimeout() != test.wantPublicationTimeout {
				t.Fatalf("Got CheckpointPublicationTimeout %v, want %v", test.opts.CheckpointPublicationTimeout(), test.wantPublicationTimeout)
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
	mirrorGrp := NewWitnessGroup(1, wit)

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
			opts := NewAppendOptions().WithMirrors(mirrorGrp, test.mirrorOpts)
			if got, want := opts.mirrorOpts.Timeout, test.expectTimeout; got != want {
				t.Errorf("expected timeout %v, got %v", want, got)
			}
			if got, want := opts.mirrorOpts.FailOpen, test.expectFailOpen; got != want {
				t.Errorf("expected FailOpen %t, got %t", want, got)
			}
		})
	}
}

func TestWithWitnessPolicy(t *testing.T) {
	witnesses := policy.TLogPolicy{}
	policy := fmt.Appendf(nil, "witness w1 %s %s\nquorum w1\n", testWit1VKey, "https://witness.example.com")
	if err := witnesses.Unmarshal(policy); err != nil {
		t.Fatalf("failed to unmarshal witness policy: %v", err)
	}

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
			opts := NewAppendOptions().WithWitnessPolicy(witnesses, test.witnessOpts)
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

func TestWithWitnesses_BackwardsCompatibility(t *testing.T) {
	wit := mustNewWitness(t, testWit1VKey, "https://witness.example.com")
	wg := NewWitnessGroup(1, wit)
	opts := NewAppendOptions().WithWitnesses(wg, &WitnessOptions{Timeout: 5 * time.Second})
	if got, want := opts.witnessOpts.Timeout, 5*time.Second; got != want {
		t.Errorf("expected timeout %v, got %v", want, got)
	}
	if got, want := len(opts.witnessPolicy.Witnesses), 1; got != want {
		t.Errorf("expected 1 witness in policy, got %d", got)
	}
	if opts.witnessPolicy.Quorum == "" || opts.witnessPolicy.Quorum == "none" {
		t.Fatalf("expected non-empty quorum in policy, got %q", opts.witnessPolicy.Quorum)
	}
	wit1Sign, err := f_note.NewSignerForCosignatureV1(testWit1SKey)
	if err != nil {
		t.Fatalf("NewSignerForCosignatureV1: %v", err)
	}
	n := &note.Note{Text: "sign me\nI'm a\nnote\n"}
	signedCP, err := note.Sign(n, wit1Sign)
	if err != nil {
		t.Fatalf("note.Sign: %v", err)
	}
	if !opts.witnessPolicy.Satisfied(signedCP) {
		t.Errorf("expected witness policy to be satisfied by witness signature")
	}
	unsignedCP, _ := note.Sign(n)
	if opts.witnessPolicy.Satisfied(unsignedCP) {
		t.Errorf("expected witness policy to fail without witness signature")
	}
}

func TestWithMirrors_BackwardsCompatibility(t *testing.T) {
	u, _ := url.Parse("https://mirror.example.com")
	wit, _ := NewWitness(testWit1VKey, u)
	wg := NewWitnessGroup(1, wit)
	opts := NewAppendOptions().WithMirrors(wg, &MirroringOptions{Timeout: 5 * time.Second})
	if got, want := opts.mirrorOpts.Timeout, 5*time.Second; got != want {
		t.Errorf("expected timeout %v, got %v", want, got)
	}
	if got, want := len(opts.mirrorPolicy.Witnesses), 1; got != want {
		t.Errorf("expected 1 mirror in policy, got %d", got)
	}
	if opts.mirrorPolicy.Quorum == "" || opts.mirrorPolicy.Quorum == "none" {
		t.Fatalf("expected non-empty quorum in mirror policy, got %q", opts.mirrorPolicy.Quorum)
	}
	mirrorSign, err := f_note.NewSignerForCosignatureV1(testWit1SKey)
	if err != nil {
		t.Fatalf("NewSignerForCosignatureV1: %v", err)
	}
	n := &note.Note{Text: "sign me\nI'm a\nnote\n"}
	signedCP, err := note.Sign(n, mirrorSign)
	if err != nil {
		t.Fatalf("note.Sign: %v", err)
	}
	if !opts.mirrorPolicy.Satisfied(signedCP) {
		t.Errorf("expected mirror policy to be satisfied by mirror signature")
	}
}

func TestMirrorGateway_DeduplicateURLs(t *testing.T) {
	mURL, _ := url.Parse("https://mirror.example.com")
	lr := newFakeLogReaderForTest(t)

	pol := policy.TLogPolicy{
		Witnesses: []policy.Witness{
			{Name: "m1", URL: mURL, VKey: testMirrorVKey},
			{Name: "m2", URL: mURL, VKey: testMirrorVKey},
		},
		Quorum: "m1",
	}
	opts := NewAppendOptions().WithMirrorPolicy(pol, nil)
	gw, err := opts.mirrorGateway(t.Context(), lr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gw == nil {
		t.Fatal("expected non-nil mirror gateway")
	}
}

const (
	testWit1VKey = "Wit1+4dd489c8+BDd/u4KCwMnyfkbOqopYZJIGSuMxMnjGav6Pjb9W8Y3c"
	testWit1SKey = "PRIVATE+KEY+Wit1+362a3b47+AVq3ou1kOLb/aTkJLoMwbMULJQNr8EQVGbpyZxMEZMyT"
	testWit2VKey = "Wit2+ef71459d+BJ1w/MdWovBzZtRD4pNwmb9SHl1U+hZzCMsgx6MmMJ0L"
	testWit2SKey = "PRIVATE+KEY+Wit2+4bf908f8+AXLjVp1/sY1o5exVavpVt8zWIVFAD7ejqaMBU38CoYgg"
	testWit3VKey = "Wit3+e1fc6196+BO1XsCjtkV0G56JUYj7n6LykElL1GcNo1BytsQMsRjyZ"
	testWit3SKey = "PRIVATE+KEY+Wit3+86588ce7+AS6qbV1WaGUoVgAz3CajG9iCm1pLZ5eUTKBD6XxJVl5x"

	testMirrorVKey = "Mirror1+e2466b7b+BECHU/Mq/HN+4Nmsxw/NxmRTZ1dvOkgf3IkAS/+XJ9Za"
	testMirrorSKey = "PRIVATE+KEY+Mirror1+eccc0fa7+ATLe/CQcL8aCY0TofdnDKFX43pZj6NOY8BNQIgqJ885A"
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

	timeout := time.Second

	for _, test := range []struct {
		desc               string
		policy             policy.TLogPolicy
		fetcher            func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte
		failOpen           bool
		greedy             bool
		expectCosignatures []note.Verifier
		expectErr          bool
		expectFailedOpen   bool
	}{
		{
			desc:   "empty policy",
			policy: policy.TLogPolicy{},
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte)
				defer close(ch)
				return ch
			},
		},
		{
			desc: "non-greedy stops after quorum is satisfied (1 of 2)",
			policy: makeGroupPolicy(t, 1, []keyURL{
				{key: testWit1VKey, url: "https://wit1.example.com"},
				{key: testWit2VKey, url: "https://wit2.example.com"}}),
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
			desc: "greedy gathers surplus signatures (2 of 3 required, 3 provided)",
			policy: makeGroupPolicy(t, 2, []keyURL{
				{key: testWit1VKey, url: "https://wit1.example.com"},
				{key: testWit2VKey, url: "https://wit2.example.com"},
				{key: testWit3VKey, url: "https://wit3.example.com"}}),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 3)
				defer close(ch)
				ch <- sig1
				ch <- sig2
				ch <- sig3
				return ch
			},
			greedy:             true,
			expectCosignatures: []note.Verifier{wit1Verifier, wit2Verifier, wit3Verifier},
		},
		{
			desc: "greedy succeeds when quorum is met and channel closes without further signatures (1 of 2 required, 1 provided)",
			policy: makeGroupPolicy(t, 1, []keyURL{
				{key: testWit1VKey, url: "https://wit1.example.com"},
				{key: testWit2VKey, url: "https://wit2.example.com"}}),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				defer close(ch)
				ch <- sig1
				return ch
			},
			greedy:             true,
			expectCosignatures: []note.Verifier{wit1Verifier},
		},
		{
			desc: "greedy fails when quorum is not met and channel closes (failOpen=false)",
			policy: makeGroupPolicy(t, 2, []keyURL{
				{key: testWit1VKey, url: "https://wit1.example.com"},
				{key: testWit2VKey, url: "https://wit2.example.com"}}),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				defer close(ch)
				ch <- sig1
				return ch
			},
			greedy:    true,
			failOpen:  false,
			expectErr: true,
		},
		{
			desc: "greedy fails open when quorum is not met and channel closes (failOpen=true)",
			policy: makeGroupPolicy(t, 2, []keyURL{
				{key: testWit1VKey, url: "https://wit1.example.com"},
				{key: testWit2VKey, url: "https://wit2.example.com"}}),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				defer close(ch)
				ch <- sig1
				return ch
			},
			greedy:             true,
			failOpen:           true,
			expectFailedOpen:   true,
			expectCosignatures: []note.Verifier{wit1Verifier},
		},
		{
			desc: "greedy fails when quorum is not met on timeout (failOpen=false)",
			policy: makeGroupPolicy(t, 2, []keyURL{
				{key: testWit1VKey, url: "https://wit1.example.com"},
				{key: testWit2VKey, url: "https://wit2.example.com"}}),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				ch <- sig1
				return ch
			},
			greedy:    true,
			failOpen:  false,
			expectErr: true,
		},
		{
			desc: "greedy fails open when quorum is not met on timeout (failOpen=true)",
			policy: makeGroupPolicy(t, 2, []keyURL{
				{key: testWit1VKey, url: "https://wit1.example.com"},
				{key: testWit2VKey, url: "https://wit2.example.com"}}),
			fetcher: func(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
				ch := make(chan []byte, 1)
				ch <- sig1
				return ch
			},
			greedy:             true,
			failOpen:           true,
			expectFailedOpen:   true,
			expectCosignatures: []note.Verifier{wit1Verifier},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			sigs, err := gatherCosignatures(ctx, "witness", test.fetcher, test.policy, signedCP, 5, test.failOpen, test.greedy)
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
	wit1Verifier, err := f_note.NewVerifierForCosignatureV1(testWit1VKey)
	if err != nil {
		t.Fatalf("failed to create witness 1 verifier: %v", err)
	}

	witnessPolicy := makeGroupPolicy(t, 1, []keyURL{{key: testWit1VKey, url: witnessServerURL1.String()}})

	witnessServer2 := httptest.NewServer(newWitnessHandler(t, logVerifier, testWit2SKey))
	t.Cleanup(witnessServer2.Close)

	witnessServerURL2, err := url.Parse(witnessServer2.URL)
	if err != nil {
		t.Fatalf("failed to parse witness server 2 url: %v", err)
	}
	wit2Verifier, err := f_note.NewVerifierForCosignatureV1(testWit2VKey)
	if err != nil {
		t.Fatalf("failed to create witness 2 verifier: %v", err)
	}

	multiWitnessPolicy := makeGroupPolicy(t, 1, []keyURL{
		{key: testWit1VKey, url: witnessServerURL1.String()},
		{key: testWit2VKey, url: witnessServerURL2.String()}})

	mirrorServer := httptest.NewServer(newMirrorHandler(t, testMirrorSKey))
	t.Cleanup(mirrorServer.Close)

	mirrorServerURL, err := url.Parse(mirrorServer.URL)
	if err != nil {
		t.Fatalf("failed to parse mirror server url: %v", err)
	}
	mirrorVerifier, err := f_note.NewVerifierForCosignatureV1(testMirrorVKey)
	if err != nil {
		t.Fatalf("failed to create mirror verifier: %v", err)
	}

	mirrorPolicy := makeGroupPolicy(t, 1, []keyURL{{testMirrorVKey, mirrorServerURL.String()}})

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
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnessPolicy(witnessPolicy, &WitnessOptions{Timeout: time.Second}),
			expectCosignatures: []note.Verifier{wit1Verifier},
		},
		{
			desc:               "mirrors only",
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithMirrorPolicy(mirrorPolicy, &MirroringOptions{Timeout: time.Second}),
			expectCosignatures: []note.Verifier{mirrorVerifier},
		},
		{
			desc:               "witnesses and mirrors",
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnessPolicy(witnessPolicy, &WitnessOptions{Timeout: time.Second}).WithMirrorPolicy(mirrorPolicy, &MirroringOptions{Timeout: time.Second}),
			expectCosignatures: []note.Verifier{wit1Verifier, mirrorVerifier},
		},
		{
			desc:         "witness fails, failOpen=false",
			opts:         NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnessPolicy(witnessPolicy, &WitnessOptions{FailOpen: false, Timeout: time.Second}),
			witnessFails: true,
			expectErr:    true,
		},
		{
			desc:         "witness fails, failOpen=true",
			opts:         NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnessPolicy(witnessPolicy, &WitnessOptions{FailOpen: true, Timeout: time.Second}),
			witnessFails: true,
		},
		{
			desc:                  "multi witnesses greedy=false",
			opts:                  NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnessPolicy(multiWitnessPolicy, &WitnessOptions{Timeout: time.Second, Greedy: false}),
			expectNumCosignatures: 1,
		},
		{
			desc:               "multi witnesses greedy=true",
			opts:               NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnessPolicy(multiWitnessPolicy, &WitnessOptions{Timeout: time.Second, Greedy: true}),
			expectCosignatures: []note.Verifier{wit1Verifier, wit2Verifier},
		},
		{
			desc:                "multi witnesses greedy=true with one failing witness",
			opts:                NewAppendOptions().WithCheckpointSigner(logSigner).WithWitnessPolicy(multiWitnessPolicy, &WitnessOptions{Timeout: time.Second, Greedy: true}),
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
				failingWitnessPolicy := makeGroupPolicy(t, 1, []keyURL{{
					key: testWit1VKey,
					url: failingURL.String()},
				})

				// Re-configure option to use failing witnesses
				test.opts.WithWitnessPolicy(failingWitnessPolicy, &test.opts.witnessOpts)
			}
			if test.partialWitnessFails {
				failingWitnessServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "internal error", http.StatusInternalServerError)
				}))
				defer failingWitnessServer.Close()

				failingURL, _ := url.Parse(failingWitnessServer.URL)
				partiallyFailingPolicy := makeGroupPolicy(t, 1, []keyURL{
					{key: testWit1VKey, url: witnessServerURL1.String()},
					{key: testWit2VKey, url: failingURL.String()},
				})

				test.opts.WithWitnessPolicy(partiallyFailingPolicy, &test.opts.witnessOpts)
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

type keyURL struct {
	key string
	url string
}

func makeGroupPolicy(t *testing.T, N int, ws []keyURL) policy.TLogPolicy {
	t.Helper()

	b := []byte{}
	grpMembers := []string{}
	for i, w := range ws {
		wName := fmt.Sprintf("w%d", i)
		b = fmt.Appendf(b, "witness %s %s %s\n", wName, w.key, w.url)
		grpMembers = append(grpMembers, wName)
	}
	b = fmt.Appendf(b, "group g1 %d %s\n", N, strings.Join(grpMembers, " "))
	b = fmt.Appendf(b, "quorum g1\n")

	r := policy.TLogPolicy{}
	if err := r.Unmarshal(b); err != nil {
		t.Fatalf("failed to unmarshal policy %q: %v", string(b), err)
	}
	return r
}
