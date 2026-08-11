// Copyright 2025 Google LLC. All Rights Reserved.
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

package witness_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/transparency-dev/formats/log"
	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/witness"
	"golang.org/x/mod/sumdb/note"
)

const (
	logVkey     = "example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
	wit1Vkey    = "Wit1+55ee4561+AVhZSmQj9+SoL+p/nN0Hh76xXmF7QcHfytUrI1XfSClk"
	wit1Skey    = "PRIVATE+KEY+Wit1+55ee4561+AeadRiG7XM4XiieCHzD8lxysXMwcViy5nYsoXURWGrlE"
	wit2Vkey    = "Wit2+85ecc407+AWVbwFJte9wMQIPSnEnj4KibeO6vSIOEDUTDp3o63c2x"
	wit2Skey    = "PRIVATE+KEY+Wit2+85ecc407+AfPTvxw5eUcqSgivo2vaiC7JPOMUZ/9baHPSDrWqgdGm"
	wit2AltSKey = "PRIVATE+KEY+Wit2+36009101+ASWI6XB1l1/fPORsXVxCbMvxfrvh7bXtYkNNlD1NYe2H"
	wit2AltVKey = "Wit2+112f0455+BCy8NYvyk7N1dkxNxgrI3YAJzQDc0FIfs0q7q8U/cwOF"
	witBadVkey  = "WitBad+b82b4b16+AY5FLOcqxs5lD+OpC6cVTrxsyNJktaCGYHNfnE5vKBQX"
	witBadSkey  = "PRIVATE+KEY+WitBad+b82b4b16+AYSil2PKfSN1a0LhdbzmK1uXqDFZbp+P1OyR54k3gdJY"
)

var (
	logVerifier = mustCreateVerifier(logVkey)
)

func collectSigs(t *testing.T, cs <-chan []byte) (sigs []byte, fromWitnesses int) {
	t.Helper()
	for sig := range cs {
		sigs = append(sigs, sig...)
		fromWitnesses++
	}
	return
}

func TestWitnessGateway(t *testing.T) {
	const logSignedCheckpointSize = 9
	logSignedCheckpoint, cp := loadCheckpoint(t, logSignedCheckpointSize)

	// Set up a fake server hosting the witnesses.
	// The witnesses just sign the checkpoint with whatever key is requested, they don't check the body at all.
	// An improvement on this would be to make the fake witnesses more realistic, but it's a non-trivial
	// amount of code to add to this already long test!
	var wit1, wit2, witBad, witMulti1, witMulti2 tessera.Witness
	var witCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w1u, err := url.Parse(wit1.URL)
		if err != nil {
			t.Fatal(err)
		}
		w2u, err := url.Parse(wit2.URL)
		if err != nil {
			t.Fatal(err)
		}
		wbu, err := url.Parse(witBad.URL)
		if err != nil {
			t.Fatal(err)
		}

		switch r.URL.Path {
		case w1u.Path + "/add-checkpoint":
			witCalls.Add(1)
			_, _ = w.Write(sigForSigner(t, cp, wit1Skey))
		case w2u.Path + "/add-checkpoint":
			witCalls.Add(1)
			_, _ = w.Write(sigForSigner(t, cp, wit2Skey))
		case wbu.Path + "/add-checkpoint":
			witCalls.Add(1)
			_, _ = w.Write([]byte("this is not a signature\n"))
		case "/wit_multi/add-checkpoint":
			witCalls.Add(1)
			res := append(sigForSigner(t, cp, wit1Skey), sigForSigner(t, cp, wit2Skey)...)
			_, _ = w.Write(res)
		default:
			t.Fatalf("Unknown case: %s", r.URL.String())
		}
	}))
	baseURL, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	wit1URL := baseURL.JoinPath("wit1")
	wit1, err = tessera.NewWitness(wit1Vkey, wit1URL)
	if err != nil {
		t.Fatal(err)
	}
	wit2URL := baseURL.JoinPath("wit2")
	wit2, err = tessera.NewWitness(wit2Vkey, wit2URL)
	if err != nil {
		t.Fatal(err)
	}
	witMulti1URL := baseURL.JoinPath("wit_multi")
	witMulti1, err = tessera.NewWitness(wit1Vkey, witMulti1URL)
	if err != nil {
		t.Fatal(err)
	}
	witMulti2, err = tessera.NewWitness(wit2Vkey, witMulti1URL)
	if err != nil {
		t.Fatal(err)
	}
	witBad, err = tessera.NewWitness(witBadVkey, baseURL)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		desc             string
		witnesses        []witness.Witness
		wantSigs         int
		wantErr          bool
		wantWitnessCalls func(actual int32) error
	}{
		{
			desc:             "no witnesses",
			witnesses:        []witness.Witness{},
			wantSigs:         0,
			wantWitnessCalls: exactly(0),
		},
		{
			desc:             "one witness",
			witnesses:        []witness.Witness{{URL: wit1URL, Verifiers: []note.Verifier{wit1.Key}}},
			wantSigs:         1,
			wantWitnessCalls: exactly(1),
		},
		{
			desc: "two witnesses",
			witnesses: []witness.Witness{
				{URL: wit1URL, Verifiers: []note.Verifier{wit1.Key}},
				{URL: wit2URL, Verifiers: []note.Verifier{wit2.Key}},
			},
			wantSigs:         2,
			wantWitnessCalls: exactly(2),
		},
		{
			desc: "one required witness twice",
			witnesses: []witness.Witness{
				{URL: wit1URL, Verifiers: []note.Verifier{wit1.Key}},
				{URL: wit1URL, Verifiers: []note.Verifier{wit1.Key}},
			},
			wantSigs:         1,
			wantWitnessCalls: exactly(1),
		},
		{
			desc:             "one witness with two keys",
			witnesses:        []witness.Witness{{URL: witMulti1URL, Verifiers: []note.Verifier{witMulti1.Key, witMulti2.Key}}},
			wantSigs:         2,
			wantWitnessCalls: exactly(1),
		},
		{
			desc: "two witnesses with same URL but different keys",
			witnesses: []witness.Witness{
				{URL: witMulti1URL, Verifiers: []note.Verifier{witMulti1.Key}},
				{URL: witMulti1URL, Verifiers: []note.Verifier{witMulti2.Key}},
			},
			wantSigs:         2,
			wantWitnessCalls: exactly(1),
		},
		{
			desc:             "bad witness",
			witnesses:        []witness.Witness{{URL: wit1URL, Verifiers: []note.Verifier{witBad.Key}}},
			wantSigs:         0,
			wantWitnessCalls: exactly(1),
		}, {
			desc: "two bad witnesses",
			witnesses: []witness.Witness{
				{URL: wit1URL, Verifiers: []note.Verifier{witBad.Key}},
				{URL: wit2URL, Verifiers: []note.Verifier{witBad.Key}},
			},
			wantSigs:         0,
			wantWitnessCalls: exactly(2),
		}, {
			desc: "one good, one bad witness",
			witnesses: []witness.Witness{
				{URL: wit1URL, Verifiers: []note.Verifier{wit1.Key}},
				{URL: wit2URL, Verifiers: []note.Verifier{witBad.Key}},
			},
			wantSigs:         1,
			wantWitnessCalls: exactly(2),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			ctx := t.Context()
			witCalls.Store(0)

			g, err := witness.NewGateway(ctx, witness.Options{
				HTTPClient: ts.Client(),
				Witnesses:  test.witnesses,
				FetchTiles: testLogTileFetcher,
			})
			if err != nil {
				t.Fatalf("NewGateway() error: %v", err)
			}

			cpSigs, _ := collectSigs(t, g.CosignCheckpoint(ctx, logSignedCheckpoint, logSignedCheckpointSize))
			witnessedCP := append(slices.Clone(logSignedCheckpoint), cpSigs...)
			n, err := note.Open(witnessedCP, note.VerifierList(logVerifier, wit1.Key, wit2.Key))
			if err != nil {
				t.Fatalf("failed to open note %q: %v", witnessedCP, err)
			}
			if len(n.Sigs)-1 < test.wantSigs {
				t.Errorf("wanted %d sigs but got %d", test.wantSigs, len(n.Sigs)-1)
			}
			if test.wantWitnessCalls != nil {
				if err := test.wantWitnessCalls(witCalls.Load()); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func exactly(x int32) func(actual int32) error {
	return func(actual int32) error {
		if actual != x {
			return fmt.Errorf("got %d calls, want %d", actual, x)
		}
		return nil
	}
}

func TestSlipperyWitness(t *testing.T) {
	logSize := 9
	logSignedCheckpoint, _ := loadCheckpoint(t, logSize)

	// Set up a fake server hosting the witness.
	// This witness will always reply that a different size is required.
	var wit1 tessera.Witness
	var count int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w1u := mustURL(t, wit1.URL)
		if got, want := r.URL.String(), w1u.Path+"/add-checkpoint"; got != want {
			t.Fatalf("Got request to URL %q but expected %q", got, want)
		}

		w.Header().Add("Content-Type", "text/x.tlog.size")
		w.WriteHeader(409)

		// Keep telling the log that we were at a different size
		_, _ = w.Write(fmt.Appendf(nil, "%d", count%logSize))
		count++
	}))
	baseURL := mustURL(t, ts.URL)
	var err error
	wit1, err = tessera.NewWitness(wit1Vkey, baseURL)
	if err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()

	g, err := witness.NewGateway(ctx, witness.Options{
		Witnesses:  []witness.Witness{{URL: baseURL, Verifiers: []note.Verifier{wit1.Key}}},
		HTTPClient: ts.Client(),
		FetchTiles: testLogTileFetcher,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	sigs, N := collectSigs(t, g.CosignCheckpoint(ctx, logSignedCheckpoint, uint64(logSize)))
	if N != 0 || len(sigs) != 0 {
		t.Errorf("Expected 0 signatures from slippery witness, got %d (%s)", N, sigs)
	}

}

func TestWitnessReusesProofs(t *testing.T) {
	var wit1, wit2 tessera.Witness
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		_, checkpoint, ok := bytes.Cut(body, []byte("\n\n"))
		if !ok {
			t.Fatalf("expected two newlines in body, got: %q", body)
		}

		_, _, n, err := log.ParseCheckpoint(checkpoint, logVerifier.Name(), logVerifier)
		if err != nil {
			t.Fatal(err)
		}
		w1u := mustURL(t, wit1.URL)
		w2u := mustURL(t, wit2.URL)

		switch r.URL.String() {
		case w1u.Path + "/add-checkpoint":
			_, _ = w.Write(sigForSigner(t, n.Text, wit1Skey))
		case w2u.Path + "/add-checkpoint":
			_, _ = w.Write(sigForSigner(t, n.Text, wit2Skey))
		default:
			t.Fatalf("Unknown case: %s", r.URL.String())
		}
	}))
	baseURL := mustURL(t, ts.URL)
	var err error
	wit1URL := baseURL.JoinPath("wit1")
	wit1, err = tessera.NewWitness(wit1Vkey, wit1URL)
	if err != nil {
		t.Fatal(err)
	}
	wit2URL := baseURL.JoinPath("wit2")
	wit2, err = tessera.NewWitness(wit2Vkey, wit2URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	var tf1 atomic.Int32
	var tf2 atomic.Int32
	cf1 := func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
		tf1.Add(1)
		return testLogTileFetcher(ctx, level, index, p)
	}
	cf2 := func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
		tf2.Add(1)
		return testLogTileFetcher(ctx, level, index, p)
	}
	g1, err := witness.NewGateway(ctx, witness.Options{
		HTTPClient: ts.Client(),
		Witnesses:  []witness.Witness{{URL: wit1URL, Verifiers: []note.Verifier{wit1.Key}}},
		FetchTiles: cf1,
	})
	if err != nil {
		t.Fatal(err)
	}
	g2, err := witness.NewGateway(ctx, witness.Options{
		HTTPClient: ts.Client(),
		Witnesses: []witness.Witness{
			{URL: wit1URL, Verifiers: []note.Verifier{wit1.Key}},
			{URL: wit2URL, Verifiers: []note.Verifier{wit2.Key}},
		},
		FetchTiles: cf2,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := range 10 {
		logSignedCheckpoint, _ := loadCheckpoint(t, i)
		_, N1 := collectSigs(t, g1.CosignCheckpoint(ctx, logSignedCheckpoint, uint64(i)))
		_, N2 := collectSigs(t, g2.CosignCheckpoint(ctx, logSignedCheckpoint, uint64(i)))
		if N1 != 1 {
			t.Fatalf("expected 1 signature from g1 but got %d", N1)
		}
		if N2 != 2 {
			t.Fatalf("expected 2 signatures from g2 but got %d", N2)
		}
	}

	if got1, got2 := tf1.Load(), tf2.Load(); got1 != got2 {
		t.Errorf("expected same number of tiles loaded for 1 witness or 2 witnesses but got (%d != %d)", got1, got2)
	}
}

func loadCheckpoint(t *testing.T, size int) (signed []byte, unsigned string) {
	t.Helper()
	path := fmt.Sprintf("../../testdata/log/checkpoint.%d", size)
	cp, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, _, n, err := log.ParseCheckpoint(cp, logVerifier.Name(), logVerifier)
	if err != nil {
		t.Fatal(err)
	}
	return cp, n.Text
}

// testLogTileFetcher is a fetcher which reads tiles from the checked-in golden test log
// data stored in $REPO_ROOT/testdata/log
func testLogTileFetcher(ctx context.Context, l, i uint64, p uint8) ([]byte, error) {
	path := filepath.Join("../../testdata/log", layout.TilePath(l, i, p))
	return os.ReadFile(path)
}

func mustURL(t *testing.T, u string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func sigForSigner(t *testing.T, cp, skey string) []byte {
	t.Helper()
	s, err := f_note.NewSignerForCosignatureV1(skey)
	if err != nil {
		t.Fatal(err)
	}
	witSignedCheckpoint, err := note.Sign(&note.Note{Text: cp}, s)
	if err != nil {
		t.Fatal(err)
	}
	return append(bytes.Trim(witSignedCheckpoint[len(cp):], "\n"), '\n')
}

func mustCreateVerifier(vkey string) note.Verifier {
	verifier, err := note.NewVerifier(vkey)
	if err != nil {
		panic(err)
	}
	return verifier
}
