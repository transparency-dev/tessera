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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/transparency-dev/formats/log"
	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/witness"
	"github.com/transparency-dev/tessera/storage/posix"
	"golang.org/x/mod/sumdb/note"
)

const (
	logVkey    = "example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
	wit1Vkey   = "Wit1+55ee4561+AVhZSmQj9+SoL+p/nN0Hh76xXmF7QcHfytUrI1XfSClk"
	wit1Skey   = "PRIVATE+KEY+Wit1+55ee4561+AeadRiG7XM4XiieCHzD8lxysXMwcViy5nYsoXURWGrlE"
	wit2Vkey   = "Wit2+85ecc407+AWVbwFJte9wMQIPSnEnj4KibeO6vSIOEDUTDp3o63c2x"
	wit2Skey   = "PRIVATE+KEY+Wit2+85ecc407+AfPTvxw5eUcqSgivo2vaiC7JPOMUZ/9baHPSDrWqgdGm"
	witBadVkey = "WitBad+b82b4b16+AY5FLOcqxs5lD+OpC6cVTrxsyNJktaCGYHNfnE5vKBQX"
	witBadSkey = "PRIVATE+KEY+WitBad+b82b4b16+AYSil2PKfSN1a0LhdbzmK1uXqDFZbp+P1OyR54k3gdJY"
)

var (
	logVerifier = mustCreateVerifier(logVkey)
)

func TestWitnessGateway_Update(t *testing.T) {
	logSignedCheckpoint, cp := loadCheckpoint(t, 9)

	// Set up a fake server hosting the witnesses.
	// The witnesses just sign the checkpoint with whatever key is requested, they don't check the body at all.
	// An improvement on this would be to make the fake witnesses more realistic, but it's a non-trivial
	// amount of code to add to this already long test!
	var wit1 tessera.Witness
	var wit2 tessera.Witness
	var witBad tessera.Witness
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

		switch r.URL.String() {
		case w1u.Path:
			_, _ = w.Write(sigForSigner(t, cp, wit1Skey))
		case w2u.Path:
			_, _ = w.Write(sigForSigner(t, cp, wit2Skey))
		case wbu.Path:
			_, _ = w.Write([]byte("this is not a signature\n"))
		default:
			t.Fatalf("Unknown case: %s", r.URL.String())
		}
	}))
	baseURL, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	wit1, err = tessera.NewWitness(wit1Vkey, baseURL.JoinPath("wit1"))
	if err != nil {
		t.Fatal(err)
	}
	wit2, err = tessera.NewWitness(wit2Vkey, baseURL.JoinPath("wit2"))
	if err != nil {
		t.Fatal(err)
	}
	witBad, err = tessera.NewWitness(witBadVkey, baseURL)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		desc     string
		group    tessera.WitnessGroup
		wantSigs int
		wantErr  bool
	}{
		{
			desc:     "no witnesses",
			group:    tessera.WitnessGroup{},
			wantSigs: 0,
		},
		{
			desc:     "one optional witness",
			group:    tessera.NewWitnessGroup(0, wit1),
			wantSigs: 0,
		},
		{
			desc:     "two optional witnesses",
			group:    tessera.NewWitnessGroup(0, wit1, wit2),
			wantSigs: 0,
		},
		{
			desc:     "one required witness",
			group:    tessera.NewWitnessGroup(1, wit1),
			wantSigs: 1,
		},
		{
			desc:     "one required witness out of 2",
			group:    tessera.NewWitnessGroup(1, wit1, wit2),
			wantSigs: 1,
		},
		{
			desc:     "two required witnesses",
			group:    tessera.NewWitnessGroup(2, wit1, wit2),
			wantSigs: 2,
		},
		{
			desc:     "one required witness twice",
			group:    tessera.NewWitnessGroup(2, wit1, wit1),
			wantSigs: 1,
		},
		{
			desc:    "bad witness",
			group:   tessera.NewWitnessGroup(1, witBad),
			wantErr: true,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			ctx := context.Background()

			g := witness.NewWitnessGateway(tC.group, ts.Client(), 0, testLogTileFetcher)

			witnessedCP, err := g.Witness(ctx, logSignedCheckpoint)
			if got, want := err != nil, tC.wantErr; got != want {
				t.Fatalf("got != want (%t != %t): %v", got, want, err)
			}
			if tC.wantErr {
				return
			}
			n, err := note.Open(witnessedCP, note.VerifierList(logVerifier, wit1.Key, wit2.Key))
			if err != nil {
				t.Fatalf("failed to open note %q: %v", witnessedCP, err)
			}
			if len(n.Sigs)-1 < tC.wantSigs {
				t.Errorf("wanted %d sigs but got %d", tC.wantSigs, len(n.Sigs)-1)
			}
		})
	}
}

func TestWitness_UpdateRequest(t *testing.T) {
	logSignedCheckpoint, _ := loadCheckpoint(t, 9)
	d, err := posix.New(context.Background(), posix.Config{Path: "../../testdata/log/"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, reader, err := tessera.NewAppender(context.Background(), d, tessera.NewAppendOptions().WithCheckpointSigner(mustCreateCoSigSigner(t, wit1Skey)))
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		desc     string
		proof    [][]byte
		witSize  uint64
		wantErr  bool
		wantBody string
	}{
		{
			desc:     "size 0 no proof needed",
			witSize:  0,
			wantBody: fmt.Sprintf("old 0\n\n%s", logSignedCheckpoint),
		},
		{
			desc:     "non zero size requires proof",
			witSize:  6,
			wantBody: fmt.Sprintf("old 6\nycRkkNklus5eMVRUvkD1pK321vMrA+jjOiZKU8aOcY4=\nnk9gCR+floFqznAPtqjjcnnV64dge2jQB95D5t164Hg=\nzY1lN35vrXYAPixXSd59LsU29xUJtuW4o2dNNg5Y2Co=\n91HQqaPzWlbBsUDk3JvSpOTK7Bc4ifZGxXZzfABOmuU=\n\n%s", logSignedCheckpoint),
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			ctx := context.Background()
			var gotBody string
			var initDone bool
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !initDone {
					w.Header().Add("Content-Type", "text/x.tlog.size")
					w.WriteHeader(409)
					_, _ = fmt.Fprintf(w, "%d", tC.witSize)
					initDone = true
					return
				}

				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}

				gotBody = string(body)
				_, checkpoint, ok := bytes.Cut(body, []byte("\n\n"))
				if !ok {
					t.Fatalf("expected two newlines in body, got: %q", body)
				}

				_, _, n, err := log.ParseCheckpoint(checkpoint, logVerifier.Name(), logVerifier)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = w.Write(sigForSigner(t, n.Text, wit1Skey))
			}))
			baseURL := mustURL(t, ts.URL)
			var err error
			wit1, err := tessera.NewWitness(wit1Vkey, baseURL)
			if err != nil {
				t.Fatal(err)
			}
			group := tessera.NewWitnessGroup(1, wit1)
			wg := witness.NewWitnessGateway(group, ts.Client(), 0, reader.ReadTile)
			_, err = wg.Witness(ctx, logSignedCheckpoint)
			if got, want := err != nil, tC.wantErr; got != want {
				t.Fatalf("got != want (%t != %t): %v", got, want, err)
			}
			if tC.wantErr {
				return
			}

			if gotBody != tC.wantBody {
				t.Errorf("body does not match expected (want vs got):\n%q\n%q", tC.wantBody, gotBody)
			}
		})
	}
}

func TestWitness_UpdateResponse(t *testing.T) {
	logSignedCheckpoint, cp := loadCheckpoint(t, 9)

	sig1 := sigForSigner(t, cp, wit1Skey)
	sig2 := sigForSigner(t, cp, wit2Skey)

	testCases := []struct {
		desc       string
		statusCode int
		body       []byte
		pre        error
		wantErr    bool
		wantResult []byte
	}{
		{
			desc:       "all good",
			statusCode: 200,
			body:       sig1,
			wantResult: sig1,
		}, {
			desc:       "all good, two sigs",
			statusCode: 200,
			body:       append(sig1, sig2...),
			wantResult: sig1,
		}, {
			desc:       "404 is an error",
			statusCode: 404,
			wantErr:    true,
		}, {
			desc:       "403 is an error",
			statusCode: 403,
			wantErr:    true,
		}, {
			desc:       "422 is an error",
			statusCode: 422,
			wantErr:    true,
		}, {
			desc:       "409 with no headers is error",
			statusCode: 409,
			wantErr:    true,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			ctx := context.Background()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tC.statusCode)
				_, _ = w.Write(tC.body)
			}))

			baseURL := mustURL(t, ts.URL)
			wit1, err := tessera.NewWitness(wit1Vkey, baseURL)
			if err != nil {
				t.Fatal(err)
			}
			g := witness.NewWitnessGateway(tessera.NewWitnessGroup(1, wit1), ts.Client(), 0, testLogTileFetcher)
			witnessed, err := g.Witness(ctx, logSignedCheckpoint)
			if got, want := err != nil, tC.wantErr; got != want {
				t.Fatalf("got != want (%t != %t): %v", got, want, err)
			}
			if tC.wantErr {
				return
			}

			sigs := witnessed[len(logSignedCheckpoint):]
			if !bytes.Equal(sigs, tC.wantResult) {
				t.Errorf("expected result %q but got %q", tC.body, sigs)
			}
		})
	}
}

func TestWitnessConflict(t *testing.T) {
	for _, test := range []struct {
		name        string
		witnessSeen uint64
		oldSizeHint uint64
		wantErr     bool
	}{
		{
			name: "nothing seen before",
		},
		{
			name:        "correct hint",
			witnessSeen: 8,
			oldSizeHint: 8,
		},
		{
			name:        "hint stale - witness missed an update",
			witnessSeen: 4,
			oldSizeHint: 8,
		},
		{
			name:        "log rolled back - witness is ahead of log",
			witnessSeen: 20,
			wantErr:     true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logSignedCheckpoint, cp := loadCheckpoint(t, 9)

			var wit1 tessera.Witness
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w1u := mustURL(t, wit1.URL)
				if got, want := r.URL.String(), w1u.Path; got != want {
					t.Fatalf("got request to URL %q but expected %q", got, want)
				}

				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("error reading body: %v", err)
				}
				lines := bytes.Split(body, []byte("\n"))
				if len(lines) == 0 {
					t.Fatal("empty body")
				}
				bits := strings.Split(string(lines[0]), " ")
				if len(bits) != 2 || bits[0] != "old" {
					t.Fatal("invalid old line")
				}
				oldSize, err := strconv.ParseUint(bits[1], 10, 64)
				if err != nil {
					t.Fatalf("Invalid old size %v", err)
				}

				if oldSize != test.witnessSeen {
					t.Logf("Saw stale %d != %d", oldSize, test.witnessSeen)
					w.Header().Add("Content-Type", "text/x.tlog.size")
					w.WriteHeader(409)
					_, _ = fmt.Fprintf(w, "%d\n", test.witnessSeen)
					return
				}

				_, _ = w.Write(sigForSigner(t, cp, wit1Skey))
				test.witnessSeen = oldSize
			}))
			baseURL := mustURL(t, ts.URL)
			var err error
			wit1, err = tessera.NewWitness(wit1Vkey, baseURL)
			if err != nil {
				t.Fatal(err)
			}

			group := tessera.NewWitnessGroup(1, wit1)
			g := witness.NewWitnessGateway(group, ts.Client(), test.oldSizeHint, testLogTileFetcher)
			_, err = g.Witness(t.Context(), logSignedCheckpoint)
			if gotErr := err != nil; gotErr != test.wantErr {
				t.Fatalf("Got err %v, want err %t", err, test.wantErr)
			}
		})
	}
}

func TestWitnessStateEvolution(t *testing.T) {
	logSignedCheckpoint, cp := loadCheckpoint(t, 9)

	// Set up a fake server hosting the witnesses.
	// The witnesses just sign the checkpoint with whatever key is requested, they don't check the body at all.
	// An improvement on this would be to make the fake witnesses more realistic, but it's a non-trivial
	// amount of code to add to this already long test!
	var wit1 tessera.Witness
	var count int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w1u := mustURL(t, wit1.URL)
		if got, want := r.URL.String(), w1u.Path; got != want {
			t.Fatalf("got request to URL %q but expected %q", got, want)
		}

		switch count {
		case 0:
			w.Header().Add("Content-Type", "text/x.tlog.size")
			w.WriteHeader(409)
			_, _ = w.Write([]byte("8"))
		case 1:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(body, []byte("old 8")) {
				t.Fatalf("expected body to start with old 8 but got\n%v", body)
			}

			_, _ = w.Write(sigForSigner(t, cp, wit1Skey))
		case 2:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(body, []byte("old 9")) {
				t.Fatalf("expected body to start with old 9 but got\n%v", string(body))
			}
			// End of test; we don't even bother constructing a valid response here
		}
		count++
	}))
	baseURL := mustURL(t, ts.URL)
	var err error
	wit1, err = tessera.NewWitness(wit1Vkey, baseURL)
	if err != nil {
		t.Fatal(err)
	}
	group := tessera.NewWitnessGroup(1, wit1)

	ctx := context.Background()

	g := witness.NewWitnessGateway(group, ts.Client(), 0, testLogTileFetcher)
	// This call will trigger case 0 and then case 1 in the witness handler above.
	// case 0 will return a response that notifies the log that its view of the witness size is wrong.
	// This method will then update its size and make a second request with a consistency proof, triggering case 1.
	_, err = g.Witness(ctx, logSignedCheckpoint)
	if err != nil {
		t.Fatal(err)
	}

	// This triggers case 2 in the witness, which isn't implemented so we don't care about any error,
	// we just invoke this to cause the validation in that witness body to trigger.
	_, _ = g.Witness(ctx, logSignedCheckpoint)
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
		if got, want := r.URL.String(), w1u.Path; got != want {
			t.Fatalf("got request to URL %q but expected %q", got, want)
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
	group := tessera.NewWitnessGroup(1, wit1)

	ctx := context.Background()

	g := witness.NewWitnessGateway(group, ts.Client(), 0, testLogTileFetcher)
	_, err = g.Witness(ctx, logSignedCheckpoint)
	if err == nil {
		t.Fatal("Expected error from not getting witness checkpoint")
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
		case w1u.Path:
			_, _ = w.Write(sigForSigner(t, n.Text, wit1Skey))
		case w2u.Path:
			_, _ = w.Write(sigForSigner(t, n.Text, wit2Skey))
		default:
			t.Fatalf("Unknown case: %s", r.URL.String())
		}
	}))
	baseURL := mustURL(t, ts.URL)
	var err error
	wit1, err = tessera.NewWitness(wit1Vkey, baseURL.JoinPath("wit1"))
	if err != nil {
		t.Fatal(err)
	}
	wit2, err = tessera.NewWitness(wit2Vkey, baseURL.JoinPath("wit2"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

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
	g1 := witness.NewWitnessGateway(tessera.NewWitnessGroup(1, wit1), ts.Client(), 0, cf1)
	g2 := witness.NewWitnessGateway(tessera.NewWitnessGroup(2, wit1, wit2), ts.Client(), 0, cf2)

	for i := range 10 {
		logSignedCheckpoint, _ := loadCheckpoint(t, i)
		_, err = g1.Witness(ctx, logSignedCheckpoint)
		if err != nil {
			t.Fatal(err)
		}
		_, err = g2.Witness(ctx, logSignedCheckpoint)
		if err != nil {
			t.Fatal(err)
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

func mustCreateCoSigSigner(t *testing.T, skey string) note.Signer {
	t.Helper()
	signer, err := f_note.NewSignerForCosignatureV1(skey)
	if err != nil {
		t.Fatal(err)
	}
	return signer
}
