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

package gateway_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/transparency-dev/formats/log"
	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/internal/mirror/gateway"
	"github.com/transparency-dev/tessera/testonly"
	"golang.org/x/mod/sumdb/note"
)

type fakeWitnessGroup struct {
	endpoints map[string][]note.Verifier
}

func (f fakeWitnessGroup) WitnessEndpoints() map[string][]note.Verifier {
	return f.endpoints
}

func TestGateway(t *testing.T) {
	opts := tessera.NewAppendOptions().
		WithCheckpointInterval(100 * time.Millisecond).
		WithCheckpointRepublishInterval(100 * time.Millisecond)

	testLog, shutdown := testonly.NewTestLog(t, opts)
	defer func() {
		_ = shutdown(t.Context())
	}()

	// Create a log with some entries.
	const size = 5
	var f tessera.IndexFuture
	for i := range size {
		entry := tessera.NewEntry(fmt.Appendf(nil, "entry-%d", i))
		f = testLog.Appender.Add(t.Context(), entry)
	}
	a := tessera.NewPublicationAwaiter(t.Context(), testLog.LogReader.ReadCheckpoint, 100*time.Millisecond)
	if _, _, err := a.Await(t.Context(), f); err != nil {
		t.Fatalf("failed to add entry: %v", err)
	}
	goalCP, err := testLog.LogReader.ReadCheckpoint(t.Context())
	if err != nil {
		t.Fatalf("failed to read checkpoint: %v", err)
	}

	const numMirrors = 3
	var verifiers []note.Verifier
	endpoints := make(map[string][]note.Verifier)

	for i := range numMirrors {
		signer, verifier := mustNewKeypair(t, fmt.Sprintf("Mirror-%d", i))
		server := startMockMirror(t, signer, testLog.SigVerifier)
		defer server.Close()

		endpoints[server.URL] = []note.Verifier{verifier}
		verifiers = append(verifiers, verifier)
	}

	policy := fakeWitnessGroup{
		endpoints: endpoints,
	}

	g := gateway.NewGateway(t.Context(), http.DefaultClient, policy, testLog.LogReader, "test")

	// Call CosignCheckpoint and gather signatures.
	sigCh := g.CosignCheckpoint(t.Context(), goalCP, size)
	var cosigs []byte
	for sig := range sigCh {
		cosigs = append(cosigs, sig...)
	}

	// Verify cosignatures.
	fullCP := append(slices.Clone(goalCP), cosigs...)
	cp, _, n, err := log.ParseCheckpoint(fullCP, testLog.SigVerifier.Name(), testLog.SigVerifier, verifiers...)
	if err != nil {
		t.Fatalf("failed to verify cosigned checkpoint: %v", err)
	}
	if got, want := len(n.Sigs), 1+numMirrors; got != want {
		t.Errorf("note signatures: got %d, want %d", got, want)
	}
	if got, want := uint64(cp.Size), uint64(size); got != want {
		t.Errorf("checkpoint size: got %d, want %d", got, want)
	}
}

func mustNewKeypair(t *testing.T, name string) (f_note.Signer, note.Verifier) {
	t.Helper()
	skey, vkey, err := note.GenerateKey(rand.Reader, name)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	s, err := f_note.NewSignerForCosignatureV1(skey)
	if err != nil {
		t.Fatalf("Failed to create signer: %v", err)
	}
	v, err := f_note.NewVerifierForCosignatureV1(vkey)
	if err != nil {
		t.Fatalf("Failed to create verifier: %v", err)
	}
	return s, v
}

func startMockMirror(t *testing.T, signer note.Signer, logVerifier note.Verifier) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	var pendingCP []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/add-checkpoint") {
			body, _ := io.ReadAll(r.Body)
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

			n, err := note.Open(cp, note.VerifierList(logVerifier))
			if err != nil {
				t.Errorf("failed to open cp in mock: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			signedNote, err := note.Sign(n, signer)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			idx := strings.Index(string(signedNote), "— "+signer.Name()+" ")
			if idx < 0 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			sigLine := string(signedNote)[idx:]
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(sigLine))
			return
		}
	}))

	return server
}
