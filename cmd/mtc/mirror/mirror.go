// Copyright 2026 The Tessera Authors. All Rights Reserved.
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

// Package mirror provides functionality for creating and running tlog-mirror services.
package mirror

import (
	"context"
	"fmt"
	"net/http"

	fnote "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/handler"
	witnessConfig "github.com/transparency-dev/witness/config"
	"github.com/transparency-dev/witness/witness"
	"golang.org/x/mod/sumdb/note"
)

// LogConfig represents a log to be mirrored.
type LogConfig struct {
	// Log is the information about the log itself.
	Log witnessConfig.Log
	// Driver is the Tessera Driver which should be used for storing the mirrored log.
	Driver tessera.Driver
}

// Mirror represents an instance of a mirror service.
//
// This struct implements the http.Handler interface and so can be directly used to serve HTTP requests.
type Mirror struct {
	handler http.Handler
}

// New creates a new mirror instance with the provided configuration.
func New(ctx context.Context, w *witness.Witness, mirrorCosigner fnote.SubtreeSigner, cfg []LogConfig) (*Mirror, error) {
	if err := assertDistinctSigners(w.Signers, []note.Signer{mirrorCosigner}); err != nil {
		return nil, err
	}
	mux := handler.NewMirrorMux()
	for _, l := range cfg {
		// Create the mirror
		mOpts := tessera.NewMirrorOptions().
			WithCheckpointSource(func(ctx context.Context) ([]byte, error) {
				return w.GetCheckpoint(ctx, l.Log.Origin)
			}).
			WithOrigin(l.Log.Origin).
			WithLogVerifier(l.Log.Verifier).
			WithSigner(mirrorCosigner)
		t, err := tessera.NewMirrorTarget(ctx, l.Driver, mOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create mirror target %q: %w", l.Log.Origin, err)
		}
		if err := mux.AddTarget(l.Log.Origin, t); err != nil {
			return nil, fmt.Errorf("failed to add target %q to mux: %w", l.Log.Origin, err)
		}
	}
	return &Mirror{
		handler: handler.New(mux, w),
	}, nil
}

// ServeHTTP implements the http.Handler interface, and exposes the tlog-mirror protocol via HTTP.
func (m *Mirror) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.handler.ServeHTTP(w, r)
}

// assertDistinctSigners asserts that the two provided lists of cosigners have no signers in common.
func assertDistinctSigners(w, m []note.Signer) error {
	type nhKey struct {
		name string
		hash uint32
	}
	aMap := make(map[nhKey]struct{}, len(w))
	for _, s := range w {
		aMap[nhKey{name: s.Name(), hash: s.KeyHash()}] = struct{}{}
	}
	for _, s := range m {
		if _, ok := aMap[nhKey{name: s.Name(), hash: s.KeyHash()}]; ok {
			return fmt.Errorf("cannot use same signing key for witness and mirror: name=%s, hash=%x", s.Name(), s.KeyHash())
		}
	}
	return nil
}
