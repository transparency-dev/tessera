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

package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"

	"log/slog"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/handler"
	"github.com/transparency-dev/witness/witness"
	"golang.org/x/mod/sumdb/note"
)

var (
	listenAddr = flag.String("listen_addr", ":8080", "The address to listen on for HTTP requests.")
)

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx := context.Background()
	
	w := witnessFromFlags(ctx)

	mux := handler.NewMirrorMux()
	if err := mux.AddTarget("example.com/log", &fakeTarget{}); err != nil {
		slog.ErrorContext(ctx, "Failed to add target", slog.Any("error", err))
		os.Exit(1)
	}

	h := handler.New(mux, w)

	slog.InfoContext(ctx, "Starting mirror service", slog.String("addr", *listenAddr))
	if err := http.ListenAndServe(*listenAddr, h); err != nil {
		slog.ErrorContext(ctx, "ListenAndServe failed", slog.Any("error", err))
		os.Exit(1)
	}
}

// witnessFromFlags returns a witness instance configured from the provided flags.
// Exits if the witness could not be created.
func witnessFromFlags(ctx context.Context) *witness.Witness {
	 w, err := witness.New(ctx, witness.Opts{
		Persistence: &fakePersistence{},
		Signers:     []note.Signer{},
		VerifierForLog: func(ctx context.Context, origin string) (note.Verifier, bool, error) {
			return nil, false, errors.New("unimplemented")
		},
	})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create witness", slog.Any("error", err))
		os.Exit(1)
	}
	return w	
}

// fakePersistence is a temporary witness persistence impl, and will be removed in due course.
type fakePersistence struct {}

// Init sets up the persistence layer. This should be idempotent,
// and will be called once per process startup.
func (f *fakePersistence) Init(ctx context.Context) error {
	slog.InfoContext(ctx, "fake persistence: Init")
	return nil
}

// Latest returns the latest checkpoint.
// If no checkpoint exists, it must return nil.
func (f *fakePersistence) Latest(ctx context.Context, origin string) ([]byte, error) {
	slog.InfoContext(ctx, "fake persistence: Latest", slog.String("origin", origin))
	return nil, nil
}

// Update allows for atomically updating the currently stored (if any)
// checkpoint for the given origin.
//
// The provided function will be passed the currently stored checkpoint
// for the provided log origin (or nil if no such checkpoint exists), and
// should return the serialised form of the updated checkpoint, or an
// error.
func (f *fakePersistence) Update(ctx context.Context, origin string, update func([]byte) ([]byte, error)) error {
	slog.InfoContext(ctx, "fake persistence: Update", slog.String("origin", origin))
	return nil
}

// fakeTarget is a temporary mirror target impl, and will be removed in due course.
type fakeTarget struct {}

func (f fakeTarget) AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) ([]byte, error) {
	slog.InfoContext(ctx, "fake target: AddEntries", slog.Uint64("uploadStart", uploadStart), slog.Uint64("uploadEnd", uploadEnd))
	return nil, nil
}

