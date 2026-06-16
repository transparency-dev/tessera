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
	"path/filepath"
	"log/slog"

	fnote "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/handler"
	"github.com/transparency-dev/witness/witness"
	"github.com/transparency-dev/witness/persistence/sqlite"
	"golang.org/x/mod/sumdb/note"
)

const (
	witnessDir = "witness"
)

var (
	listenAddr      = flag.String("listen_addr", ":8080", "The address to listen on for HTTP requests.")
	storageDir      = flag.String("storage_dir", "", "Directory to store mirror data.")
	witnessSignerPath = flag.String("witness_signer_path", "", "The path to the note-formatted witness signer secret key.")
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
	if *storageDir == "" {
		slog.ErrorContext(ctx, "Storage directory not specified")
		os.Exit(1)
	}

	wPath := filepath.Join(*storageDir, witnessDir)
	if err := os.MkdirAll(wPath, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		slog.ErrorContext(ctx, "Failed to create witness directory", slog.String("path", wPath))
		os.Exit(1)
	}


	// TODO(al): config.
	v, err := fnote.NewVerifier("example.com/inmemorylog/0+7fd3f320+AXX8yEKexoMqBPPwG4pGAhhjo5CyiHLiJZ7p3jg0aJZM")
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create note verifier", slog.Any("error", err))
		os.Exit(1)
	}

	p := sqlite.New(sqlite.Opts{
		Path: filepath.Join(wPath, "witness.db"),
	})
	w, err := witness.New(ctx, witness.Opts{
		Persistence: p,
		Signers:     witnessSignerFromFlags(ctx),
		VerifierForLog: func(ctx context.Context, origin string) (note.Verifier, bool, error) {
			return v, true, nil
		},
	})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create witness", slog.Any("error", err))
		os.Exit(1)
	}
	return w	
}

// fakeTarget is a temporary mirror target impl, and will be removed in due course.
type fakeTarget struct {}

func (f fakeTarget) AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) ([]byte, error) {
	slog.InfoContext(ctx, "fake target: AddEntries", slog.Uint64("uploadStart", uploadStart), slog.Uint64("uploadEnd", uploadEnd))
	return nil, nil
}

func witnessSignerFromFlags(ctx context.Context) []note.Signer {
	if *witnessSignerPath == "" {
		slog.WarnContext(ctx, "Witness cosigner not configured, add-checkpoint will not return cosigs")
		return []note.Signer{}
	}
	r, err := os.ReadFile(*witnessSignerPath)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to read witness cosigner file", slog.String("path", *witnessSignerPath), slog.Any("error", err))
		os.Exit(1)
	}
	s, err := fnote.NewSignerForCosignatureV1(string(r))
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create cosigner", slog.String("path", *witnessSignerPath), slog.Any("error", err))
		os.Exit(1)
	}
	return []note.Signer{s}
}


