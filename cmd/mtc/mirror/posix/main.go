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
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	fnote "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/config"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/handler"
	"github.com/transparency-dev/witness/omniwitness"
	"github.com/transparency-dev/witness/witness"
	"github.com/transparency-dev/witness/persistence/sqlite"
	"golang.org/x/mod/sumdb/note"
)

const (
	witnessDir = "witness"
)

var (
	listenAddr        = flag.String("listen_addr", ":8080", "The address to listen on for HTTP requests.")
	storageDir        = flag.String("storage_dir", "", "Directory to store mirror data.")
	witnessSignerPath = flag.String("witness_signer_path", "", "The path to the note-formatted witness signer secret key.")
	mirrorConfigPath  = flag.String("config_path", "", "The path to the mirror configuration file.")
)

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx := context.Background()
	
	w, wp, shutdown := witnessFromFlags(ctx)
	defer func() {
		if err := shutdown(); err != nil {
			slog.ErrorContext(ctx, "Failed to shut down Witness", slog.Any("error", err))
		}
	}()

	mux := handler.NewMirrorMux()
	cfg := mirrorConfigFromFlags(ctx)
	for _, l := range cfg.Logs {
		v, err := fnote.NewVerifier(l.VKey)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create verifier", slog.String("vkey", l.VKey), slog.Any("error", err))
			os.Exit(1)
		}
		origin := v.Name()
		if err := mux.AddTarget(origin, &fakeTarget{}); err != nil {
			slog.ErrorContext(ctx, "Failed to add target", slog.String("origin", origin), slog.Any("error", err))
			os.Exit(1)
		}
		if err := wp.AddLogs(ctx, []omniwitness.Log{
			{Origin: origin, VKey: l.VKey},
		}); err != nil {
			slog.ErrorContext(ctx, "Failed to add target log to witness", slog.String("origin", origin), slog.Any("error", err))
			os.Exit(1)
		}
	}

	h := handler.New(mux, w)

	slog.InfoContext(ctx, "Starting mirror service", slog.String("addr", *listenAddr))
	if err := http.ListenAndServe(*listenAddr, h); err != nil {
		slog.ErrorContext(ctx, "ListenAndServe failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func mirrorConfigFromFlags(ctx context.Context) config.Config {
	if *mirrorConfigPath == "" {
		slog.ErrorContext(ctx, "Mirror config path not specified")
		os.Exit(1)
	}
	data, err := os.ReadFile(*mirrorConfigPath)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to read mirror config file", slog.String("path", *mirrorConfigPath), slog.Any("error", err))
		os.Exit(1)
	}
	var cfg config.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		slog.ErrorContext(ctx, "Failed to unmarshal mirror config", slog.Any("error", err))
		os.Exit(1)
	}
	return cfg
}

// witnessFromFlags returns a witness instance configured from the provided flags.
// Exits if the witness could not be created.
//
// The returned shutdown func should be called once the witness is no longer in use.
func witnessFromFlags(ctx context.Context) (*witness.Witness, *sqlite.Persistence, func() error) {
 	if *storageDir == "" {
 		slog.ErrorContext(ctx, "Storage directory not specified")
 		os.Exit(1)
 	}
 
 	wPath := filepath.Join(*storageDir, witnessDir)
 	if err := os.MkdirAll(wPath, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
 		slog.ErrorContext(ctx, "Failed to create witness directory", slog.String("path", wPath))
 		os.Exit(1)
 	}
 
	p, shutdown, err := sqlite.New(ctx, sqlite.Opts{
 		Path: filepath.Join(wPath, "witness.db"),
 	})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create witness persistence", slog.String("path", wPath), slog.Any("error", err))
		os.Exit(1)
	}

 	w, err := witness.New(ctx, witness.Opts{
 		Persistence: p,
 		Signers:     witnessSignerFromFlags(ctx),
		VerifierForLog: func(ctx context.Context, origin string) (note.Verifier, bool, error) {
			log, ok, err := p.Log(ctx, origin)
			if err != nil || !ok {
				return nil, false, err
			}
			return log.Verifier, true, nil
		},
	})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create witness", slog.Any("error", err))
		os.Exit(1)
	}
	return w, p, shutdown
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


