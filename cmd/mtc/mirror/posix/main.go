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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	fnote "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/config"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/handler"
	"github.com/transparency-dev/tessera/storage/posix"
	"github.com/transparency-dev/witness/omniwitness"
	"github.com/transparency-dev/witness/persistence/sqlite"
	"github.com/transparency-dev/witness/witness"
	"golang.org/x/mod/sumdb/note"
)

const (
	witnessDir = "private/witness"
	mirrorsDir = "public/mirrors"
)

var (
	listenAddr          = flag.String("listen_addr", ":8080", "The address to listen on for HTTP requests.")
	storageDir          = flag.String("storage_dir", "", "Directory to store mirror data.")
	witnessCosignerPath = flag.String("witness_cosigner_path", "", "The path to the note-formatted witness cosigner secret key.")
	mirrorCosignerPath  = flag.String("mirror_cosigner_path", "", "The path to the note-formatted mirror cosigner secret key.")
	mirrorConfigPath    = flag.String("config_path", "", "The path to the mirror configuration file.")
	slogLevel           = flag.Int("slog_level", 0, "The cut-off threshold for structured logging.")
)

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(*slogLevel)})))
	ctx := context.Background()

	w, wp, shutdown := witnessFromFlags(ctx)
	defer func() {
		if err := shutdown(); err != nil {
			slog.ErrorContext(ctx, "Failed to shut down Witness", slog.Any("error", err))
		}
	}()

	mirrorCosigner := mustCreateCosigner(ctx, *mirrorCosignerPath)

	mux := handler.NewMirrorMux()
	cfg := mirrorConfigFromFlags(ctx)
	for _, l := range cfg.Logs {
		v, err := fnote.NewVerifier(l.VKey)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create verifier", slog.String("vkey", l.VKey), slog.Any("error", err))
			os.Exit(1)
		}

		origin := v.Name()

		// Create the mirror
		t, err := newMirrorTarget(ctx, w, v, mirrorCosigner)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create mirror target", slog.String("origin", origin), slog.Any("error", err))
			os.Exit(1)
		}
		if err := mux.AddTarget(origin, t); err != nil {
			slog.ErrorContext(ctx, "Failed to add target to mux", slog.String("origin", origin), slog.Any("error", err))
			os.Exit(1)
		}

		// Ensure log is known by the witness
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

// newMirrorTarget creates a new POSIX driver and MirrorTarget for the given origin.
//
// The target directory for the driver is derived from the storage directory and the origin in accordance
// with the `tlog-mirror` spec, allowing the root of the storage directory to be exported directly to read-only clients.
func newMirrorTarget(ctx context.Context, w *witness.Witness, logVerifier note.Verifier, mirrorSigner note.Signer) (*tessera.MirrorTarget, error) {
	origin := logVerifier.Name()

	targetDir := filepath.Join(*storageDir, mirrorsDir, fmt.Sprintf("%0x", sha256.Sum256([]byte(origin))))
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %q: %v", targetDir, err)
	}
	d, err := posix.New(ctx, posix.Config{Path: targetDir})
	if err != nil {
		return nil, fmt.Errorf("posix.New: %v", err)
	}
	mOpts := tessera.NewMirrorOptions().
		WithCheckpointSource(func(ctx context.Context) ([]byte, error) {
			return w.GetCheckpoint(ctx, origin)
		}).
		WithLogVerifier(logVerifier).
		WithSigner(mirrorSigner)
	return tessera.NewMirrorTarget(ctx, d, mOpts)
}

// mirrorConfigFromFlags returns a mirror configuration loaded from the provided flags.
// Exits if the mirror configuration could not be loaded.
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
		Signers:     witnessCosignerFromFlags(ctx),
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

func witnessCosignerFromFlags(ctx context.Context) []note.Signer {
	if *witnessCosignerPath == "" {
		slog.WarnContext(ctx, "Witness cosigner not configured, add-checkpoint will not return cosigs")
		return []note.Signer{}
	}
	return []note.Signer{mustCreateCosigner(ctx, *witnessCosignerPath)}
}

func mustCreateCosigner(ctx context.Context, path string) note.Signer {
	if path == "" {
		slog.ErrorContext(ctx, "Cosigner key path not specified")
		os.Exit(1)
	}
	r, err := os.ReadFile(path)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to read cosigner key", slog.String("path", path), slog.Any("error", err))
		os.Exit(1)
	}
	s, err := fnote.NewSignerForCosignatureV1(string(r))
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create cosigner", slog.String("path", path), slog.Any("error", err))
		os.Exit(1)
	}
	return s
}
