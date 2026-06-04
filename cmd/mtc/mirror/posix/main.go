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
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"net/url"

	"log/slog"

	"github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/storage/posix"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/handler"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror/internal/mirror"
)

var (
	listenAddr  = flag.String("listen_addr", ":8080", "The address to listen on for HTTP requests.")
	configPath  = flag.String("mirror_config", "", "Path to JSON config file describing mirror targets.")
	privKeyFile = flag.String("private_key_path", "", "Path to file containing private key to use for cosigning checkpoints.")
	storageRoot = flag.String("storage_root", "", "Path to directory to use for storing mirror data.")
	slogLevel   = flag.Int("slog_level", int(slog.LevelInfo), "The cut-off threshold for structured logging. Default is 0 (INFO).")
)

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx := context.Background()

	if *storageRoot == "" {
		slog.ErrorContext(ctx, "storage_root must be set")
		os.Exit(1)
	}

	if err := os.MkdirAll(*storageRoot, 0755); err != nil {
		slog.ErrorContext(ctx, "Failed to create storage_root", slog.Any("error", err))
		os.Exit(1)
	}

	cfg, err := mirror.Load(*configPath)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	signer := signerFromFlags(ctx)

	targets := make(map[string]mirror.Target)
	for _, log := range cfg.Logs {
		origin := log.Verifier.Name()
		mirrorRoot := filepath.Join(*storageRoot, url.PathEscape(origin))
		if err := os.MkdirAll(mirrorRoot, 0755); err != nil {
			slog.ErrorContext(ctx, "Failed to create mirror root", slog.String("origin", origin), slog.Any("error", err))
			os.Exit(1)
		}
		driver, err := posix.New(ctx, posix.Config{Path: mirrorRoot})
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create driver", slog.String("origin", origin), slog.Any("error", err))
			os.Exit(1)
		}
		t, err := tessera.NewMirrorTarget(ctx, driver, tessera.NewMirrorOptions().
			WithLogVerifier(log.Verifier).
			WithSigner(signer))
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create mirror target", slog.String("origin", origin), slog.Any("error", err))
			os.Exit(1)
		}
		targets[origin] = t
	}

	m := mirror.New(targets)
	h := handler.New(m)

	slog.InfoContext(ctx, "Starting mirror service", slog.String("addr", *listenAddr))
	if err := http.ListenAndServe(*listenAddr, h); err != nil {
		slog.ErrorContext(ctx, "ListenAndServe failed", slog.Any("error", err))
		os.Exit(1)
	}
}

// signerFromFlags creates a note.Signer for cosignature v1 signnatures from the command-line flags.
func signerFromFlags(ctx context.Context) *note.Signer {
	if *privKeyFile == "" {
		slog.ErrorContext(ctx, "Missing private_key_path flag")
		os.Exit(1)
	}
	pkRaw, err := os.ReadFile(*privKeyFile)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to read private_key_path", slog.Any("error", err))
		os.Exit(1)
	}
	signer, err := note.NewSignerForCosignatureV1(string(pkRaw))
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create signer", slog.Any("error", err))
		os.Exit(1)
	}
	return signer
}
