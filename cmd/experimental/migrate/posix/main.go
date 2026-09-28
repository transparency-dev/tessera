// Copyright 2024 The Tessera authors. All Rights Reserved.
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

// posix-migrate is a command-line tool for migrating data from a tlog-tiles
// compliant log, into a Tessera log instance.
package main

import (
	"context"
	"flag"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"log/slog"

	"github.com/transparency-dev/formats/log"
	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/storage/posix"
	"golang.org/x/mod/sumdb/note"
)

var (
	storageDir     = flag.String("storage_dir", "", "Root directory to store log data.")
	sourceURL      = flag.String("source_url", "", "Base URL for the source log.")
	sourceOrigin   = flag.String("source_origin", "", "Origin of the source log. If unset, the name of the first --source_public_key will be used.")
	sourcePubKeys  = &multiStringFlag{}
	numWorkers     = flag.Uint("num_workers", 30, "Number of migration worker goroutines.")
	slogLevel      = flag.Int("slog_level", 0, "The cut-off threshold for structured logging. Default is 0 (INFO). See https://pkg.go.dev/log/slog#Level for other levels.")
	saveCheckpoint = flag.Bool("save_checkpoint", false, "Set to true to write the checkpoint used during migration. Useful for mirrors.")
)

func init() {
	flag.Var(sourcePubKeys, "source_public_key", "Path to a file containing a public key of the source log (can be specified multiple times). The first key is used as the log's key, and the source checkpoint must be signed by all provided keys.")
}

func main() {
	flag.Parse()
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(*slogLevel)})))

	srcURL, err := url.Parse(*sourceURL)
	if err != nil {
		slog.ErrorContext(ctx, "Invalid --source_url", slog.String("param", *sourceURL), slog.Any("error", err))
		os.Exit(1)
	}
	src, err := client.NewHTTPFetcher(srcURL, nil)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create HTTP fetcher", slog.Any("error", err))
		os.Exit(1)
	}
	sourceCP, err := src.ReadCheckpoint(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "fetch initial source checkpoint", slog.Any("error", err))
		os.Exit(1)
	}
	vs := verifiersFromFlags(ctx)
	origin := *sourceOrigin
	if origin == "" {
		origin = vs[0].Name()
	}
	cp, _, n, err := log.ParseCheckpoint(sourceCP, origin, vs[0], vs[1:]...)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to parse and verify source checkpoint", slog.Any("error", err))
		os.Exit(1)
	}
	// Require the checkpoint to be signed by all provided keys.
	if got, want := len(n.Sigs), len(vs); got != want {
		slog.ErrorContext(ctx, "Checkpoint has unexpected number of verified signatures", slog.Int("got", got), slog.Int("want", want))
		os.Exit(1)
	}

	driver, err := posix.New(ctx, posix.Config{Path: *storageDir})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create new POSIX storage driver", slog.Any("error", err))
		os.Exit(1)
	}
	// Create our Tessera migration target instance
	m, err := tessera.NewMigrationTarget(ctx, driver, tessera.NewMigrationOptions())
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create MigrationTarget", slog.Any("error", err))
		os.Exit(1)
	}

	if err := m.Migrate(ctx, *numWorkers, cp.Size, cp.Hash, src.ReadEntryBundle); err != nil {
		slog.ErrorContext(ctx, "Migrate failed", slog.Any("error", err))
		os.Exit(1)
	}

	if *saveCheckpoint {
		if err := os.WriteFile(filepath.Join(*storageDir, layout.CheckpointPath), sourceCP, 0o644); err != nil {
			slog.ErrorContext(ctx, "Failed to write checkpoint", slog.Any("error", err))
			os.Exit(1)
		}
	}
}

// verifiersFromFlags creates a slice of note.Verifier based on the provided flags.
func verifiersFromFlags(ctx context.Context) []note.Verifier {
	if len(*sourcePubKeys) == 0 {
		slog.ErrorContext(ctx, "Must provide at least one --source_public_key flag")
		os.Exit(1)
	}
	vs := make([]note.Verifier, 0, len(*sourcePubKeys))
	for _, pk := range *sourcePubKeys {
		b, err := os.ReadFile(pk)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to read verifier", slog.String("pubkey", pk), slog.Any("error", err))
			os.Exit(1)
		}
		v, err := f_note.NewVerifier(string(b))
		if err != nil {
			slog.ErrorContext(ctx, "Invalid verifier", slog.String("pubkey", pk), slog.Any("error", err))
			os.Exit(1)
		}
		vs = append(vs, v)
	}
	return vs
}

// multiStringFlag allows a flag to be specified multiple times on the command
// line, and stores all of these values.
type multiStringFlag []string

func (ms *multiStringFlag) String() string {
	return strings.Join(*ms, ",")
}

func (ms *multiStringFlag) Set(w string) error {
	*ms = append(*ms, w)
	return nil
}
