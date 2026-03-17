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
	"encoding/base64"
	"flag"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"log/slog"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/storage/posix"
)

var (
	storageDir     = flag.String("storage_dir", "", "Root directory to store log data.")
	sourceURL      = flag.String("source_url", "", "Base URL for the source log.")
	numWorkers     = flag.Uint("num_workers", 30, "Number of migration worker goroutines.")
	slogLevel      = flag.Int("slog_level", 0, "The cut-off threshold for structured logging. Default is 0 (INFO). See https://pkg.go.dev/log/slog#Level for other levels.")
	saveCheckpoint = flag.Bool("save_checkpoint", false, "Set to true to write the checkpoint used during migration. Useful for mirrors.")
)

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
	bits := strings.Split(string(sourceCP), "\n")
	sourceSize, err := strconv.ParseUint(bits[1], 10, 64)
	if err != nil {
		slog.ErrorContext(ctx, "invalid CP size", slog.String("size", bits[1]), slog.Any("error", err))
		os.Exit(1)
	}
	sourceRoot, err := base64.StdEncoding.DecodeString(bits[2])
	if err != nil {
		slog.ErrorContext(ctx, "invalid checkpoint roothash", slog.String("hash", bits[2]), slog.Any("error", err))
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
		slog.ErrorContext(ctx, "Failed to create new POSIX storage", slog.Any("error", err))
		os.Exit(1)
	}

	if err := m.Migrate(context.Background(), *numWorkers, sourceSize, sourceRoot, src.ReadEntryBundle); err != nil {
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
