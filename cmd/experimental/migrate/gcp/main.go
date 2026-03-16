// Copyright 2025 The Tessera authors. All Rights Reserved.
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

// gcp-migrate is a command-line tool for migrating data from a tlog-tiles
// compliant log, into a Tessera log instance.
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"log/slog"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/storage/gcp"
	gcp_as "github.com/transparency-dev/tessera/storage/gcp/antispam"
)

var (
	bucket  = flag.String("bucket", "", "Bucket to use for storing log")
	spanner = flag.String("spanner", "", "Spanner resource URI ('projects/.../...')")

	sourceURL          = flag.String("source_url", "", "Base URL for the source log.")
	numWorkers         = flag.Uint("num_workers", 30, "Number of migration worker goroutines.")
	persistentAntispam = flag.Bool("antispam", false, "EXPERIMENTAL: Set to true to enable GCP-based persistent antispam storage")
	slogLevel          = flag.Int("slog_level", 0, "The cut-off threshold for structured logging. Default is 0 (INFO). See https://pkg.go.dev/log/slog#Level for other levels.")
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

	// Create our Tessera storage backend:
	gcpCfg := storageConfigFromFlags()
	driver, err := gcp.New(ctx, gcpCfg)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create new GCP storage driver", slog.Any("error", err))
		os.Exit(1)
	}

	opts := tessera.NewMigrationOptions()
	// Configure antispam storage, if necessary
	var antispam tessera.Antispam
	// Persistent antispam is currently experimental, so there's no terraform or documentation yet!
	if *persistentAntispam {
		asOpts := gcp_as.AntispamOpts{
			MaxBatchSize: 1500,
		}
		antispam, err = gcp_as.NewAntispam(ctx, fmt.Sprintf("%s-antispam", *spanner), asOpts)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create new GCP antispam storage", slog.Any("error", err))
			os.Exit(1)
		}
		opts.WithAntispam(antispam)
	}

	m, err := tessera.NewMigrationTarget(ctx, driver, opts)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create MigrationTarget", slog.Any("error", err))
		os.Exit(1)
	}

	if err := m.Migrate(ctx, *numWorkers, sourceSize, sourceRoot, src.ReadEntryBundle); err != nil {
		slog.ErrorContext(ctx, "Migrate failed", slog.Any("error", err))
		os.Exit(1)
	}
}

// storageConfigFromFlags returns a gcp.Config struct populated with values
// provided via flags.
func storageConfigFromFlags() gcp.Config {
	if *bucket == "" {
		slog.ErrorContext(context.Background(), "--bucket must be set")
		os.Exit(1)
	}
	if *spanner == "" {
		slog.ErrorContext(context.Background(), "--spanner must be set")
		os.Exit(1)
	}
	return gcp.Config{
		Bucket:  *bucket,
		Spanner: *spanner,
	}
}
