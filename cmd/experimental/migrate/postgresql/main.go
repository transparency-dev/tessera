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

// postgresql-migrate is a command-line tool for migrating data from a tlog-tiles
// compliant log, into a Tessera log instance backed by PostgreSQL.
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/storage/postgresql"
	"k8s.io/klog/v2"
)

var (
	pgURI          = flag.String("pg_uri", "postgres://user:password@db:5432/tessera?sslmode=disable", "Connection string for a PostgreSQL database")
	dbMaxConns     = flag.Int("db_max_conns", 64, "Maximum number of connections in the pool")
	initSchemaPath = flag.String("init_schema_path", "", "Location of the schema file if database initialization is needed")

	sourceURL  = flag.String("source_url", "", "Base URL for the source log.")
	numWorkers = flag.Uint("num_workers", 30, "Number of migration worker goroutines.")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx := context.Background()

	srcURL, err := url.Parse(*sourceURL)
	if err != nil {
		klog.Exitf("Invalid --source_url %q: %v", *sourceURL, err)
	}
	src, err := client.NewHTTPFetcher(srcURL, nil)
	if err != nil {
		klog.Exitf("Failed to create HTTP fetcher: %v", err)
	}
	sourceCP, err := src.ReadCheckpoint(ctx)
	if err != nil {
		klog.Exitf("Failed to read source checkpoint: %v", err)
	}
	bits := strings.Split(string(sourceCP), "\n")
	sourceSize, err := strconv.ParseUint(bits[1], 10, 64)
	if err != nil {
		klog.Exitf("Invalid CP size %q: %v", bits[1], err)
	}
	sourceRoot, err := base64.StdEncoding.DecodeString(bits[2])
	if err != nil {
		klog.Exitf("Invalid checkpoint roothash %q: %v", bits[2], err)
	}

	pool := createPoolOrDie(ctx)
	defer pool.Close()

	// Initialise the Tessera PostgreSQL storage
	driver, err := postgresql.New(ctx, pool)
	if err != nil {
		klog.Exitf("Failed to create new PostgreSQL storage: %v", err)
	}

	opts := tessera.NewMigrationOptions()

	m, err := tessera.NewMigrationTarget(ctx, driver, opts)
	if err != nil {
		klog.Exitf("Failed to create MigrationTarget: %v", err)
	}

	if err := m.Migrate(context.Background(), *numWorkers, sourceSize, sourceRoot, src.ReadEntryBundle); err != nil {
		klog.Exitf("Migrate failed: %v", err)
	}
}

func createPoolOrDie(ctx context.Context) *pgxpool.Pool {
	config, err := pgxpool.ParseConfig(*pgURI)
	if err != nil {
		klog.Exitf("Failed to parse PostgreSQL connection string: %v", err)
	}
	config.MaxConns = int32(*dbMaxConns)

	initDatabaseSchema(ctx)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		klog.Exitf("Failed to create PostgreSQL pool: %v", err)
	}
	return pool
}

func initDatabaseSchema(ctx context.Context) {
	if *initSchemaPath != "" {
		klog.Infof("Initializing database schema")

		pool, err := pgxpool.New(ctx, *pgURI)
		if err != nil {
			klog.Exitf("Failed to connect to DB: %v", err)
		}
		defer pool.Close()

		rawSchema, err := os.ReadFile(*initSchemaPath)
		if err != nil {
			klog.Exitf("Failed to read init schema file %q: %v", *initSchemaPath, err)
		}
		if _, err := pool.Exec(ctx, string(rawSchema)); err != nil {
			klog.Exitf("Failed to execute init database schema: %v", err)
		}

		klog.Infof("Database schema initialized")
	}
}
