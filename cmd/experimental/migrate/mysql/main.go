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

// mysql-migrate is a command-line tool for migrating data from a tlog-tiles
// compliant log, into a Tessera log instance.
package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"flag"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"log/slog"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/storage/mysql"
)

var (
	mysqlURI          = flag.String("mysql_uri", "user:password@tcp(db:3306)/tessera", "Connection string for a MySQL database")
	dbConnMaxLifetime = flag.Duration("db_conn_max_lifetime", 3*time.Minute, "")
	dbMaxOpenConns    = flag.Int("db_max_open_conns", 64, "")
	dbMaxIdleConns    = flag.Int("db_max_idle_conns", 64, "")
	initSchemaPath    = flag.String("init_schema_path", "", "Location of the schema file if database initialization is needed")

	sourceURL  = flag.String("source_url", "", "Base URL for the source log.")
	numWorkers = flag.Uint("num_workers", 30, "Number of migration worker goroutines.")
	slogLevel  = flag.Int("slog_level", 0, "The cut-off threshold for structured logging. Default is INFO. See https://pkg.go.dev/log/slog#Level.")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(*slogLevel)})))

	srcURL, err := url.Parse(*sourceURL)
	if err != nil {
		slog.Error("Invalid --source_url", slog.String("sourceurl", *sourceURL), slog.Any("error", err))
		os.Exit(255)
	}
	src, err := client.NewHTTPFetcher(srcURL, nil)
	if err != nil {
		slog.Error("Failed to create HTTP fetcher", slog.Any("error", err))
		os.Exit(255)
	}
	sourceCP, err := src.ReadCheckpoint(ctx)
	if err != nil {
		slog.Error("Failed to read source checkpoint", slog.Any("error", err))
		os.Exit(255)
	}
	bits := strings.Split(string(sourceCP), "\n")
	sourceSize, err := strconv.ParseUint(bits[1], 10, 64)
	if err != nil {
		slog.Error("Invalid CP size", slog.String("size", bits[1]), slog.Any("error", err))
		os.Exit(255)
	}
	sourceRoot, err := base64.StdEncoding.DecodeString(bits[2])
	if err != nil {
		slog.Error("Invalid checkpoint roothash", slog.String("hash", bits[2]), slog.Any("error", err))
		os.Exit(255)
	}

	db := createDatabaseOrDie(ctx)

	// Initialise the Tessera MySQL storage
	driver, err := mysql.New(ctx, db)
	if err != nil {
		slog.Error("Failed to create new MySQL storage", slog.Any("error", err))
		os.Exit(255)
	}

	opts := tessera.NewMigrationOptions()

	m, err := tessera.NewMigrationTarget(ctx, driver, opts)
	if err != nil {
		slog.Error("Failed to create MigrationTarget", slog.Any("error", err))
		os.Exit(255)
	}

	if err := m.Migrate(context.Background(), *numWorkers, sourceSize, sourceRoot, src.ReadEntryBundle); err != nil {
		slog.Error("Migrate failed", slog.Any("error", err))
		os.Exit(255)
	}
}

func initDatabaseSchema(ctx context.Context) {
	if *initSchemaPath != "" {
		slog.Info("Initializing database schema")

		db, err := sql.Open("mysql", *mysqlURI+"?multiStatements=true")
		if err != nil {
			slog.Error("Failed to connect to DB", slog.Any("error", err))
			os.Exit(255)
		}
		defer func() {
			if err := db.Close(); err != nil {
				slog.Warn("Failed to close db", slog.Any("error", err))
			}
		}()

		rawSchema, err := os.ReadFile(*initSchemaPath)
		if err != nil {
			slog.Error("Failed to read init schema file", slog.String("initschemapath", *initSchemaPath), slog.Any("error", err))
			os.Exit(255)
		}
		if _, err := db.ExecContext(ctx, string(rawSchema)); err != nil {
			slog.Error("Failed to execute init database schema", slog.Any("error", err))
			os.Exit(255)
		}

		slog.Info("Database schema initialized")
	}
}

func createDatabaseOrDie(ctx context.Context) *sql.DB {
	db, err := sql.Open("mysql", *mysqlURI)
	if err != nil {
		slog.Error("Failed to connect to DB", slog.Any("error", err))
		os.Exit(255)
	}
	db.SetConnMaxLifetime(*dbConnMaxLifetime)
	db.SetMaxOpenConns(*dbMaxOpenConns)
	db.SetMaxIdleConns(*dbMaxIdleConns)

	initDatabaseSchema(ctx)
	return db
}
